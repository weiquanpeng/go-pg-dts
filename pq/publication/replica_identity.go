package publication

import (
	"context"
	goerrors "errors"
	"fmt"
	"strings"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/weiquanpeng/go-pg-dts/logger"
)

const (
	ReplicaIdentityFull    = "FULL"
	ReplicaIdentityDefault = "DEFAULT"
)

var (
	ErrorTablesNotExists   = goerrors.New("table does not exists")
	ReplicaIdentityOptions = []string{ReplicaIdentityDefault, ReplicaIdentityFull}
	ReplicaIdentityMap     = map[string]string{
		"d": ReplicaIdentityDefault, // primary key on old value
		"f": ReplicaIdentityFull,    // full row on old value
	}
)

func (c *Publication) SetReplicaIdentities(ctx context.Context) error {
	if !c.cfg.CreateIfNotExists {
		return nil
	}

	// publish_via_partition_root mode: logical decoding reads old tuples based on the
	// LEAF partitions' replica identity, and ALTER on the partitioned parent does not
	// recurse, so each leaf must be aligned individually before the normal path below.
	if c.cfg.PublishViaPartitionRoot {
		if err := c.setLeafPartitionReplicaIdentities(ctx); err != nil {
			return err
		}
	}

	tables, err := c.GetReplicaIdentities(ctx)
	if err != nil {
		return err
	}

	diff := c.cfg.Tables.Diff(tables)

	for _, d := range diff {
		if err = c.AlterTableReplicaIdentity(ctx, d); err != nil {
			return err
		}
	}

	return nil
}

// setLeafPartitionReplicaIdentities expands each configured table into its leaf
// partitions (via pg_partition_tree, PG 12+) and aligns every leaf's replica identity
// with the configured one. Regular (non-partitioned) tables produce no rows here
// (level 0, i.e. the root itself, is excluded) and are handled by the normal path.
func (c *Publication) setLeafPartitionReplicaIdentities(ctx context.Context) error {
	for _, t := range c.cfg.Tables {
		query := fmt.Sprintf(
			`SELECT n.nspname, cl.relname, cl.relreplident::text
FROM pg_partition_tree('%s'::regclass) pt
JOIN pg_class cl ON cl.oid = pt.relid
JOIN pg_namespace n ON n.oid = cl.relnamespace
WHERE pt.isleaf AND pt.level > 0`,
			pgx.Identifier{t.Schema, t.Name}.Sanitize(),
		)

		resultReader := c.conn.Exec(ctx, query)
		results, err := resultReader.ReadAll()
		if err != nil {
			_ = resultReader.Close()
			return errors.Wrapf(err, "list leaf partitions of %s.%s", t.Schema, t.Name)
		}
		if err = resultReader.Close(); err != nil {
			return errors.Wrap(err, "leaf partitions result reader close")
		}

		altered := 0
		for _, result := range results {
			for _, row := range result.Rows {
				if ReplicaIdentityMap[string(row[2])] == t.ReplicaIdentity {
					continue
				}
				leaf := Table{
					Schema:          string(row[0]),
					Name:            string(row[1]),
					ReplicaIdentity: t.ReplicaIdentity,
				}
				if err = c.AlterTableReplicaIdentity(ctx, leaf); err != nil {
					return err
				}
				altered++
			}
		}
		if altered > 0 {
			logger.Info("leaf partition replica identities updated",
				"table", t.Schema+"."+t.Name,
				"replica_identity", t.ReplicaIdentity,
				"altered", altered)
		}
	}

	return nil
}

func (c *Publication) AlterTableReplicaIdentity(ctx context.Context, t Table) error {
	resultReader := c.conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s."%s" REPLICA IDENTITY %s;`, t.Schema, t.Name, t.ReplicaIdentity))
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "table replica identity update result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "table replica identity update result reader close")
	}

	logger.Info("table replica identity updated", "name", t.Name, "replica_identity", t.ReplicaIdentity)

	return nil
}

func (c *Publication) GetReplicaIdentities(ctx context.Context) ([]Table, error) {
	tableNames := make([]string, len(c.cfg.Tables))

	for i, t := range c.cfg.Tables {
		if t.Schema == "" && !strings.Contains(t.Name, ".") {
			tableNames[i] = "'" + t.Name + "'"
		} else {
			tableNames[i] = "'" + t.Schema + "." + t.Name + "'"
		}
	}

	query := fmt.Sprintf("SELECT relname AS table_name, n.nspname AS schema_name, relreplident AS replica_identity FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE concat(n.nspname, '.', c.relname) IN (%s)", strings.Join(tableNames, ", "))

	logger.Debug("executing query: ", query)

	resultReader := c.conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "replica identities result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorTablesNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "replica identities result reader close")
	}

	replicaIdentities, err := decodeReplicaIdentitiesResult(results)
	if err != nil {
		return nil, errors.Wrap(err, "replica identities result decode")
	}

	return replicaIdentities, nil
}

func decodeReplicaIdentitiesResult(results []*pgconn.Result) ([]Table, error) {
	var res []Table

	for _, result := range results {
		for i := range len(result.Rows) {
			var t Table
			for j, fd := range result.FieldDescriptions {
				v, err := decodeTextColumnData(result.Rows[i][j], fd.DataTypeOID)
				if err != nil {
					return nil, err
				}

				if v == nil {
					continue
				}

				switch fd.Name {
				case "table_name":
					t.Name = v.(string)
				case "schema_name":
					t.Schema = v.(string)
				case "replica_identity":
					t.ReplicaIdentity = ReplicaIdentityMap[string(v.(int32))]
				}
			}
			res = append(res, t)
		}
	}

	return res, nil
}
