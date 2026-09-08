package publication

import (
	"context"
	goerrors "errors"
	"fmt"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/weiquanpeng/go-pg-dts/logger"
	"github.com/weiquanpeng/go-pg-dts/pq"
	"strings"
)

var (
	ErrorPublicationIsNotExists = goerrors.New("publication is not exists")
)

var typeMap = pgtype.NewMap()

type Publication struct {
	conn pq.Connection
	cfg  Config
}

func New(cfg Config, conn pq.Connection) *Publication {
	return &Publication{cfg: cfg, conn: conn}
}

func (c *Publication) Create(ctx context.Context) (*Config, error) {
	info, err := c.Info(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorPublicationIsNotExists) || !c.cfg.CreateIfNotExists {
			return nil, errors.Wrap(err, "publication info")
		}
	} else {
		logger.Warn("publication already exists")
		if c.cfg.PublishViaPartitionRoot {
			c.warnIfNotViaPartitionRoot(ctx)
		}
		return info, nil
	}

	resultReader := c.conn.Exec(ctx, c.cfg.createQuery())
	_, err = resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "publication create result")
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication create result reader close")
	}

	logger.Info("publication created", "name", c.cfg.Name)

	return &c.cfg, nil
}

// warnIfNotViaPartitionRoot logs a warning when publish_via_partition_root is requested
// but the pre-existing publication was created without it. Create() reuses existing
// publications as-is, so the option would silently have no effect otherwise.
func (c *Publication) warnIfNotViaPartitionRoot(ctx context.Context) {
	query := fmt.Sprintf("SELECT pubviaroot FROM pg_publication WHERE pubname = '%s'", c.cfg.Name)
	resultReader := c.conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	_ = resultReader.Close()
	if err != nil || len(results) == 0 || len(results[0].Rows) == 0 {
		return
	}
	if string(results[0].Rows[0][0]) != "t" {
		logger.Warn("existing publication was created WITHOUT publish_via_partition_root: "+
			"changes will still be published with leaf partition names. "+
			"Recreate it or run: ALTER PUBLICATION ... SET (publish_via_partition_root = true)",
			"publication", c.cfg.Name)
	}
}

// EnsureTable adds a table to an existing publication when it is not already present.
// It is idempotent for sequential process starts.
func (c *Publication) EnsureTable(ctx context.Context, table Table) (*Config, error) {
	info, err := c.Info(ctx)
	if err != nil {
		return nil, err
	}

	for _, existing := range info.Tables {
		if existing.Schema == table.Schema && existing.Name == table.Name {
			return info, nil
		}
	}

	query := fmt.Sprintf(
		"ALTER PUBLICATION %s ADD TABLE %s",
		pgx.Identifier{c.cfg.Name}.Sanitize(),
		pgx.Identifier{table.Schema, table.Name}.Sanitize(),
	)
	resultReader := c.conn.Exec(ctx, query)
	if _, err = resultReader.ReadAll(); err != nil {
		resultReader.Close()
		return nil, errors.Wrap(err, "publication add table result")
	}
	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication add table result reader close")
	}

	logger.Info("table added to publication", "publication", c.cfg.Name, "table", table.Schema+"."+table.Name)
	return c.Info(ctx)
}

func (c *Publication) Info(ctx context.Context) (*Config, error) {
	resultReader := c.conn.Exec(ctx, c.cfg.infoQuery())
	results, err := resultReader.ReadAll()
	if err != nil {
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "42703" {
			return nil, ErrorPublicationIsNotExists
		}
		return nil, errors.Wrap(err, "publication info result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorPublicationIsNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication info result reader close")
	}

	publicationInfo, err := decodePublicationInfoResult(results[0])
	if err != nil {
		return nil, errors.Wrap(err, "publication info result decode")
	}

	return publicationInfo, nil
}

func decodePublicationInfoResult(result *pgconn.Result) (*Config, error) {
	var publicationConfig Config
	var tables []string

	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "pubname":
			publicationConfig.Name = v.(string)
		case "pubinsert":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "INSERT")
			}
		case "pubupdate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "UPDATE")
			}
		case "pubdelete":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "DELETE")
			}
		case "pubtruncate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "TRUNCATE")
			}
		case "pubtables":
			for _, val := range v.([]any) {
				tables = append(tables, val.(string))
			}
		}
	}

	for _, tableName := range tables {
		st := strings.Split(tableName, ".")
		publicationConfig.Tables = append(publicationConfig.Tables, Table{
			Name:   st[1],
			Schema: st[0],
		})
	}

	return &publicationConfig, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
