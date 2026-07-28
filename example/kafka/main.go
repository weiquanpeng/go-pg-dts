package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	cdc "github.com/weiquanpeng/go-pg-dts"
	cdcconfig "github.com/weiquanpeng/go-pg-dts/config"
	"github.com/weiquanpeng/go-pg-dts/pq/message/format"
	"github.com/weiquanpeng/go-pg-dts/pq/publication"
	"github.com/weiquanpeng/go-pg-dts/pq/replication"
	"github.com/weiquanpeng/go-pg-dts/pq/slot"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	kafkaWriter     *kafka.Writer
	pgPool          *pgxpool.Pool
	topicPrefix     string
	primaryKeyCache sync.Map
)

const (
	kafkaRetryCount    = 5
	kafkaRetryInterval = 1 * time.Second
)

func main() {
	var sourceDSN, kafkaBrokers string
	var metricPort int
	var heartbeatEnabled bool
	flag.StringVar(&sourceDSN, "source", "", "source postgres dsn")
	flag.StringVar(&kafkaBrokers, "brokers", "127.0.0.1:9092", "kafka brokers")
	flag.StringVar(&topicPrefix, "topic-prefix", "cdc", "kafka topic prefix")
	flag.IntVar(&metricPort, "port", 2112, "metric port")
	flag.BoolVar(&heartbeatEnabled, "heartbeat", true, "enable CDC heartbeat every 5 minutes")
	flag.Parse()
	if sourceDSN == "" {
		slog.Error("missing --source")
		os.Exit(1)
	}
	ctx := context.Background()
	var err error
	pgPool, err = pgxpool.New(ctx, sourceDSN)
	if err != nil {
		slog.Error("create pg pool failed", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()
	srcConnConfig, err := pgx.ParseConfig(sourceDSN)
	if err != nil {
		slog.Error("parse source dsn failed", "error", err)
		os.Exit(1)
	}
	pubTables, err := loadTables(ctx)
	if err != nil {
		slog.Error("load tables failed", "error", err)
		os.Exit(1)
	}
	kafkaWriter = &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(kafkaBrokers, ",")...),
		AllowAutoTopicCreation: true,
		BatchSize:              2000,
		BatchTimeout:           200 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		Balancer:               &kafka.Hash{},
		Async:                  false,
	}
	defer kafkaWriter.Close()
	cfg := cdcconfig.Config{
		Host:      srcConnConfig.Host,
		Port:      int(srcConnConfig.Port),
		Username:  srcConnConfig.User,
		Password:  srcConnConfig.Password,
		Database:  srcConnConfig.Database,
		DebugMode: false,
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "pg_cdc_kafka",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationUpdate,
				publication.OperationDelete,
			},
			Tables: pubTables,
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "kafka_slot",
			SlotActivityCheckerInterval: 3000,
		},
		Snapshot: cdcconfig.SnapshotConfig{Enabled: false},
		Heartbeat: cdcconfig.HeartbeatConfig{
			Enabled:  heartbeatEnabled,
			Interval: 5 * time.Minute,
		},
		Metric: cdcconfig.MetricConfig{Port: metricPort},
		Logger: cdcconfig.LoggerConfig{LogLevel: slog.LevelInfo},
	}
	slog.Info("cdc tables loaded", "count", len(pubTables))
	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector failed", "error", err)
		os.Exit(1)
	}
	defer connector.Close()
	slog.Info("cdc started")
	connector.Start(ctx)
}

func Handler(ctx *replication.ListenerContext) {
	var err error
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		if !cdcconfig.IsHeartbeatTable(msg.TableNamespace, msg.TableName) {
			err = writeKafkaMessage(context.Background(), msg.TableName, "insert", msg.Decoded)
		}
	case *format.Update:
		if !cdcconfig.IsHeartbeatTable(msg.TableNamespace, msg.TableName) {
			err = writeKafkaMessage(context.Background(), msg.TableName, "update", msg.NewDecoded)
		}
	case *format.Delete:
		if !cdcconfig.IsHeartbeatTable(msg.TableNamespace, msg.TableName) {
			err = writeKafkaMessage(context.Background(), msg.TableName, "delete", msg.OldDecoded)
		}
	}
	if err != nil {
		slog.Error("write kafka failed permanently, exiting", "error", err)
		os.Exit(1)
		return
	}
	if err := ctx.Ack(); err != nil {
		slog.Error("ack failed", "error", err)
	}
}

func writeKafkaMessage(ctx context.Context, table, operation string, payload map[string]interface{}) error {
	if payload == nil {
		return nil
	}
	payload = cloneMap(payload)
	payload["operation"] = operation
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	topic := topicPrefix + "." + table
	msg := kafka.Message{
		Topic: topic,
		Key:   buildKafkaKey(ctx, table, payload),
		Value: data,
		Headers: []kafka.Header{
			{Key: "table", Value: []byte(table)},
			{Key: "operation", Value: []byte(operation)},
		},
	}
	var lastErr error
	for i := 1; i <= kafkaRetryCount; i++ {
		err = kafkaWriter.WriteMessages(ctx, msg)
		if err == nil {
			return nil
		}
		lastErr = err
		slog.Warn("write kafka failed, retrying", "attempt", i, "max_retry", kafkaRetryCount, "topic", topic, "error", err)
		time.Sleep(kafkaRetryInterval)
	}
	return fmt.Errorf("write kafka failed after %d retries: %v", kafkaRetryCount, lastErr)
}

func loadTables(ctx context.Context) (publication.Tables, error) {
	query := `SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name IN ('ConversationNewDeleted')`
	rows, err := pgPool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pubTables publication.Tables
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		pubTables = append(pubTables, publication.Table{
			Name:            table,
			Schema:          schema,
			ReplicaIdentity: publication.ReplicaIdentityFull,
		})
	}
	return pubTables, nil
}

func getPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	if val, ok := primaryKeyCache.Load(table); ok {
		return val.([]string), nil
	}
	parts := strings.SplitN(table, ".", 2)
	schema := "public"
	tableName := table
	if len(parts) == 2 {
		schema = parts[0]
		tableName = parts[1]
	}
	query := `
SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
	ON tc.constraint_name = kcu.constraint_name
	AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
	AND tc.table_schema = $1
	AND tc.table_name = $2
ORDER BY kcu.ordinal_position
`
	rows, err := pgPool.Query(ctx, query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pks []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pks = append(pks, col)
	}
	primaryKeyCache.Store(table, pks)
	return pks, nil
}

func buildKafkaKey(ctx context.Context, table string, data map[string]interface{}) []byte {
	pks, err := getPrimaryKeys(ctx, table)
	if err != nil {
		slog.Error("get primary keys failed", "table", table, "error", err)
		return nil
	}
	if len(pks) == 0 {
		return nil
	}
	values := make([]string, 0, len(pks))
	for _, pk := range pks {
		v, ok := data[pk]
		if !ok {
			continue
		}
		values = append(values, fmt.Sprintf("%v", v))
	}
	if len(values) == 0 {
		return nil
	}
	return []byte(strings.Join(values, ":"))
}

func cloneMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src)+1)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
