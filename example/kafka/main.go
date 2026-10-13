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
	pendingCH       = make(chan pendingMessage, pendingQueueSize)
)

const (
	kafkaRetryCount    = 5
	kafkaRetryInterval = 1 * time.Second
	// 攒批参数，先到者触发。批次越大吞吐越高，但进程崩溃时重投的消息也越多。
	flushBatchSize   = 500
	flushInterval    = 50 * time.Millisecond
	pendingQueueSize = 4096
)

// pendingMessage 是批量队列里的一个单元。msg 为 nil 表示该事件不需要写 Kafka
// （心跳表、Relation 等），但它仍要占一个队列位置：ACK 必须等排在它前面的消息
// 真正落盘后再按顺序发出，否则会跨过尚未写入的消息推进复制槽位点。
type pendingMessage struct {
	msg *kafka.Message
	ack func() error
}

func main() {
	var sourceDSN, kafkaBrokers string
	var metricPort int
	var heartbeatEnabled bool
	var publishViaPartitionRoot bool
	flag.StringVar(&sourceDSN, "source", "", "source postgres dsn")
	flag.StringVar(&kafkaBrokers, "brokers", "127.0.0.1:9092", "kafka brokers")
	flag.StringVar(&topicPrefix, "topic-prefix", "cdc", "kafka topic prefix")
	flag.IntVar(&metricPort, "port", 2112, "metric port")
	flag.BoolVar(&heartbeatEnabled, "heartbeat", true, "enable CDC heartbeat every 5 minutes")
	// 分区表场景：白名单只需写分区父表，publication 以父表名义发布事件，
	// 启动时自动把父表下所有叶子分区设为 REPLICA IDENTITY FULL（需源端 PG 13+）
	flag.BoolVar(&publishViaPartitionRoot, "publish-via-partition-root", false, "以分区父表名义发布 CDC 事件")
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
		// kafka-go 按分区各自攒批，一次交付的消息散到多个分区后往往填不满
		// BatchSize，只能等计时器。这里取小值，避免每次刷出都白等一个批次窗口。
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Balancer:     &kafka.Hash{},
		Async:        false,
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
			CreateIfNotExists:       true,
			Name:                    "pg_cdc_kafka",
			PublishViaPartitionRoot: publishViaPartitionRoot,
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
	go flushWorker(ctx)
	slog.Info("cdc started")
	connector.Start(ctx)
}

// Handler 只负责组装消息并入队，不写 Kafka 也不 ACK。逐条同步写会让 kafka-go
// 每条都空等一次 BatchTimeout，而 process 循环是串行的，吞吐会被压到每秒几条。
func Handler(ctx *replication.ListenerContext) {
	var (
		kafkaMsg *kafka.Message
		err      error
	)
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		if !cdcconfig.IsHeartbeatTable(msg.TableNamespace, msg.TableName) {
			kafkaMsg, err = buildKafkaMessage(context.Background(), msg.TableName, "insert", msg.Decoded)
		}
	case *format.Update:
		if !cdcconfig.IsHeartbeatTable(msg.TableNamespace, msg.TableName) {
			kafkaMsg, err = buildKafkaMessage(context.Background(), msg.TableName, "update", msg.NewDecoded)
		}
	case *format.Delete:
		if !cdcconfig.IsHeartbeatTable(msg.TableNamespace, msg.TableName) {
			kafkaMsg, err = buildKafkaMessage(context.Background(), msg.TableName, "delete", msg.OldDecoded)
		}
	}
	if err != nil {
		slog.Error("build kafka message failed, exiting", "error", err)
		os.Exit(1)
		return
	}
	// 队列满时在此阻塞，背压沿 process 循环一路传导回 walsender。
	pendingCH <- pendingMessage{msg: kafkaMsg, ack: ctx.Ack}
}

// flushWorker 把队列里的消息攒成批次写入 Kafka，整批落盘后统一推进复制槽位点。
func flushWorker(ctx context.Context) {
	batch := make([]kafka.Message, 0, flushBatchSize)
	var lastAck func() error

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if lastAck == nil {
			return
		}
		if len(batch) > 0 {
			if err := writeKafkaBatch(ctx, batch); err != nil {
				slog.Error("write kafka failed permanently, exiting", "error", err)
				os.Exit(1)
			}
		}
		// 只 ACK 本批最后一条：消息自 WAL 起每一跳都保持顺序，位点推进又是单调的，
		// 确认末条即覆盖同批次中排在它之前的全部消息。写失败时不 ACK，整批留给
		// 进程重启后重新投递。
		if err := lastAck(); err != nil {
			slog.Error("ack failed", "error", err)
		}
		batch = batch[:0]
		lastAck = nil
	}

	for {
		select {
		case pending := <-pendingCH:
			if pending.msg != nil {
				batch = append(batch, *pending.msg)
			}
			lastAck = pending.ack
			if len(batch) >= flushBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func buildKafkaMessage(ctx context.Context, table, operation string, payload map[string]interface{}) (*kafka.Message, error) {
	if payload == nil {
		return nil, nil
	}
	payload = cloneMap(payload)
	payload["operation"] = operation
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &kafka.Message{
		Topic: topicPrefix + "." + table,
		Key:   buildKafkaKey(ctx, table, payload),
		Value: data,
		Headers: []kafka.Header{
			{Key: "table", Value: []byte(table)},
			{Key: "operation", Value: []byte(operation)},
		},
	}, nil
}

func writeKafkaBatch(ctx context.Context, batch []kafka.Message) error {
	var lastErr error
	for i := 1; i <= kafkaRetryCount; i++ {
		err := kafkaWriter.WriteMessages(ctx, batch...)
		if err == nil {
			return nil
		}
		lastErr = err
		slog.Warn("write kafka failed, retrying", "attempt", i, "max_retry", kafkaRetryCount, "size", len(batch), "error", err)
		time.Sleep(kafkaRetryInterval)
	}
	return fmt.Errorf("write kafka failed after %d retries: %v", kafkaRetryCount, lastErr)
}

func loadTables(ctx context.Context) (publication.Tables, error) {
	query := `SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name IN ('PromptI18N')`
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
