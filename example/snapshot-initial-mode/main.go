package main

import (
	"context"
	"flag"
	"fmt"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"log/slog"
	"os"
	"time"
)

// 🔥 全局声明 targetPool，所有函数都能访问
var targetPool *pgxpool.Pool

func main() {
	var metricPort int
	var syncMode string
	var enableSnapshot bool
	flag.IntVar(&metricPort, "port", 8081, "metric server port")
	flag.StringVar(&syncMode, "mode", "", "sync mode: full for all tables, empty for preset tables")
	flag.BoolVar(&enableSnapshot, "snapshot", true, "enable snapshot: true/false (default true)")
	flag.Parse()

	ctx := context.Background()

	// 🔥 初始化全局连接池
	var err error
	targetPool, err = pgxpool.New(ctx, "postgres://postgres:cPhGLp2lg1CHD5420d@staging20241109.cmg2ypxvbvye.us-east-2.rds.amazonaws.com:5432/sit2")
	if err != nil {
		slog.Error("连接目标库失败", "err", err)
		os.Exit(1)
	}
	defer targetPool.Close()

	cfg := config.Config{
		Host:      "staging20241109.cmg2ypxvbvye.us-east-2.rds.amazonaws.com",
		Port:      5432,
		Username:  "postgres",
		Password:  "cPhGLp2lg1CHD5420d",
		Database:  "staging",
		DebugMode: false,
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{
				{
					Name:            "Conversation_bak",
					ReplicaIdentity: publication.ReplicaIdentityFull,
					Schema:          "public",
				},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot",
			SlotActivityCheckerInterval: 3000,
		},
		Snapshot: config.SnapshotConfig{
			Enabled:           enableSnapshot,
			Mode:              config.SnapshotModeInitial,
			ChunkSize:         5000,
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Metric: config.MetricConfig{
			Port: metricPort,
		},
		Logger: config.LoggerConfig{
			LogLevel: slog.LevelInfo,
		},
	}
	if syncMode == "all" {
		handleFullSyncMode(ctx, &cfg)
	}

	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}
	defer connector.Close()
	connector.Start(ctx)
}

func Handler(ctx *replication.ListenerContext) {
	var sqlStr string

	switch msg := ctx.Message.(type) {
	case *format.Insert:
		slog.Info("insert message received", "new", msg.Decoded)
	case *format.Delete:
		slog.Info("delete message received", "old", msg.OldDecoded)
	case *format.Update:
		slog.Info("update message received", "new", msg.NewDecoded, "old", msg.OldDecoded)
	case *format.Snapshot:
		handleSnapshot(msg)
		return
	}

	if sqlStr != "" {
		// 🔥 现在可以正常访问 targetPool 了
		_, err := targetPool.Exec(context.Background(), sqlStr)
		fmt.Println(sqlStr)
		if err != nil {
			slog.Error("执行SQL失败", "sql", sqlStr, "err", err)
		}
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}

func handleSnapshot(s *format.Snapshot) {
	switch s.EventType {
	case format.SnapshotEventTypeBegin:
		log.Printf("📸 SNAPSHOT BEGIN")

	case format.SnapshotEventTypeData:
		sql := s.Data["batch_sql"].(string)
		// 🔥 现在可以正常访问 targetPool 了
		_, err := targetPool.Exec(context.Background(), sql)
		if err != nil {
			slog.Error("快照批量执行失败", "err", err)
		}

	case format.SnapshotEventTypeEnd:
		log.Printf("📸 SNAPSHOT END")
	}
}

func handleFullSyncMode(ctx context.Context, cfg *config.Config) {
	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		slog.Error("create pq connection failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	query := `SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' and table_name not in ('cdc_snapshot_job','cdc_snapshot_chunks','table_primary_keys');`
	pwq_tables := conn.Exec(ctx, query)
	pwq_results, err := pwq_tables.ReadAll()
	if err != nil {
		slog.Error("query tables failed", "error", err)
		os.Exit(1)
	}
	var pubTables publication.Tables
	for _, res := range pwq_results {
		for _, row := range res.Rows {
			pubTable := publication.Table{
				Name:            string(row[1]),
				ReplicaIdentity: publication.ReplicaIdentityFull,
				Schema:          string(row[0]),
			}
			pubTables = append(pubTables, pubTable)
		}
	}
	cfg.Publication.Tables = pubTables
}
