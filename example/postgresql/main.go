package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/pq"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Message 表示一个待执行的数据库操作
type Message struct {
	Table   string                 // 表名（可能包含 schema）
	Action  string                 // "insert", "update", "delete"
	Data    map[string]interface{} // 新数据（insert/update）
	OldData map[string]interface{} // 旧数据（delete/update）
	Ack     func() error           // 确认函数
	Source  string
}

// pendingItem 用于批量处理时保存 SQL 和对应的 ack 函数
type pendingItem struct {
	query  *pgx.QueuedQuery
	ack    func() error
	source string
}

// 主键缓存：表名 -> 主键列名切片
var primaryKeyCache = sync.Map{}

func main() {
	var sourceDSN, targetDSN string
	var metricPort int
	var syncMode string
	var enableSnapshot bool

	flag.StringVar(&sourceDSN, "source", "", "源 PostgreSQL 连接 URL")
	flag.StringVar(&targetDSN, "target", "", "目标 PostgreSQL 连接 URL")
	flag.IntVar(&metricPort, "port", 8081, "metric server port")
	flag.StringVar(&syncMode, "mode", "", "sync mode: full for all tables, empty for preset tables")
	flag.BoolVar(&enableSnapshot, "snapshot", true, "enable snapshot: true/false")
	flag.Parse()

	if sourceDSN == "" || targetDSN == "" {
		fmt.Fprintf(os.Stderr, "错误: 必须提供 --source 和 --target 参数\n")
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	// 解析源库 DSN
	srcConnConfig, err := pgx.ParseConfig(sourceDSN)
	if err != nil {
		slog.Error("解析源库连接串失败", "error", err)
		os.Exit(1)
	}

	// 构建 CDC 配置
	cfg := config.Config{
		Host:     srcConnConfig.Host,
		Port:     int(srcConnConfig.Port),
		Username: srcConnConfig.User,
		Password: srcConnConfig.Password,
		Database: srcConnConfig.Database,
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{publication.Table{
				Name:            "Conversation",
				ReplicaIdentity: publication.ReplicaIdentityDefault,
				Schema:          "public",
			}},
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

	// 目标库连接池
	targetPool, err := pgxpool.New(ctx, targetDSN)
	if err != nil {
		slog.Error("连接目标库失败", "error", err)
		os.Exit(1)
	}
	defer targetPool.Close()

	if syncMode == "all" {
		handleFullSyncMode(ctx, &cfg)
	}

	messages := make(chan Message, 10000)
	go Produce(ctx, targetPool, messages)

	connector, err := cdc.NewConnector(ctx, cfg, FilteredMapper(messages))
	if err != nil {
		slog.Error("创建 CDC 连接器失败", "error", err)
		os.Exit(1)
	}
	connector.Start(ctx)
}

// FilteredMapper 将 CDC 事件转换为通用 Message
func FilteredMapper(messages chan Message) replication.ListenerFunc {
	return func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert:
			messages <- Message{
				Table:  msg.TableName,
				Action: "insert",
				Data:   msg.Decoded,
				Ack:    ctx.Ack,
				Source: "cdc",
			}
		case *format.Update:
			messages <- Message{
				Table:   msg.TableName,
				Action:  "update",
				Data:    msg.NewDecoded,
				OldData: msg.OldDecoded,
				Ack:     ctx.Ack,
				Source:  "cdc",
			}
		case *format.Delete:
			messages <- Message{
				Table:   msg.TableName,
				Action:  "delete",
				OldData: msg.OldDecoded,
				Ack:     ctx.Ack,
				Source:  "cdc",
			}
		case *format.Snapshot:
			handleSnapshot(ctx, messages)
		}
	}
}

// 修改 handleSnapshot 签名，增加 messages 通道参数
func handleSnapshot(ctx *replication.ListenerContext, messages chan<- Message) {
	msg, ok := ctx.Message.(*format.Snapshot)
	if !ok {
		slog.Error("handleSnapshot: 类型断言失败")
		return
	}

	switch msg.EventType {
	case format.SnapshotEventTypeBegin:
		log.Println("📸 快照开始")

	case format.SnapshotEventTypeData:
		messages <- Message{
			Table:  msg.Table,
			Action: "insert", // 快照数据视为插入
			Data:   msg.Data, // 直接使用 msg.Data，它包含整行字段
			Ack:    ctx.Ack,  // 重要：直接使用 ctx.Ack 作为确认函数
			Source: "snapshot",
		}

	case format.SnapshotEventTypeEnd:
		log.Println("📸 快照结束")
		// 确认快照结束事件
		if err := ctx.Ack(); err != nil {
			slog.Error("确认快照结束失败", "err", err)
		}
	}
}

// Produce 从 messages 通道读取事件，批量写入目标库
func Produce(ctx context.Context, w *pgxpool.Pool, messages <-chan Message) {
	const bulkSize = 10000
	queue := make([]pendingItem, 0, bulkSize)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case event := <-messages:
			// 根据事件类型构建 SQL
			sql, args, err := buildSQL(ctx, w, event)
			if err != nil {
				slog.Error("构建 SQL 失败", "table", event.Table, "action", event.Action, "error", err)
				// 不确认该消息，导致 CDC 可能重试？这里简单跳过
				continue
			}
			queue = append(queue, pendingItem{
				query:  &pgx.QueuedQuery{SQL: sql, Arguments: args},
				ack:    event.Ack,
				source: event.Source,
			})

			if len(queue) >= bulkSize {
				startTarget := time.Now()
				if err := flushBatch(ctx, w, queue); err != nil {
					slog.Error("批量写入失败", "error", err)
				}
				slog.Info("目标端写入耗时", "duration_ms", time.Since(startTarget).Milliseconds(), "batch_size", len(queue))
				queue = queue[:0]
			}

		case <-ticker.C:
			if len(queue) > 0 {
				if err := flushBatch(ctx, w, queue); err != nil {
					slog.Error("超时批量写入失败", "error", err)
				}
				queue = queue[:0]
			}
		}
	}
}

// flushBatch 执行批量 SQL，并逐一确认所有消息
func flushBatch(ctx context.Context, conn *pgxpool.Pool, items []pendingItem) error {
	if len(items) == 0 {
		return nil
	}

	// 统计来源
	snapshotCount := 0
	cdcCount := 0
	for _, it := range items {
		if it.source == "snapshot" {
			snapshotCount++
		} else {
			cdcCount++
		}
	}

	// 执行批量操作（代码不变）
	batch := &pgx.Batch{}
	for _, it := range items {
		batch.QueuedQueries = append(batch.QueuedQueries, it.query)
	}
	br := conn.SendBatch(ctx, batch)
	defer br.Close()

	var batchErr error
	for i := 0; i < len(items); i++ {
		if _, err := br.Exec(); err != nil {
			batchErr = errors.Join(batchErr, fmt.Errorf("第 %d 条 SQL 执行失败: %w", i, err))
		}
	}
	if batchErr != nil {
		return batchErr
	}

	// 确认消息
	for _, it := range items {
		if err := it.ack(); err != nil {
			slog.Error("确认消息失败", "error", err)
		}
	}

	// 分别打印日志
	if snapshotCount > 0 {
		slog.Info("snapshot write", "count", snapshotCount)
	}
	if cdcCount > 0 {
		slog.Info("cdc write", "count", cdcCount)
	}
	// 也可以合并打印
	// slog.Info("batch write", "snapshot", snapshotCount, "cdc", cdcCount)

	return nil
}

// buildSQL 根据事件类型动态生成 SQL 和参数
func buildSQL(ctx context.Context, conn *pgxpool.Pool, msg Message) (string, []interface{}, error) {
	switch msg.Action {
	case "insert", "update":
		// 对于 update，我们也视为 upsert，使用新数据
		return buildUpsertSQL(ctx, conn, msg.Table, msg.Data)
	case "delete":
		return buildDeleteSQL(ctx, conn, msg.Table, msg.OldData)
	default:
		return "", nil, fmt.Errorf("未知操作类型: %s", msg.Action)
	}
}

// buildUpsertSQL 生成 INSERT ... ON CONFLICT ... DO UPDATE 语句
func buildUpsertSQL(ctx context.Context, conn *pgxpool.Pool, table string, data map[string]interface{}) (string, []interface{}, error) {
	pks, err := getPrimaryKeys(ctx, conn, table)
	if err != nil {
		return "", nil, err
	}
	if len(pks) == 0 {
		return "", nil, fmt.Errorf("表 %s 没有主键，无法执行 upsert", table)
	}

	// 1. 引用表名
	schema, tableName := parseSchemaTable(table)
	quotedTable := pgx.Identifier{schema, tableName}.Sanitize()

	// 提取列并排序
	columns := make([]string, 0, len(data))
	for col := range data {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// 2. 引用列名
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = pgx.Identifier{col}.Sanitize()
	}

	// 占位符
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	// 引用主键列
	quotedPKs := make([]string, len(pks))
	for i, pk := range pks {
		quotedPKs[i] = pgx.Identifier{pk}.Sanitize()
	}
	conflictCols := strings.Join(quotedPKs, ", ")

	// 构建 INSERT 部分
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET ",
		quotedTable,
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "),
		conflictCols,
	)

	// SET 子句
	sets := make([]string, len(quotedColumns))
	for i, col := range quotedColumns {
		sets[i] = fmt.Sprintf("%s = EXCLUDED.%s", col, col)
	}
	sql += strings.Join(sets, ", ")

	// 参数
	args := make([]interface{}, len(columns))
	for i, col := range columns {
		args[i] = data[col]
	}
	return sql, args, nil
}

// buildDeleteSQL 生成 DELETE FROM table WHERE pk1=$1 AND pk2=$2 ...
func buildDeleteSQL(ctx context.Context, conn *pgxpool.Pool, table string, oldData map[string]interface{}) (string, []interface{}, error) {
	pks, err := getPrimaryKeys(ctx, conn, table)
	if err != nil {
		return "", nil, err
	}
	if len(pks) == 0 {
		return "", nil, fmt.Errorf("表 %s 没有主键，无法执行 delete", table)
	}

	// 引用表名
	schema, tableName := parseSchemaTable(table)
	quotedTable := pgx.Identifier{schema, tableName}.Sanitize()

	// 引用主键列
	quotedPKs := make([]string, len(pks))
	for i, pk := range pks {
		quotedPKs[i] = pgx.Identifier{pk}.Sanitize()
	}

	// 构建 WHERE 条件
	conditions := make([]string, len(pks))
	args := make([]interface{}, len(pks))
	for i, pk := range pks {
		conditions[i] = fmt.Sprintf("%s = $%d", quotedPKs[i], i+1)
		val, ok := oldData[pk]
		if !ok {
			return "", nil, fmt.Errorf("旧数据中缺少主键列 %s", pk)
		}
		args[i] = val
	}
	whereClause := strings.Join(conditions, " AND ")
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", quotedTable, whereClause)

	return sql, args, nil
}

// getPrimaryKeys 查询表的所有主键列名（按顺序），结果缓存
func getPrimaryKeys(ctx context.Context, conn *pgxpool.Pool, table string) ([]string, error) {
	// 尝试从缓存获取
	if pks, ok := primaryKeyCache.Load(table); ok {
		return pks.([]string), nil
	}

	// 解析 schema 和表名
	schema, name := parseSchemaTable(table)

	// 查询 information_schema，按 ordinal_position 排序
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
	rows, err := conn.Query(ctx, query, schema, name)
	if err != nil {
		return nil, fmt.Errorf("查询主键失败: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(pks) == 0 {
		return nil, fmt.Errorf("表 %s 没有主键", table)
	}

	// 存入缓存
	primaryKeyCache.Store(table, pks)
	return pks, nil
}

// parseSchemaTable 将 "schema.table" 拆分为 schema 和 table，默认为 public
func parseSchemaTable(fullName string) (schema, table string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
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
