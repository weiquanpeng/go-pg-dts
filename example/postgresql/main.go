package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/weiquanpeng/go-pg-dts/pq"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	cdc "github.com/weiquanpeng/go-pg-dts"
	"github.com/weiquanpeng/go-pg-dts/config"
	"github.com/weiquanpeng/go-pg-dts/pq/message/format"
	"github.com/weiquanpeng/go-pg-dts/pq/publication"
	"github.com/weiquanpeng/go-pg-dts/pq/replication"
	"github.com/weiquanpeng/go-pg-dts/pq/slot"
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

type pendingItem struct {
	query  *pgx.QueuedQuery
	ack    func() error
	source string
	msg    *Message // 新增：保存原始消息，用于快照批量优化
}

// 主键缓存：表名 -> 主键列名切片
var primaryKeyCache = sync.Map{}

func main() {
	var sourceDSN, targetDSN string
	var metricPort int
	var snapshotFlag string
	var chunkSize int
	var publicationName, slotName string
	var heartbeatEnabled bool

	flag.StringVar(&sourceDSN, "source", "", "源 PostgreSQL 连接 URL")
	flag.StringVar(&targetDSN, "target", "", "目标 PostgreSQL 连接 URL")
	flag.IntVar(&metricPort, "port", 8081, "metric server port")
	flag.StringVar(&snapshotFlag, "snapshot", "true", "true=全量+增量, false=仅增量, only=仅全量（不创建复制槽，跑完退出）")
	flag.IntVar(&chunkSize, "chunksize", 5000, "快照分块大小、通道缓冲区大小、批量写入大小")
	flag.StringVar(&publicationName, "publication", "cdc_publication", "源库 publication 名称")
	// 该名字同时是快照任务的标识（cdc_snapshot_job.slot_name），换名会重新执行一次全量
	flag.StringVar(&slotName, "slot", "cdc_slot", "源库复制槽名称，同时作为快照任务标识，同一实例内需唯一")
	flag.BoolVar(&heartbeatEnabled, "heartbeat", true, "是否启用 CDC heartbeat（每 5 分钟更新一次）")
	flag.Parse()

	if sourceDSN == "" || targetDSN == "" {
		fmt.Fprintf(os.Stderr, "错误: 必须提供 --source 和 --target 参数\n")
		flag.Usage()
		os.Exit(1)
	}

	snapshotEnabled, snapshotMode, err := parseSnapshotFlag(snapshotFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		flag.Usage()
		os.Exit(1)
	}
	snapshotOnly := snapshotMode == config.SnapshotModeSnapshotOnly

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
			Name:              publicationName,
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationUpdate,
			},
			// Tables 留空，由 loadPublicationTables 从源库查询后填充
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        slotName,
			SlotActivityCheckerInterval: 3000,
		},
		Snapshot: config.SnapshotConfig{
			Enabled:           snapshotEnabled,
			Mode:              snapshotMode,
			ChunkSize:         int64(chunkSize),
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Heartbeat: config.HeartbeatConfig{
			Enabled:  heartbeatEnabled && !snapshotOnly,
			Interval: 5 * time.Minute,
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

	if cfg.Heartbeat.Enabled {
		if err := ensureTargetHeartbeatTable(ctx, targetPool); err != nil {
			slog.Error("初始化目标端 heartbeat 表失败", "error", err)
			os.Exit(1)
		}
	}

	loadPublicationTables(ctx, &cfg)
	ensureTargetPrimaryKeys(ctx, targetPool, cfg.Publication.Tables)

	// snapshot_only 模式取表走 Snapshot.Tables 分支，与 publication 无关。
	// initial 模式由 Config.GetSnapshotTables 自动从全量表清单排除 cdc_heartbeat。
	if snapshotOnly {
		cfg.Snapshot.Tables = cfg.Publication.Tables
		// 快照任务标识固定用 --slot，否则库会退化成 snapshot_only_<数据库名>
		cfg.Snapshot.ID = slotName
	}

	messages := make(chan Message, chunkSize)
	produceDone := make(chan struct{})
	go Produce(ctx, targetPool, chunkSize, messages, produceDone)

	connector, err := cdc.NewConnector(ctx, cfg, FilteredMapper(messages))
	if err != nil {
		slog.Error("创建 CDC 连接器失败", "error", err)
		os.Exit(1)
	}
	connector.Start(ctx)

	// snapshot_only 模式下 Start 在快照结束后返回，此时快照 worker 已停止写入，
	// 关闭通道让 Produce 落盘残余数据，避免最后不满一批的行随进程退出而丢失。
	// CDC 模式 Start 不会正常返回，且 stream 仍可能写入 messages，因此不能关闭。
	if snapshotOnly {
		close(messages)
		<-produceDone
		slog.Info("快照结束，残余数据已落盘")
	}
}

func ensureTargetHeartbeatTable(ctx context.Context, conn *pgxpool.Pool) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS public.cdc_heartbeat (
			id integer PRIMARY KEY,
			updated_at timestamptz NOT NULL
		)
	`)
	return err
}

// parseSnapshotFlag 解析 --snapshot 取值：
//   - true：全量 + 增量，创建复制槽后常驻
//   - false：跳过全量，仅增量
//   - only：仅全量，不创建复制槽，跑完即退出
func parseSnapshotFlag(v string) (enabled bool, mode config.SnapshotMode, err error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true":
		return true, config.SnapshotModeInitial, nil
	case "false":
		return false, config.SnapshotModeInitial, nil
	case "only":
		return true, config.SnapshotModeSnapshotOnly, nil
	default:
		return false, "", fmt.Errorf("--snapshot 取值无效: %q，可选 true / false / only", v)
	}
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

// Produce 从 messages 通道读取事件，批量写入目标库。
// messages 被关闭后会先落盘残余数据，再关闭 done 通知调用方收尾完成。
func Produce(ctx context.Context, w *pgxpool.Pool, bulkSize int, messages <-chan Message, done chan<- struct{}) {
	defer close(done)

	queue := make([]pendingItem, 0, bulkSize)

	// 攒不满一批时的兜底落盘只在通道空闲 idleFlush 之后触发。
	// 无条件的固定周期 flush 会落在快照 worker 读取下一个分块的间隔里（分块越大间隔越长），
	// 把攒到一半的队列切成碎批，导致大量远小于 bulkSize 的 COPY 和事务。
	const idleFlush = time.Second
	ticker := time.NewTicker(idleFlush / 4)
	defer ticker.Stop()
	lastRecv := time.Now()

	flush := func(reason string) {
		n := len(queue)
		start := time.Now()
		if err := flushBatch(ctx, w, queue); err != nil {
			slog.Error("批量写入失败", "reason", reason, "error", err)
			os.Exit(1)
		}
		queue = queue[:0]
		slog.Info("目标端写入耗时", "duration_ms", time.Since(start).Milliseconds(), "batch_size", n, "reason", reason)
	}

	for {
		select {
		case event, ok := <-messages:
			if !ok {
				// 通道关闭且已取空，落盘最后不满一批的数据后退出
				if len(queue) > 0 {
					flush("eof")
				}
				return
			}
			lastRecv = time.Now()

			// 根据事件类型构建 SQL
			sql, args, err := buildSQL(ctx, w, event)
			if err != nil {
				slog.Error("构建 SQL 失败，迁移终止",
					"table", event.Table,
					"action", event.Action,
					"source", event.Source,
					"error", err,
				)
				// 不能跳过该事件：后续事件成功 ACK 会跨过当前 WAL 位置，造成永久数据丢失。
				// continue
				os.Exit(1)
			}
			queue = append(queue, pendingItem{
				query:  &pgx.QueuedQuery{SQL: sql, Arguments: args},
				ack:    event.Ack,
				source: event.Source,
				msg:    &event, // 新增
			})

			if len(queue) >= bulkSize {
				flush("full")
			}

		case <-ticker.C:
			if len(queue) > 0 && time.Since(lastRecv) >= idleFlush {
				flush("idle")
			}
		}
	}
}

func flushBatch(ctx context.Context, conn *pgxpool.Pool, items []pendingItem) error {
	if len(items) == 0 {
		return nil
	}

	// 检查是否全是快照消息
	allSnapshot := true
	for _, it := range items {
		if it.source != "snapshot" {
			allSnapshot = false
			break
		}
	}

	if allSnapshot {
		return flushBatchSnapshotMultiRow(ctx, conn, items)
	}
	return flushBatchGeneric(ctx, conn, items)
}

func flushBatchGeneric(ctx context.Context, conn *pgxpool.Pool, items []pendingItem) error {
	batch := &pgx.Batch{}
	for _, it := range items {
		batch.QueuedQueries = append(batch.QueuedQueries, it.query)
	}
	br := conn.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(items); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("第 %d 条 SQL 失败: %w", i, err)
		}
	}

	for _, it := range items {
		it.ack()
	}

	cdcCount := 0
	for _, it := range items {
		if it.source == "cdc" {
			cdcCount++
		}
	}
	if cdcCount > 0 {
		slog.Info("cdc write", "count", cdcCount)
	}
	return nil
}

// 快照专用 COPY + 事务（比多行 INSERT 更快）
func flushBatchSnapshotMultiRow(ctx context.Context, conn *pgxpool.Pool, items []pendingItem) error {
	// 按表分组
	groups := make(map[string][]*Message)
	for _, it := range items {
		if it.msg == nil {
			return flushBatchGeneric(ctx, conn, items) // fallback
		}
		groups[it.msg.Table] = append(groups[it.msg.Table], it.msg)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	const maxRowsPerCopy = 5000 // COPY 批次大小（可调，不受参数限制）

	for table, msgs := range groups {
		// 检查主键是否存在（只校验，不用于 SQL）
		if _, err := getPrimaryKeys(ctx, conn, table); err != nil {
			return err
		}
		colTypes, err := getColumnTypes(ctx, conn, table)
		if err != nil {
			return err
		}

		// 获取列名列表（以第一条消息为准，排序保证顺序稳定）
		columns := getSortedColumns(msgs[0].Data) // 辅助函数见下方

		// 将 msgs 拆分成多个子批次进行 COPY
		for i := 0; i < len(msgs); i += maxRowsPerCopy {
			end := i + maxRowsPerCopy
			if end > len(msgs) {
				end = len(msgs)
			}
			sub := msgs[i:end]

			// 准备 COPY 数据
			copyData := make([][]interface{}, len(sub))
			for idx, msg := range sub {
				row := make([]interface{}, len(columns))
				for j, col := range columns {
					val := msg.Data[col]
					typ := colTypes[col]
					if typ == "json" || typ == "jsonb" {
						if val == nil {
							row[j] = nil
						} else {
							jsonBytes, err := json.Marshal(val)
							if err != nil {
								return fmt.Errorf("序列化 JSON 列 %s 失败: %w", col, err)
							}
							row[j] = string(jsonBytes)
						}
					} else {
						row[j] = val
					}
				}
				copyData[idx] = row
			}

			// 执行 COPY
			schema, tableName := parseSchemaTable(table)
			_, err = tx.CopyFrom(
				ctx,
				pgx.Identifier{schema, tableName},
				columns,
				pgx.CopyFromSlice(len(copyData), func(i int) ([]interface{}, error) {
					return copyData[i], nil
				}),
			)
			if err != nil {
				return fmt.Errorf("COPY 到表 %s 失败: %w", table, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	for _, it := range items {
		it.ack()
	}
	slog.Info("snapshot (COPY)", "rows", len(items))
	return nil
}

// 辅助函数：获取排序后的列名切片
func getSortedColumns(data map[string]interface{}) []string {
	cols := make([]string, 0, len(data))
	for col := range data {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
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

var columnTypeCache = sync.Map{} // key: "schema.table" -> map[string]string

// getColumnTypes 查询目标表的列名到数据类型的映射，并缓存
func getColumnTypes(ctx context.Context, conn *pgxpool.Pool, table string) (map[string]string, error) {
	if cached, ok := columnTypeCache.Load(table); ok {
		return cached.(map[string]string), nil
	}
	schema, name := parseSchemaTable(table)
	rows, err := conn.Query(ctx, `
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
    `, schema, name)
	if err != nil {
		return nil, fmt.Errorf("查询列类型失败: %w", err)
	}
	defer rows.Close()
	colTypes := make(map[string]string)
	for rows.Next() {
		var col, typ string
		if err := rows.Scan(&col, &typ); err != nil {
			return nil, err
		}
		colTypes[col] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	columnTypeCache.Store(table, colTypes)
	return colTypes, nil
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
	colTypes, err := getColumnTypes(ctx, conn, table)
	if err != nil {
		return "", nil, fmt.Errorf("获取目标表列类型失败: %w", err)
	}

	// 构建参数，对 JSON 列进行特殊处理
	args := make([]interface{}, len(columns))
	for i, col := range columns {
		val := data[col]
		typ := colTypes[col]
		if typ == "json" || typ == "jsonb" {
			if val == nil {
				args[i] = nil
			} else {
				// 使用 json.Marshal 将任意值（通常为字符串或 map）正确编码为 JSON 字符串
				jsonBytes, err := json.Marshal(val)
				if err != nil {
					return "", nil, fmt.Errorf("序列化 JSON 列 %s 失败: %w", col, err)
				}
				args[i] = string(jsonBytes)
			}
		} else {
			// 非 JSON 列，保持原值
			args[i] = val
		}
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

// ensureTargetPrimaryKeys 在迁移开始前校验目标库每张表都有主键，缺一张就退出。
// 放在启动阶段是因为写入路径依赖主键生成 UPSERT，等跑到那里才报错会先把一部分数据写进去。
// 一次列出所有缺主键的表，避免一张一张试。
func ensureTargetPrimaryKeys(ctx context.Context, conn *pgxpool.Pool, tables publication.Tables) {
	var missing []string
	for _, t := range tables {
		full := t.Schema + "." + t.Name
		if _, err := getPrimaryKeys(ctx, conn, full); err != nil {
			missing = append(missing, fmt.Sprintf("%s (%v)", full, err))
		}
	}
	if len(missing) > 0 {
		slog.Error("目标库存在没有主键的表，迁移终止", "count", len(missing))
		for _, m := range missing {
			slog.Error("  缺少主键", "table", m)
		}
		os.Exit(1)
	}
	slog.Info("目标库主键校验通过", "tables", len(tables))
}

// parseSchemaTable 将 "schema.table" 拆分为 schema 和 table，默认为 public
func parseSchemaTable(fullName string) (schema, table string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}

// loadPublicationTables 从源库查询待迁移的表，是表清单的唯一来源。
// 调整迁移范围请修改下方 query 的 WHERE 条件。
func loadPublicationTables(ctx context.Context, cfg *config.Config) {
	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		slog.Error("create pq connection failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	query := `SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' and table_name in ('Conversation');`
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
	// 表清单为空会在 NewConnector 的配置校验里报错，这里提前拦下给出可定位的提示
	if len(pubTables) == 0 {
		slog.Error("未查询到任何待迁移的表，请检查 loadPublicationTables 中的查询条件")
		os.Exit(1)
	}
	slog.Info("待迁移表清单已加载", "count", len(pubTables))
	cfg.Publication.Tables = pubTables
}
