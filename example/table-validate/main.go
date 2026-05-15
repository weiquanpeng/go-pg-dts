package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TableInfo struct {
	Name          string
	PrimaryKeys   []string
	EstimatedRows int64
}

type pkTuple struct {
	Value  interface{}
	String string
}

type StatsCollector struct {
	mu         sync.Mutex
	checked    map[string]int64
	mismatches map[string][]string
}

func NewStatsCollector() *StatsCollector {
	return &StatsCollector{
		checked:    make(map[string]int64),
		mismatches: make(map[string][]string),
	}
}

func (sc *StatsCollector) AddChecked(table string, n int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.checked[table] += n
}

func (sc *StatsCollector) AddMismatch(table string, pk string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.mismatches[table] = append(sc.mismatches[table], pk)
}

func (sc *StatsCollector) GetChecked() map[string]int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	copied := make(map[string]int64, len(sc.checked))
	for k, v := range sc.checked {
		copied[k] = v
	}
	return copied
}

func (sc *StatsCollector) GetMismatchesCopy() map[string][]string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	copied := make(map[string][]string, len(sc.mismatches))
	for k, v := range sc.mismatches {
		copied[k] = append([]string(nil), v...)
	}
	return copied
}

var wg sync.WaitGroup

func main() {
	var sourceDSN, targetDSN string
	var tables sliceFlag
	var tablesSQL string
	var batchSize int
	var interval time.Duration
	var workers int
	var ratePerSec int

	flag.StringVar(&sourceDSN, "source", "", "源数据库 DSN")
	flag.StringVar(&targetDSN, "target", "", "目标数据库 DSN")
	flag.Var(&tables, "tables", "要校验的表名（可多次指定）")
	flag.StringVar(&tablesSQL, "tables-sql", "", "执行自定义 SQL 获取表名列表")
	flag.IntVar(&batchSize, "batch", 1000, "每批校验的主键数量")
	flag.DurationVar(&interval, "interval", 30*time.Second, "统计报告输出间隔")
	flag.IntVar(&workers, "workers", 4, "并发校验的协程数")
	flag.IntVar(&ratePerSec, "rate", 1000, "每张表每秒最多校验的行数")
	flag.Parse()

	if sourceDSN == "" || targetDSN == "" {
		fmt.Fprintf(os.Stderr, "必须提供 --source 和 --target\n")
		flag.Usage()
		os.Exit(1)
	}

	var tableNames []string
	if tablesSQL != "" {
		ctxTemp := context.Background()
		tempPool, err := pgxpool.New(ctxTemp, sourceDSN)
		if err != nil {
			log.Fatalf("临时连接源库失败: %v", err)
		}
		defer tempPool.Close()

		rows, err := tempPool.Query(ctxTemp, tablesSQL)
		if err != nil {
			log.Fatalf("执行 tables-sql 失败: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var tbl string
			if err := rows.Scan(&tbl); err != nil {
				log.Fatalf("扫描表名失败: %v", err)
			}
			if tbl != "" {
				tableNames = append(tableNames, tbl)
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("读取表名失败: %v", err)
		}
		if len(tableNames) == 0 {
			log.Fatal("tables-sql 未返回任何表名")
		}
		log.Printf("通过 SQL 获取到 %d 张表: %v", len(tableNames), tableNames)
	} else if len(tables) > 0 {
		tableNames = tables
	} else {
		fmt.Fprintf(os.Stderr, "必须提供 --tables 或 --tables-sql\n")
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("收到退出信号，正在关闭...")
		cancel()
	}()

	srcPool, err := pgxpool.New(ctx, sourceDSN)
	if err != nil {
		log.Fatalf("连接源库失败: %v", err)
	}
	defer srcPool.Close()

	tgtPool, err := pgxpool.New(ctx, targetDSN)
	if err != nil {
		log.Fatalf("连接目标库失败: %v", err)
	}
	defer tgtPool.Close()

	var tableInfos []TableInfo
	for _, tbl := range tableNames {
		pks, err := getPrimaryKeys(ctx, srcPool, tbl)
		if err != nil {
			log.Fatalf("获取表 %s 主键失败: %v", tbl, err)
		}
		if len(pks) != 1 {
			log.Fatalf("表 %s 主键列数不为1（当前为 %d），本工具仅支持单列主键", tbl, len(pks))
		}
		estRows, err := getEstimatedRowCount(ctx, srcPool, tbl)
		if err != nil {
			log.Printf("表 %s 获取预估行数失败: %v (将不显示进度百分比)", tbl, err)
			estRows = 0
		}
		tableInfos = append(tableInfos, TableInfo{
			Name:          tbl,
			PrimaryKeys:   pks,
			EstimatedRows: estRows,
		})
		log.Printf("表 %s 主键: %v, 预估行数: %d", tbl, pks, estRows)
	}

	stats := NewStatsCollector()
	sem := make(chan struct{}, workers)

	for _, ti := range tableInfos {
		wg.Add(1)
		go func(ti TableInfo) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			log.Printf("开始校验表 %s（每批 %d 行，目标速率 %d 行/秒）", ti.Name, batchSize, ratePerSec)

			var lastPK interface{} = nil
			throttle := time.NewTicker(time.Second / time.Duration(ratePerSec))
			defer throttle.Stop()

			for {
				select {
				case <-ctx.Done():
					log.Printf("表 %s 校验协程退出", ti.Name)
					return
				default:
				}

				pkTuples, nextPK, err := fetchPrimaryKeysBatch(ctx, srcPool, ti, lastPK, batchSize)
				if err != nil {
					log.Printf("表 %s 获取主键批次失败: %v", ti.Name, err)
					time.Sleep(5 * time.Second)
					continue
				}

				if len(pkTuples) == 0 {
					log.Printf("表 %s 全表扫描完成（无更多数据），校验结束", ti.Name)
					break
				}

				consistent, err := batchCheck(ctx, srcPool, tgtPool, ti, pkTuples)
				if err != nil {
					log.Printf("表 %s 批量比对失败: %v", ti.Name, err)
					time.Sleep(5 * time.Second)
					continue
				}

				if consistent {
					stats.AddChecked(ti.Name, int64(len(pkTuples)))
				} else {
					diffPKs, err := findDiffPKs(ctx, srcPool, tgtPool, ti, pkTuples)
					if err != nil {
						log.Printf("表 %s 查找差异行失败: %v", ti.Name, err)
					} else {
						stats.AddChecked(ti.Name, int64(len(pkTuples)))
						for _, pk := range diffPKs {
							stats.AddMismatch(ti.Name, pk.String)
						}
					}
				}

				lastPK = nextPK

				select {
				case <-throttle.C:
				case <-ctx.Done():
					return
				}

				if len(pkTuples) < batchSize {
					log.Printf("表 %s 全表扫描完成（最后一批不足 %d 行），校验结束", ti.Name, batchSize)
					break
				}
			}
		}(ti)
	}

	go func() {
		wg.Wait()
		log.Println("所有表校验完成，程序即将退出...")
		cancel()
	}()

	reportTicker := time.NewTicker(interval)
	defer reportTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("程序退出")
			return
		case <-reportTicker.C:
			checked := stats.GetChecked()
			mismatches := stats.GetMismatchesCopy()
			printProgressReport(checked, mismatches, tableInfos)
		}
	}
}

// 增强版进度报告：包含不一致率和主键列表（即使无不一致也显示）
func printProgressReport(checked map[string]int64, mismatches map[string][]string, tableInfos []TableInfo) {
	log.Println("========== 校验进度报告 ==========")
	estMap := make(map[string]int64)
	for _, ti := range tableInfos {
		estMap[ti.Name] = ti.EstimatedRows
	}

	for table, cnt := range checked {
		est := estMap[table]
		if est > 0 {
			percent := float64(cnt) / float64(est) * 100
			log.Printf("表 %s: 已校验 %d / %d 行 (%.2f%%)", table, cnt, est, percent)
		} else {
			log.Printf("表 %s: 已校验 %d 行 (无预估行数)", table, cnt)
		}

		// 不一致信息
		mismatchList := mismatches[table]
		mismatchCount := len(mismatchList)
		if mismatchCount == 0 {
			log.Printf("  无不一致行")
		} else {
			rate := float64(mismatchCount) / float64(cnt) * 100
			log.Printf("  累计发现 %d 行不一致，不一致率: %.4f%%", mismatchCount, rate)
			log.Printf("  不一致主键列表：")
			for _, pk := range mismatchList {
				log.Printf("    - %s", pk)
			}
		}
	}
	log.Println("===================================")
}

func fetchPrimaryKeysBatch(ctx context.Context, pool *pgxpool.Pool, ti TableInfo, lastPK interface{}, limit int) ([]pkTuple, interface{}, error) {
	pkColumn := ti.PrimaryKeys[0]
	quotedPK := quoteIdentifier(pkColumn)
	quotedTable := quoteIdentifier(ti.Name)

	var sql string
	var args []interface{}

	if lastPK == nil {
		sql = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT $1", quotedPK, quotedTable, quotedPK)
		args = []interface{}{limit}
	} else {
		sql = fmt.Sprintf("SELECT %s FROM %s WHERE %s > $1 ORDER BY %s LIMIT $2", quotedPK, quotedTable, quotedPK, quotedPK)
		args = []interface{}{lastPK, limit}
	}

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var tuples []pkTuple
	var lastValue interface{}
	for rows.Next() {
		var val interface{}
		if err := rows.Scan(&val); err != nil {
			return nil, nil, err
		}
		tuples = append(tuples, pkTuple{
			Value:  val,
			String: fmt.Sprintf("%v", val),
		})
		lastValue = val
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return tuples, lastValue, nil
}

func batchCheck(ctx context.Context, srcPool, tgtPool *pgxpool.Pool, ti TableInfo, pks []pkTuple) (bool, error) {
	if len(pks) == 0 {
		return true, nil
	}
	pkColumn := ti.PrimaryKeys[0]
	quotedPK := quoteIdentifier(pkColumn)
	quotedTable := quoteIdentifier(ti.Name)

	values := make([]interface{}, len(pks))
	for i, pk := range pks {
		values[i] = pk.Value
	}

	sql := fmt.Sprintf(`
		SELECT md5(string_agg(row_to_json(t)::text, '' ORDER BY %s))
		FROM %s t
		WHERE %s = ANY($1)
	`, quotedPK, quotedTable, quotedPK)

	var srcHash, tgtHash string
	err := srcPool.QueryRow(ctx, sql, values).Scan(&srcHash)
	if err != nil && err != pgx.ErrNoRows {
		return false, err
	}
	err = tgtPool.QueryRow(ctx, sql, values).Scan(&tgtHash)
	if err != nil && err != pgx.ErrNoRows {
		return false, err
	}
	return srcHash == tgtHash, nil
}

func findDiffPKs(ctx context.Context, srcPool, tgtPool *pgxpool.Pool, ti TableInfo, pks []pkTuple) ([]pkTuple, error) {
	var diff []pkTuple
	for _, pk := range pks {
		pkMap := map[string]interface{}{
			ti.PrimaryKeys[0]: pk.Value,
		}
		srcHash, err := getRowHash(ctx, srcPool, ti, pkMap)
		if err != nil {
			return nil, fmt.Errorf("源端哈希失败 pk=%v: %w", pk.Value, err)
		}
		tgtHash, err := getRowHash(ctx, tgtPool, ti, pkMap)
		if err != nil {
			return nil, fmt.Errorf("目标端哈希失败 pk=%v: %w", pk.Value, err)
		}
		if srcHash != tgtHash {
			diff = append(diff, pk)
		}
	}
	return diff, nil
}

func getRowHash(ctx context.Context, pool *pgxpool.Pool, ti TableInfo, pkValues map[string]interface{}) (string, error) {
	conditions := make([]string, 0, len(ti.PrimaryKeys))
	args := make([]interface{}, 0, len(ti.PrimaryKeys))
	argIdx := 1
	for _, pk := range ti.PrimaryKeys {
		val, ok := pkValues[pk]
		if !ok {
			return "", fmt.Errorf("缺少主键列 %s 的值", pk)
		}
		conditions = append(conditions, fmt.Sprintf("%s = $%d", quoteIdentifier(pk), argIdx))
		args = append(args, val)
		argIdx++
	}
	whereClause := strings.Join(conditions, " AND ")

	sql := fmt.Sprintf(`
		SELECT md5(row_to_json(t)::text)
		FROM (SELECT * FROM %s) t
		WHERE %s
	`, quoteIdentifier(ti.Name), whereClause)

	var hash string
	err := pool.QueryRow(ctx, sql, args...).Scan(&hash)
	if err != nil {
		return "", fmt.Errorf("查询行哈希失败: %w", err)
	}
	return hash, nil
}

func getEstimatedRowCount(ctx context.Context, pool *pgxpool.Pool, tableName string) (int64, error) {
	schema, table := parseSchemaTable(tableName)
	var reltuples float32
	err := pool.QueryRow(ctx, `
		SELECT c.reltuples
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	`, schema, table).Scan(&reltuples)
	if err != nil {
		return 0, fmt.Errorf("查询 reltuples 失败: %w", err)
	}
	return int64(reltuples), nil
}

func getPrimaryKeys(ctx context.Context, pool *pgxpool.Pool, tableName string) ([]string, error) {
	schema, table := parseSchemaTable(tableName)
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
	rows, err := pool.Query(ctx, query, schema, table)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(pks) == 0 {
		return nil, fmt.Errorf("表 %s 没有主键", tableName)
	}
	return pks, nil
}

func quoteIdentifier(ident string) string {
	return fmt.Sprintf(`"%s"`, ident)
}

func parseSchemaTable(fullName string) (schema, table string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}

type sliceFlag []string

func (s *sliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *sliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}
