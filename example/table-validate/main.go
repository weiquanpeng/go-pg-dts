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

	"github.com/jackc/pgx/v5/pgxpool"
)

type TableInfo struct {
	Name        string
	PrimaryKeys []string
}

type pkTuple struct {
	Values map[string]interface{}
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

func (sc *StatsCollector) GetAndReset() (checked map[string]int64, mismatches map[string][]string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	checked = sc.checked
	mismatches = sc.mismatches
	sc.checked = make(map[string]int64)
	sc.mismatches = make(map[string][]string)
	return
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
	flag.Var(&tables, "tables", "要校验的表名（可多次指定），如 --tables=table1 --tables=table2")
	flag.StringVar(&tablesSQL, "tables-sql", "", "执行自定义 SQL 获取表名列表")
	flag.IntVar(&batchSize, "batch", 1000, "每次抽样的主键数量")
	flag.DurationVar(&interval, "interval", 30*time.Second, "统计报告输出间隔")
	flag.IntVar(&workers, "workers", 4, "并发校验的协程数（控制同时校验几张表）")
	flag.IntVar(&ratePerSec, "rate", 1000, "每张表每秒最多校验的行数（限流）")
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
		tableInfos = append(tableInfos, TableInfo{
			Name:        tbl,
			PrimaryKeys: pks,
		})
		log.Printf("表 %s 主键: %v", tbl, pks)
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

			log.Printf("开始持续校验表 %s（目标速率 %d 行/秒）", ti.Name, ratePerSec)

			innerSem := make(chan struct{}, 20)
			throttle := time.NewTicker(time.Second / time.Duration(ratePerSec))
			defer throttle.Stop()

			for {
				select {
				case <-ctx.Done():
					log.Printf("表 %s 校验协程退出", ti.Name)
					return
				default:
				}

				pkTuples, err := samplePrimaryKeys(ctx, srcPool, ti, batchSize)
				if err != nil {
					log.Printf("表 %s 抽样主键失败: %v", ti.Name, err)
					time.Sleep(5 * time.Second)
					continue
				}
				if len(pkTuples) == 0 {
					time.Sleep(5 * time.Second)
					continue
				}

				var wgInner sync.WaitGroup
				for _, pk := range pkTuples {
					wgInner.Add(1)
					go func(pk pkTuple) {
						defer wgInner.Done()
						innerSem <- struct{}{}
						defer func() { <-innerSem }()

						select {
						case <-throttle.C:
						case <-ctx.Done():
							return
						}

						srcHash, err := getRowHash(ctx, srcPool, ti, pk.Values)
						if err != nil {
							log.Printf("获取源哈希失败 pk=%v: %v", pk.Values, err)
							return
						}
						tgtHash, err := getRowHash(ctx, tgtPool, ti, pk.Values)
						if err != nil {
							log.Printf("获取目标哈希失败 pk=%v: %v", pk.Values, err)
							return
						}
						if srcHash != tgtHash {
							stats.AddMismatch(ti.Name, pk.String)
						}
						stats.AddChecked(ti.Name, 1)
					}(pk)
				}
				wgInner.Wait()
			}
		}(ti)
	}

	reportTicker := time.NewTicker(interval)
	defer reportTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("程序退出")
			return
		case <-reportTicker.C:
			checked, mismatches := stats.GetAndReset()
			printReport(checked, mismatches)
		}
	}
}

func printReport(checked map[string]int64, mismatches map[string][]string) {
	log.Println("========== 校验统计报告 ==========")
	for table, cnt := range checked {
		mismatchList := mismatches[table]
		if len(mismatchList) == 0 {
			log.Printf("表 %s: 校验 %d 行，全部一致", table, cnt)
		} else {
			log.Printf("表 %s: 校验 %d 行，不一致行数 %d，主键列表：", table, cnt, len(mismatchList))
			for _, pk := range mismatchList {
				log.Printf("  - %s", pk)
			}
		}
	}
	log.Println("===================================")
}

func samplePrimaryKeys(ctx context.Context, pool *pgxpool.Pool, ti TableInfo, limit int) ([]pkTuple, error) {
	if limit <= 0 {
		return nil, nil
	}
	pkCols := quoteIdentifiers(ti.PrimaryKeys)
	quotedTable := quoteIdentifier(ti.Name)

	sql := fmt.Sprintf(`
		SELECT %s FROM %s TABLESAMPLE SYSTEM(1)
		LIMIT $1
	`, pkCols, quotedTable)

	rows, err := pool.Query(ctx, sql, limit)
	if err != nil {
		return nil, fmt.Errorf("抽样查询失败: %w", err)
	}
	defer rows.Close()

	var tuples []pkTuple
	for rows.Next() {
		values := make([]interface{}, len(ti.PrimaryKeys))
		valuePtrs := make([]interface{}, len(ti.PrimaryKeys))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		pkMap := make(map[string]interface{})
		pkStrs := make([]string, len(ti.PrimaryKeys))
		for i, col := range ti.PrimaryKeys {
			pkMap[col] = values[i]
			pkStrs[i] = fmt.Sprintf("%v", values[i])
		}
		tuples = append(tuples, pkTuple{
			Values: pkMap,
			String: strings.Join(pkStrs, ","),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(tuples) == 0 && limit > 0 {
		sqlRand := fmt.Sprintf(`
			SELECT %s FROM %s ORDER BY random() LIMIT $1
		`, pkCols, quotedTable)
		rows2, err := pool.Query(ctx, sqlRand, limit)
		if err != nil {
			return nil, err
		}
		defer rows2.Close()
		for rows2.Next() {
			values := make([]interface{}, len(ti.PrimaryKeys))
			valuePtrs := make([]interface{}, len(ti.PrimaryKeys))
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			if err := rows2.Scan(valuePtrs...); err != nil {
				return nil, err
			}
			pkMap := make(map[string]interface{})
			pkStrs := make([]string, len(ti.PrimaryKeys))
			for i, col := range ti.PrimaryKeys {
				pkMap[col] = values[i]
				pkStrs[i] = fmt.Sprintf("%v", values[i])
			}
			tuples = append(tuples, pkTuple{
				Values: pkMap,
				String: strings.Join(pkStrs, ","),
			})
		}
	}
	return tuples, nil
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

func quoteIdentifiers(idents []string) string {
	quoted := make([]string, len(idents))
	for i, id := range idents {
		quoted[i] = quoteIdentifier(id)
	}
	return strings.Join(quoted, ", ")
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
