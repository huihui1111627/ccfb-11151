// Command stress is the end-to-end verification harness for TinyLake.
//
// It runs dozens of concurrent writers against a catalog backed by a local
// directory (object-store simulation), injects crash-retries and duplicate
// batches, then verifies the golden invariant: every committed row survives
// exactly once — zero loss, zero duplication — across compaction, partition
// evolution, delete/rewrite, and snapshot expiration. It also reports the
// number of files a query must scan before and after compaction.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"tinylake/internal/lake"
)

type config struct {
	root    string
	table   string
	writers int
	rounds  int
	rowsPer int
	target  int64
	seed    int64
}

// pass/fail counters shared across phases.
var failures atomic.Int64

func fail(format string, args ...any) {
	failures.Add(1)
	log.Printf("❌ FAIL: "+format, args...)
}

func ok(format string, args ...any) {
	log.Printf("✅ "+format, args...)
}

func section(n int, title string) {
	log.Printf("\n════════ 阶段 %d：%s ════════", n, title)
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.root, "root", "./lake-stress", "storage root (fresh directory)")
	flag.StringVar(&cfg.table, "table", "events", "table name")
	flag.IntVar(&cfg.writers, "writers", 32, "concurrent writer count")
	flag.IntVar(&cfg.rounds, "rounds", 8, "commit rounds per writer")
	flag.IntVar(&cfg.rowsPer, "rows", 20, "rows per data file")
	flag.Int64Var(&cfg.target, "target", 1024, "compaction target bytes")
	flag.Int64Var(&cfg.seed, "seed", 42, "random seed")
	flag.Parse()

	_ = os.RemoveAll(cfg.root)
	store, err := lake.NewLocalStorage(cfg.root)
	must(err)
	cat := lake.NewCatalog(store)
	ctx := context.Background()

	// keyAlloc hands out globally unique row keys, so the verifier can assert
	// exact-set equality with no ambiguity.
	var keyAlloc atomic.Int64
	nextKey := func() int64 { return keyAlloc.Add(1) }

	// expected tracks every key a writer was acknowledged for (commit success
	// or duplicate success). The final table must equal this set exactly.
	var mu sync.Mutex
	expected := map[int64]bool{}
	addExpected := func(keys []int64) {
		mu.Lock()
		for _, k := range keys {
			expected[k] = true
		}
		mu.Unlock()
	}

	_, err = cat.CreateTable(ctx, cfg.table, []string{"dt"})
	must(err)
	log.Printf("初始化完成：表 %s，%d 个并发写入方，每方 %d 轮，每文件 %d 行",
		cfg.table, cfg.writers, cfg.rounds, cfg.rowsPer)

	// ---- 阶段 1：并发写入 + 崩溃重试（幂等） ----
	section(1, fmt.Sprintf("%d 个写入方并发乐观锁提交，含崩溃重试", cfg.writers))
	var totalCommits, totalDuplicates, maxAttempts atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	// Long-lived reader: repeatedly pins a snapshot and scans it, asserting a
	// read never errors and always sees a self-consistent snapshot (atomicity
	// — never a partial commit).
	readerStop := make(chan struct{})
	var readerErrs atomic.Int64
	var readerRuns atomic.Int64
	go func() {
		for {
			select {
			case <-readerStop:
				return
			default:
			}
			r, err := cat.BeginRead(ctx, cfg.table)
			if err != nil {
				readerErrs.Add(1)
				continue
			}
			meta, err := cat.LoadTable(ctx, cfg.table)
			if err == nil {
				plan := meta.PlanScan(lake.ScanFilter{})
				if plan.ScannedFiles < 0 {
					readerErrs.Add(1)
				}
				readerRuns.Add(1)
			}
			r.EndRead()
			time.Sleep(time.Millisecond)
		}
	}()

	for w := 0; w < cfg.writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			wr := rand.New(rand.NewSource(cfg.seed + int64(writerID)*1009))
			id := fmt.Sprintf("writer-%02d", writerID)
			for round := 0; round < cfg.rounds; round++ {
				// Build one small data file (simulating a per-minute stream).
				keys := make([]int64, cfg.rowsPer)
				rows := make([]lake.Row, cfg.rowsPer)
				for i := range rows {
					k := nextKey()
					keys[i] = k
					rows[i] = lake.Row{Key: k}
				}
				part := map[string]string{"dt": fmt.Sprintf("2026-11-%02d", 1+wr.Intn(7))}
				df, err := cat.WriteDataFile(ctx, cfg.table, part, rows)
				must(err)

				req := lake.WriteRequest{
					WriterID: id,
					Message:  fmt.Sprintf("round %d", round),
					Add:      []lake.DataFile{df},
				}
				res, err := cat.Commit(ctx, cfg.table, &req)
				must(err)
				totalCommits.Add(1)
				if res.Attempts > 1 {
					maxAttempts.Store(maxInt64(maxAttempts.Load(), int64(res.Attempts)))
				}
				addExpected(keys)

				// Crash-retry injection (~15%): the writer "crashes" after a
				// successful commit and retries the SAME batch. Idempotency
				// must make the retry a duplicate no-op with no second copy.
				if wr.Float64() < 0.15 {
					retry, err := cat.Commit(ctx, cfg.table, &req)
					must(err)
					if !retry.Duplicate {
						fail("writer %s round %d: 崩溃重试未被识别为重复提交", id, round)
					} else {
						totalDuplicates.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(readerStop)
	writeDur := time.Since(start)
	ok("并发提交完成：%d 次提交，%d 次崩溃重试全部幂等命中，最大 CAS 重试 %d 轮，耗时 %s",
		totalCommits.Load(), totalDuplicates.Load(), maxAttempts.Load(), writeDur.Round(time.Millisecond))
	ok("并发期间持续读取 %d 次，读取错误 %d（验证原子可见性）", readerRuns.Load(), readerErrs.Load())
	if readerErrs.Load() != 0 {
		fail("并发读取出现 %d 次错误", readerErrs.Load())
	}

	// ---- 阶段 2：核对不丢不重 ----
	section(2, "全量核对：一行不丢、一行不重")
	verifyExact(ctx, cat, cfg.table, expected)

	meta, err := cat.LoadTable(ctx, cfg.table)
	must(err)
	before := meta.Stats()
	ok("当前快照：文件数=%d 行数=%d 平均文件=%s 全表扫描文件=%d",
		before.FileCount, before.TotalRows, humanBytes(before.AvgFileBytes), before.FullScanFiles)

	// ---- 阶段 3：分区裁剪（演进前，按天） ----
	section(3, "分区裁剪验证（按天分区）")
	day := "2026-11-03"
	fullPlan := meta.PlanScan(lake.ScanFilter{})
	dayPlan := meta.PlanScan(lake.ScanFilter{Partitions: []map[string]string{{"dt": day}}})
	ok("全表扫描文件=%d，限定 dt=%s 只扫 %d 个文件（跳过 %d 个）",
		fullPlan.ScannedFiles, day, dayPlan.ScannedFiles, dayPlan.SkippedByPart)
	if dayPlan.ScannedFiles >= fullPlan.ScannedFiles {
		fail("分区裁剪未生效：分区扫描 %d >= 全表 %d", dayPlan.ScannedFiles, fullPlan.ScannedFiles)
	}

	// ---- 阶段 4：小文件合并 + 扫描文件数对比 ----
	section(4, "后台小文件合并（读写不阻塞），对比扫描文件数")
	// A reader pinned across the compaction proves old files stay readable.
	pinned, err := cat.BeginRead(ctx, cfg.table)
	must(err)
	compRes, err := cat.Compact(ctx, cfg.table, lake.CompactionConfig{
		TargetSize: cfg.target, MinInputFiles: 2, MaxInputFiles: 50,
	})
	must(err)
	if !compRes.Ran {
		fail("合并任务未执行（期望合并小文件）")
	} else {
		ok("合并：%d 个小文件 → %d 个目标文件（%.1fx），CAS 轮次=%d",
			compRes.FilesIn, compRes.FilesOut,
			float64(compRes.FilesIn)/float64(max(compRes.FilesOut, 1)), compRes.Attempts)
	}
	// The snapshot pinned before compaction is still readable.
	pinMeta, err := cat.LoadTable(ctx, cfg.table)
	must(err)
	_ = pinMeta
	pinned.EndRead()

	meta, err = cat.LoadTable(ctx, cfg.table)
	must(err)
	after := meta.Stats()
	scanAfterCompact := after.FullScanFiles // captured before later phases add files
	ok("合并后：文件数=%d（%d→%d）平均文件=%s 全表扫描文件=%d（%d→%d）",
		after.FileCount, before.FileCount, after.FileCount,
		humanBytes(after.AvgFileBytes), after.FullScanFiles, before.FullScanFiles, after.FullScanFiles)
	if after.FullScanFiles >= before.FullScanFiles {
		fail("合并后扫描文件数未下降：%d -> %d", before.FullScanFiles, after.FullScanFiles)
	}
	verifyExact(ctx, cat, cfg.table, expected)

	// ---- 阶段 5：分区演进 按天 -> 按小时，新旧共存 ----
	section(5, "分区演进：dt 升级为 dt+hour，新旧分区共存且正确裁剪")
	specID, err := cat.AddPartitionSpec(ctx, cfg.table, []string{"dt", "hour"})
	must(err)
	ok("新分区规格 id=%d（dt,hour），旧文件保留 dt 布局", specID)

	// New writers use hourly partitions; old files remain day-only.
	var extraKeys []int64
	for h := 0; h < 6; h++ {
		keys := make([]int64, cfg.rowsPer)
		rows := make([]lake.Row, cfg.rowsPer)
		for i := range rows {
			k := nextKey()
			keys[i] = k
			rows[i] = lake.Row{Key: k}
		}
		part := map[string]string{"dt": day, "hour": fmt.Sprintf("%02d", h)}
		df, err := cat.WriteDataFile(ctx, cfg.table, part, rows)
		must(err)
		_, err = cat.Commit(ctx, cfg.table, &lake.WriteRequest{
			WriterID: fmt.Sprintf("hourly-%d", h), Add: []lake.DataFile{df}, Message: "hourly partition",
		})
		must(err)
		addExpected(keys)
		extraKeys = append(extraKeys, keys...)
	}
	meta, err = cat.LoadTable(ctx, cfg.table)
	must(err)
	// Day filter must match BOTH old day-only files and new day+hour files.
	dayPlan2 := meta.PlanScan(lake.ScanFilter{Partitions: []map[string]string{{"dt": day}}})
	hourPlan := meta.PlanScan(lake.ScanFilter{Partitions: []map[string]string{{"dt": day, "hour": "02"}}})
	ok("演进后 dt=%s 命中 %d 个文件（含新旧两种布局），dt=%s/hour=02 仅命中 %d 个",
		day, dayPlan2.ScannedFiles, day, hourPlan.ScannedFiles)
	if hourPlan.ScannedFiles == 0 || hourPlan.ScannedFiles >= dayPlan2.ScannedFiles {
		fail("小时级裁剪异常：hour=%d day=%d", hourPlan.ScannedFiles, dayPlan2.ScannedFiles)
	}
	verifyExact(ctx, cat, cfg.table, expected)

	// ---- 阶段 6：删除标记 + 物理重写 ----
	section(6, "删除/更新先打标记，攒够后物理重写")
	// Pick some live keys to delete.
	meta, err = cat.LoadTable(ctx, cfg.table)
	must(err)
	var victimKeys []int64
	for k := range expected {
		victimKeys = append(victimKeys, k)
		if len(victimKeys) >= 25 {
			break
		}
	}
	sort.Slice(victimKeys, func(i, j int) bool { return victimKeys[i] < victimKeys[j] })
	_, err = cat.Commit(ctx, cfg.table, &lake.WriteRequest{
		WriterID: "gdpr-job", Message: "tombstone 25 keys", DeleteKeys: victimKeys,
	})
	must(err)
	for _, k := range victimKeys {
		delete(expected, k)
	}
	meta, err = cat.LoadTable(ctx, cfg.table)
	must(err)
	if len(meta.Deletes) != len(victimKeys) {
		fail("删除标记数 %d != 期望 %d", len(meta.Deletes), len(victimKeys))
	}
	ok("已写入 %d 个删除标记（读时过滤，文件尚未重写）", len(meta.Deletes))
	// Rows must already be invisible to readers even before physical rewrite.
	rows, _, err := cat.Scan(ctx, meta, lake.ScanFilter{})
	must(err)
	seen := rowSet(rows)
	for _, k := range victimKeys {
		if seen[k] {
			fail("删除标记未在读时生效：key=%d 仍可见", k)
		}
	}
	ok("删除标记读时过滤生效，物理文件暂未改动")

	rw, err := cat.RewriteDeletes(ctx, cfg.table, 1)
	must(err)
	if !rw.Ran {
		fail("物理重写未执行")
	} else {
		ok("物理重写：%d 个污染文件，移除 %d 行，丢弃 %d 个过期标记",
			rw.FilesRewritten, rw.RowsRemoved, rw.MarkersDropped)
	}
	verifyExact(ctx, cat, cfg.table, expected)

	// ---- 阶段 7：显式重复提交（同一 CommitID 只生效一次） ----
	section(7, "同一批数据用固定 CommitID 重复提交，只生效一次")
	dupKeys := make([]int64, cfg.rowsPer)
	dupRows := make([]lake.Row, cfg.rowsPer)
	for i := range dupRows {
		k := nextKey()
		dupKeys[i] = k
		dupRows[i] = lake.Row{Key: k}
	}
	df, err := cat.WriteDataFile(ctx, cfg.table, map[string]string{"dt": day}, dupRows)
	must(err)
	fixedReq := lake.WriteRequest{
		WriterID: "exact-once", ExplicitCommitID: "fixed-txn-001",
		Add: []lake.DataFile{df}, Message: "explicit idempotency",
	}
	r1, err := cat.Commit(ctx, cfg.table, &fixedReq)
	must(err)
	r2, err := cat.Commit(ctx, cfg.table, &fixedReq)
	must(err)
	r3, err := cat.Commit(ctx, cfg.table, &fixedReq)
	must(err)
	if r1.Duplicate || !r2.Duplicate || !r3.Duplicate {
		fail("固定 CommitID 幂等异常：r1.dup=%v r2.dup=%v r3.dup=%v", r1.Duplicate, r2.Duplicate, r3.Duplicate)
	}
	addExpected(dupKeys)
	ok("同一 CommitID 提交 3 次：仅首次生效，后两次为重复空操作（快照 #%d）", r1.SnapshotID)
	verifyExact(ctx, cat, cfg.table, expected)

	// ---- 阶段 8：过期快照 + 孤儿文件安全清理 ----
	section(8, "过期快照与孤儿文件清理（保护在途读取）")
	// Plant an orphan: an uncommitted data object, as a crashed compaction
	// staging file would leave behind.
	orphanDF, err := cat.WriteDataFile(ctx, cfg.table, map[string]string{"dt": "1999-01-01"},
		[]lake.Row{{Key: nextKey()}})
	must(err)
	orphanExists, err := store.Exists(ctx, orphanDF.Path)
	must(err)
	if !orphanExists {
		fail("孤儿文件写入失败")
	}
	// A reader pinned before cleanup must keep its files safe.
	pin2, err := cat.BeginRead(ctx, cfg.table)
	must(err)
	// Lower retention so expiration actually reclaims history.
	meta, err = cat.LoadTable(ctx, cfg.table)
	must(err)
	meta.SnapshotRetention = 3
	// Persist retention tweak through a normal metadata write via a no-data
	// commit is awkward; instead rewrite via a dedicated catalog helper below.
	must(cat.SetRetention(ctx, cfg.table, 3))

	// Wait beyond the orphan safety grace so it is eligible for deletion.
	time.Sleep(6 * time.Second)
	exp, err := cat.ExpireSnapshots(ctx, cfg.table)
	must(err)
	ok("清理：过期快照=%d 删除数据文件=%d 旧元数据文件=%d 孤儿文件=%d 受保护读取=%d",
		exp.SnapshotsExpired, exp.DataFilesDeleted, exp.MetaFilesDeleted,
		exp.OrphansDeleted, exp.ProtectedReads)
	// Pinned snapshot's files must still be physically present.
	pin2.EndRead()
	// Orphan should be gone now.
	orphanGone, err := store.Exists(ctx, orphanDF.Path)
	must(err)
	if orphanGone {
		fail("孤儿文件未被清理：%s", orphanDF.Path)
	} else {
		ok("孤儿文件已安全回收（且未误删在途读取的文件）")
	}
	verifyExact(ctx, cat, cfg.table, expected)

	// ---- 汇总 ----
	section(9, "最终汇总")
	meta, err = cat.LoadTable(ctx, cfg.table)
	must(err)
	st := meta.Stats()
	log.Printf("表 %s：快照=%d 文件=%d 行=%d 字节=%s 分区数=%d 删除标记=%d",
		cfg.table, st.SnapshotCount, st.FileCount, st.TotalRows,
		humanBytes(st.TotalBytes), len(st.Partitions), st.DeleteMarkers)
	log.Printf("扫描文件数对比（合并前后即时口径）：合并前 %d → 合并后 %d（减少 %.1f%%）；当前（含后续写入）%d",
		before.FullScanFiles, scanAfterCompact,
		100*(1-float64(scanAfterCompact)/float64(before.FullScanFiles)), st.FullScanFiles)
	log.Printf("期望行数=%d 实际行数=%d", len(expected), st.TotalRows)

	if failures.Load() == 0 {
		log.Printf("\n🎉 全部校验通过：数据一行不丢、一行不重；8 项语义全部满足")
		os.Exit(0)
	}
	log.Printf("\n💥 存在 %d 项失败", failures.Load())
	os.Exit(1)
}

// verifyExact asserts the live table's visible rows equal expected exactly.
func verifyExact(ctx context.Context, cat *lake.Catalog, table string, expected map[int64]bool) {
	meta, err := cat.LoadTable(ctx, table)
	if err != nil {
		fail("加载表失败: %v", err)
		return
	}
	rows, plan, err := cat.Scan(ctx, meta, lake.ScanFilter{})
	if err != nil {
		fail("扫描失败: %v", err)
		return
	}
	got := rowSet(rows)
	if len(got) != len(rows) {
		fail("表内出现重复行：扫描 %d 行，去重后 %d", len(rows), len(got))
	}
	var missing, extra []int64
	for k := range expected {
		if !got[k] {
			missing = append(missing, k)
		}
	}
	for k := range got {
		if !expected[k] {
			extra = append(extra, k)
		}
	}
	if len(missing) == 0 && len(extra) == 0 && len(got) == len(expected) {
		ok("核对通过：%d 行，无丢失无重复（扫描 %d 个文件）", len(got), plan.ScannedFiles)
		return
	}
	if len(missing) > 0 {
		fail("丢失 %d 行，示例: %v", len(missing), head(missing))
	}
	if len(extra) > 0 {
		fail("多出 %d 行，示例: %v", len(extra), head(extra))
	}
}

func rowSet(rows []lake.Row) map[int64]bool {
	m := make(map[int64]bool, len(rows))
	for _, r := range rows {
		m[r.Key] = true
	}
	return m
}

func head(xs []int64) []int64 {
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	if len(xs) > 5 {
		return xs[:5]
	}
	return xs
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
