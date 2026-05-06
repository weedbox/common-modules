package sqlite_connector

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// slowSQLThreshold matches the gorm logger SlowThreshold configured in the
// connector — any single query at or above this duration would be flagged as
// "SLOW SQL" in production logs.
const slowSQLThreshold = 200 * time.Millisecond

type latencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (r *latencyRecorder) Add(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d)
	r.mu.Unlock()
}

type latencySummary struct {
	n                  int
	p50, p95, p99, max time.Duration
	slowCount          int
}

func (s latencySummary) String() string {
	return fmt.Sprintf("n=%d p50=%v p95=%v p99=%v max=%v slow(>=%v)=%d",
		s.n, s.p50, s.p95, s.p99, s.max, slowSQLThreshold, s.slowCount)
}

func (r *latencyRecorder) summary(slowThreshold time.Duration) latencySummary {
	r.mu.Lock()
	samples := append([]time.Duration(nil), r.samples...)
	r.mu.Unlock()
	if len(samples) == 0 {
		return latencySummary{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	pick := func(pct int) time.Duration {
		idx := len(samples) * pct / 100
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		return samples[idx]
	}
	s := latencySummary{
		n:   len(samples),
		max: samples[len(samples)-1],
		p50: pick(50),
		p95: pick(95),
		p99: pick(99),
	}
	for _, d := range samples {
		if d >= slowThreshold {
			s.slowCount++
		}
	}
	return s
}

type testRecord struct {
	ID        uint `gorm:"primaryKey"`
	Bucket    int  `gorm:"index"`
	Payload   string
	CreatedAt time.Time
}

type testConfig struct {
	enableSplit       bool
	maxOpenConns      int
	maxIdleConns      int
	connMaxIdleTime   int
	writeMaxOpenConns int
	writeMaxIdleConns int
	busyTimeout       int
}

var testScopeCounter atomic.Int64

// newTestConnector spins up a SQLiteConnector backed by a fresh temp DB and
// returns the connector + GORM handle. The connector is closed automatically
// when the test ends.
func newTestConnector(t *testing.T, cfg testConfig) (*SQLiteConnector, *gorm.DB) {
	t.Helper()

	scope := fmt.Sprintf("sqlite_test_%d", testScopeCounter.Add(1))
	dbPath := filepath.Join(t.TempDir(), "test.db")

	c := &SQLiteConnector{
		params: Params{Logger: zap.NewNop()},
		logger: zap.NewNop(),
		scope:  scope,
	}
	c.initDefaultConfigs()

	viper.Set(scope+".path", dbPath)
	viper.Set(scope+".enable_read_write_split", cfg.enableSplit)
	if cfg.maxOpenConns > 0 {
		viper.Set(scope+".max_open_conns", cfg.maxOpenConns)
	}
	if cfg.maxIdleConns > 0 {
		viper.Set(scope+".max_idle_conns", cfg.maxIdleConns)
	}
	if cfg.connMaxIdleTime > 0 {
		viper.Set(scope+".conn_max_idle_time", cfg.connMaxIdleTime)
	}
	if cfg.writeMaxOpenConns > 0 {
		viper.Set(scope+".write_max_open_conns", cfg.writeMaxOpenConns)
	}
	if cfg.writeMaxIdleConns > 0 {
		viper.Set(scope+".write_max_idle_conns", cfg.writeMaxIdleConns)
	}
	if cfg.busyTimeout > 0 {
		viper.Set(scope+".busy_timeout", cfg.busyTimeout)
	}

	if err := c.onStart(context.Background()); err != nil {
		t.Fatalf("onStart: %v", err)
	}
	t.Cleanup(func() {
		if err := c.onStop(context.Background()); err != nil {
			t.Logf("onStop: %v", err)
		}
	})

	if err := c.GetDB().AutoMigrate(&testRecord{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return c, c.GetDB()
}

// runForBothModes runs `body` once with split disabled and once with split
// enabled, so every scenario covers both code paths.
func runForBothModes(t *testing.T, body func(t *testing.T, split bool)) {
	t.Helper()
	for _, split := range []bool{false, true} {
		split := split
		t.Run(fmt.Sprintf("split=%v", split), func(t *testing.T) {
			body(t, split)
		})
	}
}

// TestConnector_StartStopRoundTrip is the smoke test: starting the connector,
// writing one row, reading it back, and stopping cleanly should always work
// in both modes.
func TestConnector_StartStopRoundTrip(t *testing.T) {
	runForBothModes(t, func(t *testing.T, split bool) {
		_, db := newTestConnector(t, testConfig{enableSplit: split, busyTimeout: 5000})

		if err := db.Create(&testRecord{Payload: "hello"}).Error; err != nil {
			t.Fatalf("write: %v", err)
		}

		var got testRecord
		if err := db.First(&got).Error; err != nil {
			t.Fatalf("read: %v", err)
		}
		if got.Payload != "hello" {
			t.Fatalf("payload mismatch: got %q", got.Payload)
		}
	})
}

// TestConnector_ConcurrentInserts hammers the writer pool from many
// goroutines and verifies the final row count matches what was issued. With
// busy_timeout high enough no SQLITE_BUSY should leak through.
func TestConnector_ConcurrentInserts(t *testing.T) {
	runForBothModes(t, func(t *testing.T, split bool) {
		_, db := newTestConnector(t, testConfig{
			enableSplit: split,
			busyTimeout: 10000,
		})

		const writers = 8
		const perWriter = 200

		var wg sync.WaitGroup
		var failures atomic.Int64
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(bucket int) {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					r := &testRecord{
						Bucket:  bucket,
						Payload: fmt.Sprintf("w%d-i%d", bucket, i),
					}
					if err := db.Create(r).Error; err != nil {
						failures.Add(1)
						t.Errorf("create w=%d i=%d: %v", bucket, i, err)
						return
					}
				}
			}(w)
		}
		wg.Wait()

		if failures.Load() > 0 {
			return
		}

		var count int64
		if err := db.Model(&testRecord{}).Count(&count).Error; err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != int64(writers*perWriter) {
			t.Fatalf("count mismatch: got %d want %d", count, writers*perWriter)
		}
	})
}

// TestConnector_ConcurrentReads seeds a batch of rows and then reads from
// many goroutines simultaneously. Validates that read-only contention does
// not produce errors.
func TestConnector_ConcurrentReads(t *testing.T) {
	runForBothModes(t, func(t *testing.T, split bool) {
		_, db := newTestConnector(t, testConfig{
			enableSplit:  split,
			busyTimeout:  10000,
			maxOpenConns: 16,
		})

		const seedRows = 500
		for i := 0; i < seedRows; i++ {
			if err := db.Create(&testRecord{
				Bucket:  i % 10,
				Payload: fmt.Sprintf("seed-%d", i),
			}).Error; err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
		}

		const readers = 16
		const perReader = 100

		var wg sync.WaitGroup
		var failures atomic.Int64
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func(rid int) {
				defer wg.Done()
				for i := 0; i < perReader; i++ {
					var rs []testRecord
					if err := db.Where("bucket = ?", i%10).Limit(20).Find(&rs).Error; err != nil {
						failures.Add(1)
						t.Errorf("find r=%d i=%d: %v", rid, i, err)
						return
					}
				}
			}(r)
		}
		wg.Wait()

		if failures.Load() > 0 {
			t.Fatalf("%d failed reads", failures.Load())
		}
	})
}

// TestConnector_MixedReadWrite runs writers and readers concurrently for a
// fixed wall-clock window, then checks the final row count. Reads must keep
// succeeding while writes are in-flight, and the total persisted matches the
// number of issued INSERTs.
func TestConnector_MixedReadWrite(t *testing.T) {
	runForBothModes(t, func(t *testing.T, split bool) {
		_, db := newTestConnector(t, testConfig{
			enableSplit:     split,
			busyTimeout:     10000,
			maxOpenConns:    16,
			connMaxIdleTime: 5,
		})

		const writers = 4
		const readers = 8
		const perWriter = 250

		var wg sync.WaitGroup
		var writeErrs, readErrs, readOps atomic.Int64

		ctx, cancel := context.WithCancel(context.Background())

		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil {
					var rs []testRecord
					if err := db.Limit(50).Find(&rs).Error; err != nil {
						readErrs.Add(1)
						return
					}
					readOps.Add(1)
				}
			}()
		}

		var writeWg sync.WaitGroup
		for w := 0; w < writers; w++ {
			writeWg.Add(1)
			go func(bucket int) {
				defer writeWg.Done()
				for i := 0; i < perWriter; i++ {
					if err := db.Create(&testRecord{
						Bucket:  bucket,
						Payload: fmt.Sprintf("mix-%d-%d", bucket, i),
					}).Error; err != nil {
						writeErrs.Add(1)
						return
					}
				}
			}(w)
		}
		writeWg.Wait()

		// Let readers run a brief tail so they observe the post-write state.
		time.Sleep(200 * time.Millisecond)
		cancel()
		wg.Wait()

		if writeErrs.Load() > 0 {
			t.Fatalf("write errors: %d", writeErrs.Load())
		}
		if readErrs.Load() > 0 {
			t.Fatalf("read errors: %d", readErrs.Load())
		}
		if readOps.Load() == 0 {
			t.Fatalf("readers issued zero ops; mixed scenario did not exercise read path")
		}

		var count int64
		if err := db.Model(&testRecord{}).Count(&count).Error; err != nil {
			t.Fatalf("count: %v", err)
		}
		if count != int64(writers*perWriter) {
			t.Fatalf("count mismatch: got %d want %d", count, writers*perWriter)
		}
	})
}

// TestConnector_ReadAfterWriteVisibility tightens the screws on split mode:
// every write must be visible to the very next read on the replica pool.
// This is the test that would catch a regression like re-introducing
// `mode=ro` (which makes replicas miss WAL frames).
func TestConnector_ReadAfterWriteVisibility(t *testing.T) {
	runForBothModes(t, func(t *testing.T, split bool) {
		_, db := newTestConnector(t, testConfig{
			enableSplit:  split,
			busyTimeout:  10000,
			maxOpenConns: 8,
		})

		const iterations = 200
		for i := 0; i < iterations; i++ {
			want := fmt.Sprintf("tag-%d", i)
			if err := db.Create(&testRecord{Bucket: 1, Payload: want}).Error; err != nil {
				t.Fatalf("create i=%d: %v", i, err)
			}
			var got testRecord
			if err := db.Where("payload = ?", want).First(&got).Error; err != nil {
				t.Fatalf("read-after-write i=%d: %v", i, err)
			}
			if got.Payload != want {
				t.Fatalf("i=%d: got %q want %q", i, got.Payload, want)
			}
		}
	})
}

// TestConnector_BurstWriteUnderSustainedReads simulates the realistic case:
// background read traffic running steadily while the writer commits a burst.
// Verifies neither side errors out and the final state is correct.
func TestConnector_BurstWriteUnderSustainedReads(t *testing.T) {
	if testing.Short() {
		t.Skip("burst test skipped in -short mode")
	}

	_, db := newTestConnector(t, testConfig{
		enableSplit:     true,
		busyTimeout:     10000,
		maxOpenConns:    16,
		connMaxIdleTime: 5,
	})

	// Seed so readers have something to scan.
	for i := 0; i < 1000; i++ {
		if err := db.Create(&testRecord{Bucket: i % 10, Payload: fmt.Sprintf("seed-%d", i)}).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var readWg sync.WaitGroup
	var readErrs, readOps atomic.Int64

	const readers = 12
	for r := 0; r < readers; r++ {
		readWg.Add(1)
		go func() {
			defer readWg.Done()
			for ctx.Err() == nil {
				var rs []testRecord
				if err := db.Limit(100).Find(&rs).Error; err != nil {
					readErrs.Add(1)
					return
				}
				readOps.Add(1)
			}
		}()
	}

	// Burst: 5 writers, 400 inserts each = 2000 rows, slammed in fast.
	const writers = 5
	const perWriter = 400
	var writeWg sync.WaitGroup
	var writeErrs atomic.Int64
	for w := 0; w < writers; w++ {
		writeWg.Add(1)
		go func(bucket int) {
			defer writeWg.Done()
			for i := 0; i < perWriter; i++ {
				if err := db.Create(&testRecord{
					Bucket:  bucket,
					Payload: fmt.Sprintf("burst-%d-%d", bucket, i),
				}).Error; err != nil {
					writeErrs.Add(1)
					return
				}
			}
		}(w)
	}
	writeWg.Wait()
	time.Sleep(250 * time.Millisecond)
	cancel()
	readWg.Wait()

	if writeErrs.Load() > 0 {
		t.Fatalf("write errors during burst: %d", writeErrs.Load())
	}
	if readErrs.Load() > 0 {
		t.Fatalf("read errors during burst: %d", readErrs.Load())
	}
	if readOps.Load() < int64(readers) {
		t.Fatalf("expected each reader to issue at least one op, got total %d", readOps.Load())
	}

	var count int64
	if err := db.Model(&testRecord{}).Count(&count).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	wantTotal := int64(1000 + writers*perWriter)
	if count != wantTotal {
		t.Fatalf("count mismatch: got %d want %d", count, wantTotal)
	}
}

// TestConnector_SimultaneousReadWriteUnderLoad runs every kind of operation
// (INSERT, UPDATE, DELETE, SELECT, COUNT) concurrently for a fixed wall-clock
// window. Unlike the other mixed test, every worker starts at the same time
// and stops at the same time, so we exercise the case where writes and reads
// are genuinely in flight against each other for the entire duration.
//
// Asserts: no errors from any worker, every worker class made progress, and
// the final row count is consistent with the issued INSERT/DELETE counts.
func TestConnector_SimultaneousReadWriteUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("simultaneous-load test skipped in -short mode")
	}

	runForBothModes(t, func(t *testing.T, split bool) {
		_, db := newTestConnector(t, testConfig{
			enableSplit:     split,
			busyTimeout:     10000,
			maxOpenConns:    16,
			connMaxIdleTime: 5,
		})

		// Seed enough rows that updaters/deleters/readers always have targets.
		const seedRows = 1000
		for i := 0; i < seedRows; i++ {
			if err := db.Create(&testRecord{
				Bucket:  i % 16,
				Payload: fmt.Sprintf("seed-%d", i),
			}).Error; err != nil {
				t.Fatalf("seed: %v", err)
			}
		}

		const (
			inserters = 4
			updaters  = 4
			deleters  = 2
			readers   = 8
			counters  = 2
			duration  = 2 * time.Second
		)

		var (
			insertOps, updateOps, deleteOps, readOps, countOps atomic.Int64
			insertErr, updateErr, deleteErr, readErr, countErr atomic.Int64
		)

		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		var wg sync.WaitGroup

		spawn := func(n int, fn func()) {
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					fn()
				}()
			}
		}

		// Inserters: continuously add new rows.
		spawn(inserters, func() {
			i := 0
			for ctx.Err() == nil {
				if err := db.Create(&testRecord{
					Bucket:  i % 16,
					Payload: fmt.Sprintf("ins-%d", i),
				}).Error; err != nil {
					insertErr.Add(1)
					return
				}
				insertOps.Add(1)
				i++
			}
		})

		// Updaters: rewrite an existing row's payload.
		spawn(updaters, func() {
			i := 0
			for ctx.Err() == nil {
				bucket := i % 16
				res := db.Model(&testRecord{}).
					Where("bucket = ?", bucket).
					Limit(1).
					Update("payload", fmt.Sprintf("upd-%d", i))
				if res.Error != nil {
					updateErr.Add(1)
					return
				}
				updateOps.Add(res.RowsAffected)
				i++
			}
		})

		// Deleters: drop one row at a time, only from buckets that the
		// inserters and seeders have populated heavily so deletes don't stall.
		spawn(deleters, func() {
			i := 0
			for ctx.Err() == nil {
				bucket := i % 16
				// Remove only inserter rows so seed rows stay roughly intact;
				// keeps the total count math tractable.
				res := db.Where("bucket = ? AND payload LIKE ?", bucket, "ins-%").
					Limit(1).
					Delete(&testRecord{})
				if res.Error != nil {
					deleteErr.Add(1)
					return
				}
				deleteOps.Add(res.RowsAffected)
				i++
			}
		})

		// Readers: targeted lookups against buckets being mutated.
		spawn(readers, func() {
			i := 0
			for ctx.Err() == nil {
				var rs []testRecord
				if err := db.
					Where("bucket = ?", i%16).
					Limit(50).
					Find(&rs).Error; err != nil {
					readErr.Add(1)
					return
				}
				readOps.Add(1)
				i++
			}
		})

		// Counters: full-table COUNT, the canonical "is the snapshot stable" probe.
		spawn(counters, func() {
			for ctx.Err() == nil {
				var n int64
				if err := db.Model(&testRecord{}).Count(&n).Error; err != nil {
					countErr.Add(1)
					return
				}
				countOps.Add(1)
			}
		})

		wg.Wait()

		if got := insertErr.Load(); got > 0 {
			t.Errorf("insert errors: %d", got)
		}
		if got := updateErr.Load(); got > 0 {
			t.Errorf("update errors: %d", got)
		}
		if got := deleteErr.Load(); got > 0 {
			t.Errorf("delete errors: %d", got)
		}
		if got := readErr.Load(); got > 0 {
			t.Errorf("read errors: %d", got)
		}
		if got := countErr.Load(); got > 0 {
			t.Errorf("count errors: %d", got)
		}

		// Every worker class must have made forward progress; if any went to
		// zero we are not actually exercising "simultaneous" anything.
		if insertOps.Load() == 0 {
			t.Errorf("inserter did no work")
		}
		if updateOps.Load() == 0 {
			t.Errorf("updater did no work")
		}
		if deleteOps.Load() == 0 {
			t.Errorf("deleter did no work")
		}
		if readOps.Load() == 0 {
			t.Errorf("reader did no work")
		}
		if countOps.Load() == 0 {
			t.Errorf("counter did no work")
		}

		var finalCount int64
		if err := db.Model(&testRecord{}).Count(&finalCount).Error; err != nil {
			t.Fatalf("final count: %v", err)
		}
		want := int64(seedRows) + insertOps.Load() - deleteOps.Load()
		if finalCount != want {
			t.Fatalf("final count mismatch: got %d want %d (seed=%d ins=%d del=%d)",
				finalCount, want, seedRows, insertOps.Load(), deleteOps.Load())
		}

		t.Logf("split=%v ops: insert=%d update=%d delete=%d read=%d count=%d",
			split, insertOps.Load(), updateOps.Load(), deleteOps.Load(), readOps.Load(), countOps.Load())
	})
}

// TestConnector_TransactionsRouteToPrimary verifies that an explicit
// transaction (which must always go to the writer) does not error out and
// commits cleanly even when split mode is on.
func TestConnector_TransactionsRouteToPrimary(t *testing.T) {
	_, db := newTestConnector(t, testConfig{
		enableSplit: true,
		busyTimeout: 10000,
	})

	const txCount = 50
	const perTx = 10

	var wg sync.WaitGroup
	var failures atomic.Int64
	for tx := 0; tx < txCount; tx++ {
		wg.Add(1)
		go func(bucket int) {
			defer wg.Done()
			err := db.Transaction(func(tx *gorm.DB) error {
				for i := 0; i < perTx; i++ {
					if err := tx.Create(&testRecord{
						Bucket:  bucket,
						Payload: fmt.Sprintf("tx-%d-%d", bucket, i),
					}).Error; err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				failures.Add(1)
				t.Errorf("tx %d: %v", bucket, err)
			}
		}(tx)
	}
	wg.Wait()

	if failures.Load() > 0 {
		return
	}

	var count int64
	if err := db.Model(&testRecord{}).Count(&count).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != int64(txCount*perTx) {
		t.Fatalf("count mismatch: got %d want %d", count, txCount*perTx)
	}
}

// ---------------------------------------------------------------------------
// SLOW SQL prevention scenarios
//
// These tests exist to catch regressions that would surface as "SLOW SQL" log
// entries in production. The connector's gorm logger flags anything ≥ 200ms,
// so the same threshold is reused here.
// ---------------------------------------------------------------------------

// TestSlowSQL_ReadsStayFastUnderSustainedWrites: with writers continuously
// committing into the database, the reader pool should not see read latency
// spike. This is the primary value proposition of read/write split + WAL —
// readers and writers do not contend on the same connections.
//
// Asserts: < 2% of reads cross the slow threshold.
func TestSlowSQL_ReadsStayFastUnderSustainedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("slow-sql test skipped in -short mode")
	}

	runForBothModes(t, func(t *testing.T, split bool) {
		_, db := newTestConnector(t, testConfig{
			enableSplit:     split,
			busyTimeout:     10000,
			maxOpenConns:    16,
			connMaxIdleTime: 1,
		})

		const seedRows = 1000
		for i := 0; i < seedRows; i++ {
			if err := db.Create(&testRecord{
				Bucket:  i % 16,
				Payload: fmt.Sprintf("seed-%d", i),
			}).Error; err != nil {
				t.Fatalf("seed: %v", err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		var readLat, writeLat latencyRecorder
		var readErrs, writeErrs atomic.Int64

		const writers = 4
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(b int) {
				defer wg.Done()
				i := 0
				for ctx.Err() == nil {
					start := time.Now()
					err := db.Create(&testRecord{
						Bucket:  b,
						Payload: fmt.Sprintf("w-%d-%d", b, i),
					}).Error
					writeLat.Add(time.Since(start))
					if err != nil {
						writeErrs.Add(1)
						return
					}
					i++
				}
			}(w)
		}

		const readers = 8
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				i := 0
				for ctx.Err() == nil {
					var rs []testRecord
					start := time.Now()
					err := db.Where("bucket = ?", i%16).Limit(50).Find(&rs).Error
					readLat.Add(time.Since(start))
					if err != nil {
						readErrs.Add(1)
						return
					}
					i++
				}
			}()
		}
		wg.Wait()

		if writeErrs.Load() > 0 {
			t.Fatalf("write errors: %d", writeErrs.Load())
		}
		if readErrs.Load() > 0 {
			t.Fatalf("read errors: %d", readErrs.Load())
		}

		rs := readLat.summary(slowSQLThreshold)
		ws := writeLat.summary(slowSQLThreshold)
		t.Logf("split=%v reads:  %s", split, rs)
		t.Logf("split=%v writes: %s", split, ws)

		if rs.n < 100 {
			t.Fatalf("not enough read samples: %d", rs.n)
		}

		readSlowRatio := float64(rs.slowCount) / float64(rs.n)
		if readSlowRatio > 0.02 {
			t.Errorf("read slow-query ratio %.2f%% (>%d slow / %d total) exceeds 2%% budget",
				readSlowRatio*100, rs.slowCount, rs.n)
		}
	})
}

// TestSlowSQL_LargeTransactionDoesNotBlockReads: a single large transaction
// (5000 inserts) running on the writer must not cause concurrent reads to
// stall. In WAL mode, readers see a consistent pre-commit snapshot and do
// not block on the writer's lock; the same has to hold through dbresolver.
//
// Asserts: < 5% of reads issued during the big transaction are slow.
func TestSlowSQL_LargeTransactionDoesNotBlockReads(t *testing.T) {
	if testing.Short() {
		t.Skip("slow-sql test skipped in -short mode")
	}

	_, db := newTestConnector(t, testConfig{
		enableSplit:     true,
		busyTimeout:     10000,
		maxOpenConns:    8,
		connMaxIdleTime: 1,
	})

	for i := 0; i < 200; i++ {
		if err := db.Create(&testRecord{
			Bucket:  i % 16,
			Payload: fmt.Sprintf("seed-%d", i),
		}).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var readLat latencyRecorder
	var readErrs atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for ctx.Err() == nil {
			var rs []testRecord
			start := time.Now()
			err := db.Where("bucket = ?", i%16).Limit(50).Find(&rs).Error
			readLat.Add(time.Since(start))
			if err != nil {
				readErrs.Add(1)
				return
			}
			i++
		}
	}()

	// Big single-transaction write while readers are running.
	if err := db.Transaction(func(tx *gorm.DB) error {
		for i := 0; i < 5000; i++ {
			if err := tx.Create(&testRecord{
				Bucket:  i % 16,
				Payload: fmt.Sprintf("bigtx-%d", i),
			}).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("big tx: %v", err)
	}

	// Let reader observe the post-commit state briefly, then stop.
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	if readErrs.Load() > 0 {
		t.Fatalf("read errors during big tx: %d", readErrs.Load())
	}

	rs := readLat.summary(slowSQLThreshold)
	t.Logf("reads during 5000-row tx: %s", rs)
	if rs.n < 50 {
		t.Fatalf("too few read samples: %d", rs.n)
	}

	slowRatio := float64(rs.slowCount) / float64(rs.n)
	if slowRatio > 0.05 {
		t.Errorf("read slow-query ratio %.2f%% during big tx exceeds 5%% budget", slowRatio*100)
	}
}

// TestSlowSQL_NoReadLatencyDegradationOverTime catches the WAL-pin failure
// mode: if idle reader connections are not closed promptly, the WAL grows
// and read latency creeps up over time. Compare median latency in the first
// quarter of the run against the last quarter — if it's much worse, idle
// snapshots are not being released.
//
// Asserts: last-quarter median ≤ 5× first-quarter median.
func TestSlowSQL_NoReadLatencyDegradationOverTime(t *testing.T) {
	if testing.Short() {
		t.Skip("slow-sql test skipped in -short mode")
	}

	_, db := newTestConnector(t, testConfig{
		enableSplit:     true,
		busyTimeout:     10000,
		maxOpenConns:    8,
		connMaxIdleTime: 1,
	})

	for i := 0; i < 500; i++ {
		if err := db.Create(&testRecord{
			Bucket:  i % 16,
			Payload: fmt.Sprintf("seed-%d", i),
		}).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// One sustained writer to keep the WAL active.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for ctx.Err() == nil {
			if err := db.Create(&testRecord{
				Bucket:  i % 16,
				Payload: fmt.Sprintf("bg-%d", i),
			}).Error; err != nil {
				return
			}
			i++
		}
	}()

	type stamp struct {
		at time.Time
		d  time.Duration
	}
	var (
		mu      sync.Mutex
		samples []stamp
	)
	const readers = 4
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for ctx.Err() == nil {
				var rs []testRecord
				start := time.Now()
				if err := db.Where("bucket = ?", i%16).Limit(50).Find(&rs).Error; err != nil {
					return
				}
				d := time.Since(start)
				mu.Lock()
				samples = append(samples, stamp{at: start, d: d})
				mu.Unlock()
				i++
			}
		}()
	}
	wg.Wait()

	if len(samples) < 200 {
		t.Fatalf("not enough samples for degradation analysis: %d", len(samples))
	}

	// Sort by timestamp so we slice by wall-clock order, not arrival order.
	sort.Slice(samples, func(i, j int) bool { return samples[i].at.Before(samples[j].at) })

	medianOf := func(s []stamp) time.Duration {
		ds := make([]time.Duration, len(s))
		for i, x := range s {
			ds[i] = x.d
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		return ds[len(ds)/2]
	}

	n := len(samples)
	first := samples[:n/4]
	last := samples[3*n/4:]
	firstMed := medianOf(first)
	lastMed := medianOf(last)

	t.Logf("read latency drift: first-quarter median=%v, last-quarter median=%v (samples=%d)",
		firstMed, lastMed, n)

	if lastMed > firstMed*5 {
		t.Errorf("read latency degraded over time: first-quarter median=%v, last-quarter median=%v (>5x slower)",
			firstMed, lastMed)
	}
	if lastMed > slowSQLThreshold {
		t.Errorf("last-quarter median read %v already crosses slow threshold %v", lastMed, slowSQLThreshold)
	}
}

// TestSlowSQL_WALCheckpointProgressesAfterIdle directly verifies that no
// reader connection pins the WAL once it has been idle past
// conn_max_idle_time. PRAGMA wal_checkpoint(TRUNCATE) is run on the primary
// underlying *sql.DB (bypassing dbresolver routing). It returns three ints:
// busy, log, checkpointed. busy=0 means every reader's snapshot has been
// released — i.e. no WAL pin.
func TestSlowSQL_WALCheckpointProgressesAfterIdle(t *testing.T) {
	if testing.Short() {
		t.Skip("slow-sql test skipped in -short mode")
	}

	c, db := newTestConnector(t, testConfig{
		enableSplit:     true,
		busyTimeout:     10000,
		maxOpenConns:    8,
		connMaxIdleTime: 1,
	})
	_ = c

	// Burst of mixed traffic so the WAL accumulates frames and reader conns
	// register read marks.
	var wg sync.WaitGroup
	const writers = 4
	const writesPerWriter = 500
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				if err := db.Create(&testRecord{
					Bucket:  b,
					Payload: fmt.Sprintf("burst-%d-%d", b, i),
				}).Error; err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(w)
	}
	const readers = 4
	const readsPerReader = 200
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				var rs []testRecord
				if err := db.Limit(50).Find(&rs).Error; err != nil {
					t.Errorf("read: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Wait long enough for the database/sql idle-connection cleaner to reap
	// idle conns (cleaner runs at min(maxLifetime, maxIdleTime) intervals;
	// with maxIdleTime=1s it ticks every 1s).
	time.Sleep(3 * time.Second)

	// Run the checkpoint directly on the primary sql.DB to bypass dbresolver
	// routing — we explicitly want to see the writer's view of WAL state.
	primary, err := db.DB()
	if err != nil {
		t.Fatalf("get primary sql.DB: %v", err)
	}
	var busy, logPages, checkpointed int
	row := primary.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)")
	if err := row.Scan(&busy, &logPages, &checkpointed); err != nil {
		t.Fatalf("wal_checkpoint pragma: %v", err)
	}
	t.Logf("wal_checkpoint(TRUNCATE) busy=%d log_pages=%d checkpointed=%d",
		busy, logPages, checkpointed)

	if busy != 0 {
		t.Errorf("wal_checkpoint reported busy=%d — a reader is still pinning WAL frames after the idle window; either ConnMaxIdleTime is not wired or the idle conn is leaking a snapshot", busy)
	}
}
