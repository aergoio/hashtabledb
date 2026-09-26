package hashtabledb

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Bench constants: 2M inserts, random 33-byte keys, 250-byte values,
// explicit transactions batched at 50k sets. Tuned via the environment for
// quick experiments
const (
	benchTotalItems = 2_000_000
	benchTxSize     = 50_000
	benchKeySize    = 33
	benchValueSize  = 250
	benchRuns       = 2
)

// benchFixture holds the pre-generated workload shared by all runs
type benchFixture struct {
	keys    [][]byte
	valueFn func(i int) []byte
}

// newBenchFixture pre-generates the random keys once so every run inserts the
// same workload, and derives each 250-byte value from a shared random block
// with the item index stamped in front, so values differ per key without
// paying for per-set randomness
func newBenchFixture(t *testing.T) *benchFixture {
	t.Helper()

	keys := make([][]byte, benchTotalItems)
	buf := make([]byte, benchKeySize)
	for i := range keys {
		if _, err := rand.Read(buf); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}
		keys[i] = append([]byte(nil), buf...)
	}

	base := make([]byte, benchValueSize)
	if _, err := rand.Read(base); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	valueFn := func(i int) []byte {
		value := append([]byte(nil), base...)
		binary.BigEndian.PutUint32(value, uint32(i))
		return value
	}

	return &benchFixture{keys: keys, valueFn: valueFn}
}

// benchInsertTransactions writes the whole workload through explicit
// transactions of benchTxSize sets each
func benchInsertTransactions(t *testing.T, db *DB, fx *benchFixture) {
	t.Helper()

	for start := 0; start < benchTotalItems; start += benchTxSize {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		for i := start; i < start+benchTxSize && i < benchTotalItems; i++ {
			if err := tx.Set(fx.keys[i], fx.valueFn(i)); err != nil {
				t.Fatalf("tx.Set(%d): %v", i, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("tx.Commit at %d: %v", start, err)
		}
	}
}

// benchInsertBulk writes the whole workload through one bulk session; the
// sub-batch rotation happens inside set at the dirty page threshold
func benchInsertBulk(t *testing.T, db *DB, fx *benchFixture) {
	t.Helper()

	bulk, err := db.NewBulk()
	if err != nil {
		t.Fatalf("NewBulk: %v", err)
	}
	for i := 0; i < benchTotalItems; i++ {
		if err := bulk.Set(fx.keys[i], fx.valueFn(i)); err != nil {
			t.Fatalf("bulk.Set(%d): %v", i, err)
		}
	}
	if err := bulk.Flush(); err != nil {
		t.Fatalf("bulk.Flush: %v", err)
	}
}

// benchInsertAutoCommit writes the whole workload through plain auto-commit
// sets: one transaction per set, the pre-bulk baseline
func benchInsertAutoCommit(t *testing.T, db *DB, fx *benchFixture) {
	t.Helper()

	for i := 0; i < benchTotalItems; i++ {
		if err := db.Set(fx.keys[i], fx.valueFn(i)); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
}

// TestBulkVsTransactionsBenchmark compares explicit 50k-set transactions,
// one bulk session, and plain auto-commit sets on the same workload.
// Skipped unless HTDB_BENCH_BULK_VS_TX is set: it writes a few GB and runs
// for minutes
func TestBulkVsTransactionsBenchmark(t *testing.T) {
	if os.Getenv("HTDB_BENCH_BULK_VS_TX") == "" {
		t.Skip("set HTDB_BENCH_BULK_VS_TX=1 to run the bulk vs transactions benchmark")
	}
	fx := newBenchFixture(t)

	type strategy struct {
		name   string
		insert func(t *testing.T, db *DB, fx *benchFixture)
	}
	strategies := []strategy{
		{"auto-commit", benchInsertAutoCommit},
		{"transactions-50k", benchInsertTransactions},
		{"bulk", benchInsertBulk},
	}

	for _, writeMode := range writeModeLabels {
		for run := 1; run <= benchRuns; run++ {
			for _, s := range strategies {
				s := s
				name := fmt.Sprintf("%s/%s/run%d", writeMode, s.name, run)
				t.Run(name, func(t *testing.T) {
					dbPath := filepath.Join(t.TempDir(), "bench.db")
					db := openTestDB(t, dbPath, writeMode)

					start := time.Now()
					s.insert(t, db, fx)
					elapsed := time.Since(start)

					// Transactions opened by the run (txnSequence minus the
					// Open-time base of 1): ~2M for auto-commit, 40 for
					// transactions-50k, and the sub-batch count for bulk —
					// each sub-batch rotation pairs one commit with the
					// begin of the next, and Flush adds the final commit
					t.Logf("RESULT %s: %v (%.0f ops/s, transactions=%d dirtyThreshold=%d cacheThreshold=%d)",
						name, elapsed.Round(time.Millisecond),
						float64(benchTotalItems)/elapsed.Seconds(),
						db.txnSequence-1,
						db.dirtyPageThreshold.Load(), db.cacheSizeThreshold.Load())

					// Verify a sample of the inserted keys survives a reopen
					if err := db.Close(); err != nil {
						t.Fatalf("Close: %v", err)
					}
					db2 := openTestDB(t, dbPath, writeMode)
					defer func() {
						db2.Close()
					}()
					for i := 0; i < benchTotalItems; i += 100_003 {
						value, err := db2.Get(fx.keys[i])
						if err != nil {
							t.Fatalf("Get(%d) after reopen: %v", i, err)
						}
						if !bytes.Equal(value, fx.valueFn(i)) {
							t.Fatalf("Get(%d) after reopen: wrong value", i)
						}
					}
				})
			}
		}
	}
}

// TestBulkThresholdSweepBenchmark sweeps the bulk rotation threshold in
// worker mode to find where bulk catches up with 50k-set transactions, which
// win on the default threshold because they never rotate. Threshold 0 means
// the engine default (10% of the auto-sized cache). Absolute thresholds are
// not capped and do not rescale with adaptive cache sizing. Skipped unless
// HTDB_BENCH_BULK_SWEEP is set
func TestBulkThresholdSweepBenchmark(t *testing.T) {
	if os.Getenv("HTDB_BENCH_BULK_SWEEP") == "" {
		t.Skip("set HTDB_BENCH_BULK_SWEEP=1 to run the bulk threshold sweep")
	}
	fx := newBenchFixture(t)

	// Baseline: explicit 50k-set transactions at the default threshold
	func() {
		db := openTestDB(t, filepath.Join(t.TempDir(), "bench.db"), "wal")
		start := time.Now()
		benchInsertTransactions(t, db, fx)
		elapsed := time.Since(start)
		t.Logf("RESULT worker/transactions-50k/default: %v (%.0f ops/s, txnSequence=%d)",
			elapsed.Round(time.Millisecond),
			float64(benchTotalItems)/elapsed.Seconds(), db.txnSequence)
		db.Close()
	}()

	for _, threshold := range []int{0, 4096, 8192, 16384, 32768, 50000, 100000} {
		threshold := threshold
		name := "default"
		if threshold > 0 {
			name = strconv.Itoa(threshold)
		}
		t.Run(name, func(t *testing.T) {
			var extra []Options
			if threshold > 0 {
				extra = append(extra, Options{"DirtyPageThreshold": strconv.Itoa(threshold)})
			}
			db := openTestDB(t, filepath.Join(t.TempDir(), "bench.db"), "wal", extra...)

			start := time.Now()
			benchInsertBulk(t, db, fx)
			elapsed := time.Since(start)

			t.Logf("RESULT worker/bulk/%s: %v (%.0f ops/s, txnSequence=%d dirtyThreshold=%d)",
				name, elapsed.Round(time.Millisecond),
				float64(benchTotalItems)/elapsed.Seconds(),
				db.txnSequence, db.dirtyPageThreshold.Load())
			db.Close()
		})
	}
}

// ---------------------------------------------------------------------------
// Get() lookup benchmark: builds a database with a bulk session, then
// measures db.Get() over rotating existing keys (hits) and over absent keys
// that stay inside the slot domain (misses). Skipped unless HTDB_BENCH_GET
// is set. Item count is tuned via HTDB_BENCH_GET_ITEMS (default 200000)
// ---------------------------------------------------------------------------

const (
	benchGetKeySize   = 33
	benchGetValueSize = 250
)

func benchGetItemCount() int {
	if s := os.Getenv("HTDB_BENCH_GET_ITEMS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 200_000
}

// benchGetSetup builds the database and reopens it so the benchmark measures
// the committed read path; returns the db and the keys to look up
func benchGetSetup(b *testing.B, miss bool) (*DB, [][]byte) {
	b.Helper()

	n := benchGetItemCount()
	dbPath := filepath.Join(b.TempDir(), "get.db")

	// Pre-generate the random keys and derived values once
	keys := make([][]byte, n)
	buf := make([]byte, benchGetKeySize)
	for i := range keys {
		if _, err := rand.Read(buf); err != nil {
			b.Fatalf("rand.Read: %v", err)
		}
		keys[i] = append([]byte(nil), buf...)
	}
	base := make([]byte, benchGetValueSize)
	if _, err := rand.Read(base); err != nil {
		b.Fatalf("rand.Read: %v", err)
	}

	// Write the whole workload through one bulk session
	db := openTestDB(b, dbPath, "wal", benchGetExtraOpts()...)
	bulk, err := db.NewBulk()
	if err != nil {
		b.Fatalf("NewBulk: %v", err)
	}
	for i := 0; i < n; i++ {
		value := append([]byte(nil), base...)
		binary.BigEndian.PutUint32(value, uint32(i))
		if err := bulk.Set(keys[i], value); err != nil {
			b.Fatalf("bulk.Set(%d): %v", i, err)
		}
	}
	if err := bulk.Flush(); err != nil {
		b.Fatalf("bulk.Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}

	// Reopen: the benchmark measures the reopened, committed state
	db = openTestDB(b, dbPath, "wal", benchGetExtraOpts()...)

	// Verify a sample of the keys
	for i := 0; i < n; i += 40_003 {
		value, err := db.Get(keys[i])
		if err != nil {
			b.Fatalf("Get(%d): %v", i, err)
		}
		if !bytes.Equal(value, valueFnB(base, i)) {
			b.Fatalf("Get(%d): wrong value", i)
		}
	}

	// Absent keys stay inside the slot domain: an existing key with its
	// last byte flipped, verified to be missing
	lookup := keys
	if miss {
		lookup = make([][]byte, n)
		for i := range keys {
			k := append([]byte(nil), keys[i]...)
			k[len(k)-1] ^= 0xFF
			lookup[i] = k
			if _, err := db.Get(k); !errors.Is(err, ErrKeyNotFound) {
				b.Fatalf("miss key %d collides: %v", i, err)
			}
		}
	}
	return db, lookup
}

// benchGetExtraOpts reads HTDB_BENCH_GET_TABLE_PAGES (main index pages,
// 818 slots each); a small table packs many entries per slot and makes the
// sub-page scan the visible cost. Unset keeps the engine default
func benchGetExtraOpts() []Options {
	if s := os.Getenv("HTDB_BENCH_GET_TABLE_PAGES"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return []Options{{"HashTableSize": n}}
		}
	}
	return nil
}

func valueFnB(base []byte, i int) []byte {
	value := append([]byte(nil), base...)
	binary.BigEndian.PutUint32(value, uint32(i))
	return value
}

var benchGetSink int

func BenchmarkGet(b *testing.B) {
	if os.Getenv("HTDB_BENCH_GET") == "" {
		b.Skip("set HTDB_BENCH_GET=1 to run the Get benchmark")
	}
	b.Run("hits", func(b *testing.B) {
		db, keys := benchGetSetup(b, false)
		defer func() { db.Close() }()
		h := uint32(12345)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h = h*1664525 + 1013904223
			value, err := db.Get(keys[(h>>8)%uint32(len(keys))])
			if err != nil {
				b.Fatalf("Get: %v", err)
			}
			benchGetSink += len(value)
		}
	})
	b.Run("misses", func(b *testing.B) {
		db, keys := benchGetSetup(b, true)
		defer func() { db.Close() }()
		h := uint32(12345)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h = h*1664525 + 1013904223
			_, err := db.Get(keys[(h>>8)%uint32(len(keys))])
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				b.Fatalf("Get: %v", err)
			}
			benchGetSink++
		}
	})
}
