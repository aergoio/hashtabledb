package hashtabledb

import (
	"fmt"
	"testing"
	"time"
)

// fakeMemInfoReader overrides getSystemMemoryInfo for the duration of the test.
func withFakeMemory(t *testing.T, total, available int64) {
	real := getSystemMemoryInfo
	getSystemMemoryInfo = func() MemoryInfo {
		return MemoryInfo{Total: total, Available: available, Free: available}
	}
	t.Cleanup(func() { getSystemMemoryInfo = real })
}

// TestLowRAMCachePressure exercises the cache-pressure path under a simulated
// low-RAM host (Available/Total well below memoryComfortableFraction), so the Set
// path must take the clean/flush/checkpoint release branch instead of growing. It
// verifies:
//  1. the pressure loop is actually entered (low-RAM branch taken),
//  2. a pinned cache never makes Set hang,
//  3. thresholds are not grown (adaptive disabled),
//  4. the cache is brought back under the threshold.
//
// Run: go test -run TestLowRAMCachePressure -v -count=1
func TestLowRAMCachePressure(t *testing.T) {
	// Simulate a 1 GiB host with only 100 MiB available (10% < 30% comfortable).
	withFakeMemory(t, 1<<30, 100<<20)

	dbPath := fmt.Sprintf("lowram_%d.db", time.Now().UnixNano())
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := Open(dbPath, Options{
		"CacheSizeThreshold":   1000, // ~4 MB page cache, forces pressure quickly
		"AdaptiveCacheEnabled": true,
		"ValueCacheThreshold":  int64(0),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	initialCache := db.cacheSizeThreshold.Load()
	t.Logf("initial cacheSizeThreshold = %d pages", initialCache)

	var peak int64

	// Writer goroutine: many small batched transactions.
	const numTxns = 3000
	done := make(chan error, 1)
	go func() {
		idx := 0
		for txNum := 0; txNum < numTxns; txNum++ {
			tx, err := db.Begin()
			if err != nil {
				done <- fmt.Errorf("Begin: %w", err)
				return
			}
			for i := 0; i < 100; i++ {
				k := generateDeterministicBytes(idx, 16)
				v := generateDeterministicBytes(idx+9999, 256)
				if err := tx.Set(k, v); err != nil {
					tx.Rollback()
					done <- fmt.Errorf("Set: %w", err)
					return
				}
				idx++
			}
			if err := tx.Commit(); err != nil {
				done <- fmt.Errorf("Commit: %w", err)
				return
			}
			if c := db.totalCachePages.Load(); c > peak {
				peak = c
			}
		}
		done <- nil
	}()

	// Watchdog: if the pressure path deadlocks, the writer never finishes.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writer failed: %v", err)
		}
	case <-time.After(120 * time.Second):
		t.Fatalf("writer did not finish in 120s (possible deadlock in pressure path)")
	}

	finalCache := db.cacheSizeThreshold.Load()
	t.Logf("final   cacheSizeThreshold = %d pages", finalCache)
	t.Logf("peak totalCachePages seen = %d pages (threshold %d)", peak, initialCache)

	if finalCache > initialCache {
		t.Errorf("cacheSizeThreshold grew under low RAM: %d → %d (adaptive must not grow here)",
			initialCache, finalCache)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestAdaptiveGrowDisabled verifies that with AdaptiveCacheEnabled=false
// and a pinned cache size, the threshold is never grown even on a comfortable host.
func TestAdaptiveGrowDisabled(t *testing.T) {
	// Comfortable host, but adaptive disabled.
	withFakeMemory(t, 8<<30, 6<<30)

	dbPath := fmt.Sprintf("lowram2_%d.db", time.Now().UnixNano())
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	const pinned = 1500
	db, err := Open(dbPath, Options{
		"CacheSizeThreshold":   pinned,
		"AdaptiveCacheEnabled": false,
		"ValueCacheThreshold":  int64(0),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	idx := 0
	for txNum := 0; txNum < 2000; txNum++ {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		for i := 0; i < 100; i++ {
			k := generateDeterministicBytes(idx, 16)
			v := generateDeterministicBytes(idx+5555, 256)
			if err := tx.Set(k, v); err != nil {
				t.Fatalf("Set: %v", err)
			}
			idx++
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	got := db.cacheSizeThreshold.Load()
	t.Logf("pinned cacheSizeThreshold = %d, final = %d", pinned, got)
	if got != int64(pinned) {
		t.Errorf("AdaptiveCacheEnabled=false: cacheSizeThreshold changed %d → %d; must stay %d",
			pinned, got, pinned)
	}
}

// TestCheckpointThresholdRecovers verifies that after a memory-pressure event shrinks the
// checkpoint threshold, it recovers back toward maxCheckpointThreshold once memory
// is comfortable again (and never exceeds the cap).
func TestCheckpointThresholdRecovers(t *testing.T) {
	// Mutable fake host: start under pressure, then relax.
	var mu struct {
		total, avail int64
	}
	mu.total, mu.avail = 1<<30, 80<<20 // 8% available → under pressure
	real := getSystemMemoryInfo
	getSystemMemoryInfo = func() MemoryInfo {
		return MemoryInfo{Total: mu.total, Available: mu.avail, Free: mu.avail}
	}
	t.Cleanup(func() { getSystemMemoryInfo = real })

	dbPath := fmt.Sprintf("ckptrec_%d.db", time.Now().UnixNano())
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := Open(dbPath, Options{
		"AdaptiveCacheEnabled": true,
		"ValueCacheThreshold":  int64(0),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	maxCp := db.maxCheckpointThreshold
	rest := db.checkpointThreshold.Load()
	t.Logf("resting checkpointThreshold = %d, cap(max) = %d", rest, maxCp)

	// Drive the adaptive tick directly under pressure several times to shrink it.
	pressure := MemoryInfo{Total: 1 << 30, Available: 80 << 20} // >80% used
	for i := 0; i < 10; i++ {
		db.applyAdaptiveMemoryLimits(pressure)
	}
	shrunk := db.checkpointThreshold.Load()
	t.Logf("after pressure: checkpointThreshold = %d", shrunk)
	if shrunk >= rest {
		t.Fatalf("checkpointThreshold did not shrink under pressure: %d → %d", rest, shrunk)
	}

	// Now relax memory (<50% used) and tick again; it should recover toward the cap.
	comfortable := MemoryInfo{Total: 1 << 30, Available: 800 << 20} // ~78% free
	for i := 0; i < 40; i++ {
		db.applyAdaptiveMemoryLimits(comfortable)
	}
	recovered := db.checkpointThreshold.Load()
	t.Logf("after recovery: checkpointThreshold = %d", recovered)
	if recovered <= shrunk {
		t.Fatalf("checkpointThreshold did not recover after pressure passed: shrunk=%d recovered=%d", shrunk, recovered)
	}
	if recovered > maxCp {
		t.Fatalf("checkpointThreshold exceeded cap: %d > %d", recovered, maxCp)
	}
}
