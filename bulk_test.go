package hashtabledb

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// bulkKey builds a key that spreads over many index pages so each Set dirties
// several pages and the internal auto-commit threshold trips quickly
func bulkKey(i int) []byte {
	return []byte(fmt.Sprintf("bulk/key-%06d", i))
}

// bulkValue builds a value large enough to force a few hybrid pages per key
func bulkValue(i int, tag string) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("%s-%06d|", tag, i)), 8)
}

// TestBulkFlushPersists writes more than enough keys to trip several internal
// auto-commits, flushes, and verifies every key survives a reopen: the
// internal commits must leave the same durable state an ordinary transaction
// commit leaves
func TestBulkFlushPersists(t *testing.T) {
	withWriteModes(t, func(t *testing.T, writeMode string) {
		dbPath := testDBPath(".", "test_bulk_flush.db", writeMode)
		cleanupTestFiles(dbPath)

		db := openTestDB(t, dbPath, writeMode, Options{"DirtyPageThreshold": 20})
		defer func() {
			db.Close()
			cleanupTestFiles(dbPath)
		}()

		const numKeys = 2000

		bulk, err := db.NewBulk()
		if err != nil {
			t.Fatalf("NewBulk: %v", err)
		}
		for i := 0; i < numKeys; i++ {
			if err := bulk.Set(bulkKey(i), bulkValue(i, "v")); err != nil {
				t.Fatalf("bulk.Set(%d): %v", i, err)
			}
		}

		// The internal auto-commits must have advanced the cloning mark
		// while the bulk was running
		if db.cloningSequence <= 0 {
			t.Fatalf("cloningSequence %d: no internal commit fired during the bulk", db.cloningSequence)
		}

		if err := bulk.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		// Everything must be readable right after the flush
		for i := 0; i < numKeys; i += 97 {
			value, err := db.Get(bulkKey(i))
			if err != nil {
				t.Fatalf("Get(%d) after Flush: %v", i, err)
			}
			if !bytes.Equal(value, bulkValue(i, "v")) {
				t.Fatalf("Get(%d) after Flush: wrong value", i)
			}
		}

		// Reopen and verify the internal commits made the data durable
		db.Close()
		db2 := openTestDB(t, dbPath, writeMode, Options{"DirtyPageThreshold": 20})
		defer func() {
			db2.Close()
			cleanupTestFiles(dbPath)
		}()
		for i := 0; i < numKeys; i += 97 {
			value, err := db2.Get(bulkKey(i))
			if err != nil {
				t.Fatalf("Get(%d) after reopen: %v", i, err)
			}
			if !bytes.Equal(value, bulkValue(i, "v")) {
				t.Fatalf("Get(%d) after reopen: wrong value", i)
			}
		}
	})
}

// TestBulkDiscardRollsBackToLastInternalCommit verifies the rollback
// semantics of Discard: keys from sub-batches already committed by the
// internal auto-commit stay, everything written after the last internal
// commit is rolled back. The rotation threshold is frozen before the tail
// wave: the flusher drains dirty pages asynchronously, so the live dirty
// count is racy and the tail must not depend on it
func TestBulkDiscardRollsBackToLastInternalCommit(t *testing.T) {
	dbPath := testDBPath(".", "test_bulk_discard.db", WAL_NoSync)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, WAL_NoSync, Options{"DirtyPageThreshold": 20})
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Seed a pre-bulk key so the bulk starts from a non-empty database
	if err := db.Set([]byte("pre-bulk"), []byte("pre-value")); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	bulk, err := db.NewBulk()
	if err != nil {
		t.Fatalf("NewBulk: %v", err)
	}

	// First wave: enough keys to trip several internal commits
	const wave1 = 800
	for i := 0; i < wave1; i++ {
		if err := bulk.Set(bulkKey(i), bulkValue(i, "w1")); err != nil {
			t.Fatalf("bulk.Set(%d): %v", i, err)
		}
	}
	cloningSeqAfterWave1 := db.cloningSequence
	if cloningSeqAfterWave1 <= 0 {
		t.Fatalf("cloningSequence %d: no internal commit fired during wave 1", cloningSeqAfterWave1)
	}

	// Freeze the rotation boundary for the tail wave: the flusher drains
	// dirty pages asynchronously, so the live dirty count is racy and the
	// tail keys must not depend on it
	if err := db.SetOption("DirtyPageThreshold", 1000000); err != nil {
		t.Fatalf("SetOption DirtyPageThreshold: %v", err)
	}

	// Second wave: a couple of keys land in the still-open sub-batch after
	// the last internal commit and must disappear on Discard
	for i := wave1; i < wave1+3; i++ {
		if err := bulk.Set(bulkKey(i), bulkValue(i, "w2")); err != nil {
			t.Fatalf("bulk.Set(%d): %v", i, err)
		}
	}
	if db.cloningSequence != cloningSeqAfterWave1 {
		t.Fatalf("cloningSequence moved from %d to %d during the tail wave",
			cloningSeqAfterWave1, db.cloningSequence)
	}

	bulk.Discard()

	// The pre-bulk key and the early internally committed wave survive; the
	// tail of wave 1 may sit in the last open sub-batch, so for late keys
	// only the committed value or absence is acceptable — never a torn one
	value, err := db.Get([]byte("pre-bulk"))
	if err != nil || !bytes.Equal(value, []byte("pre-value")) {
		t.Fatalf("pre-bulk key after Discard: %v %q", err, value)
	}
	for i := 0; i < wave1; i++ {
		value, err := db.Get(bulkKey(i))
		if i < wave1/2 {
			// Rotations fired dozens of times before the halfway point, so
			// these keys are guaranteed to be internally committed
			if err != nil {
				t.Fatalf("Get(%d) committed wave after Discard: %v", i, err)
			}
			if !bytes.Equal(value, bulkValue(i, "w1")) {
				t.Fatalf("Get(%d) committed wave after Discard: wrong value", i)
			}
		} else if err == nil && !bytes.Equal(value, bulkValue(i, "w1")) {
			t.Fatalf("Get(%d) late wave after Discard: torn value", i)
		}
	}

	// The tail written after the last internal commit is gone
	for i := wave1; i < wave1+3; i++ {
		if _, err := db.Get(bulkKey(i)); err != ErrKeyNotFound {
			t.Fatalf("Get(%d) rolled-back tail: expected ErrKeyNotFound, got %v", i, err)
		}
	}

	// Reopen and verify the same boundary is durable
	db.Close()
	db2 := openTestDB(t, dbPath, WAL_NoSync, Options{"DirtyPageThreshold": 20})
	defer func() {
		db2.Close()
	}()
	for i := 0; i < wave1/2; i += 41 {
		if _, err := db2.Get(bulkKey(i)); err != nil {
			t.Fatalf("Get(%d) committed wave after reopen: %v", i, err)
		}
	}
	for i := wave1; i < wave1+3; i++ {
		if _, err := db2.Get(bulkKey(i)); err != ErrKeyNotFound {
			t.Fatalf("Get(%d) rolled-back tail after reopen: expected ErrKeyNotFound, got %v", i, err)
		}
	}
}

// TestBulkWithoutRotationDiscardsEverything verifies the small-bulk case: no
// internal commit ever fires, so Discard rolls the whole bulk back
func TestBulkWithoutRotationDiscardsEverything(t *testing.T) {
	withWriteModes(t, func(t *testing.T, writeMode string) {
		dbPath := testDBPath(".", "test_bulk_no_rotation.db", writeMode)
		cleanupTestFiles(dbPath)

		db := openTestDB(t, dbPath, writeMode)
		defer func() {
			db.Close()
			cleanupTestFiles(dbPath)
		}()

		if err := db.Set([]byte("keep"), []byte("keep-value")); err != nil {
			t.Fatalf("seed Set: %v", err)
		}

		bulk, err := db.NewBulk()
		if err != nil {
			t.Fatalf("NewBulk: %v", err)
		}
		for i := 0; i < 20; i++ {
			if err := bulk.Set(bulkKey(i), bulkValue(i, "v")); err != nil {
				t.Fatalf("bulk.Set(%d): %v", i, err)
			}
		}
		bulk.Discard()

		if _, err := db.Get([]byte("keep")); err != nil {
			t.Fatalf("pre-bulk key after Discard: %v", err)
		}
		for i := 0; i < 20; i++ {
			if _, err := db.Get(bulkKey(i)); err != ErrKeyNotFound {
				t.Fatalf("Get(%d) after Discard without rotation: expected ErrKeyNotFound, got %v", i, err)
			}
		}
	})
}

// TestBulkConcurrentReader runs reads against the bulk the whole time it is
// writing. Reads always succeed and see either the pre-bulk value or the
// last internally committed value, never a torn one and never
// ErrReadNotAllowed. After Flush every read must return the bulk value
func TestBulkConcurrentReader(t *testing.T) {
	withWriteModes(t, func(t *testing.T, writeMode string) {
		dbPath := testDBPath(".", "test_bulk_reader.db", writeMode)
		cleanupTestFiles(dbPath)

		db := openTestDB(t, dbPath, writeMode, Options{"DirtyPageThreshold": 20})
		defer func() {
			db.Close()
			cleanupTestFiles(dbPath)
		}()

		const numKeys = 500

		// Overwrite existing keys so a concurrent read always has a valid
		// old value to compare against
		for i := 0; i < numKeys; i++ {
			if err := db.Set(bulkKey(i), bulkValue(i, "old")); err != nil {
				t.Fatalf("seed Set(%d): %v", i, err)
			}
		}

		bulk, err := db.NewBulk()
		if err != nil {
			t.Fatalf("NewBulk: %v", err)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		readerFailed := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < numKeys; i += 7 {
					// Reads resolve the last internally committed state the
					// whole time the bulk is running
					value, err := db.Get(bulkKey(i))
					if err != nil {
						select {
						case readerFailed <- fmt.Errorf("concurrent Get(%d): %w", i, err):
						default:
						}
						return
					}
					if !bytes.Equal(value, bulkValue(i, "old")) && !bytes.Equal(value, bulkValue(i, "new")) {
						select {
						case readerFailed <- fmt.Errorf("concurrent Get(%d): torn value", i):
						default:
						}
						return
					}
				}
			}
		}()

		for i := 0; i < numKeys; i++ {
			if err := bulk.Set(bulkKey(i), bulkValue(i, "new")); err != nil {
				t.Fatalf("bulk.Set(%d): %v", i, err)
			}
		}

		if err := bulk.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		close(stop)

		select {
		case err := <-readerFailed:
			t.Fatal(err)
		default:
		}
		wg.Wait()
		select {
		case err := <-readerFailed:
			t.Fatal(err)
		default:
		}

		// After Flush every read must observe the bulk value
		for i := 0; i < numKeys; i += 13 {
			value, err := db.Get(bulkKey(i))
			if err != nil {
				t.Fatalf("Get(%d) after Flush: %v", i, err)
			}
			if !bytes.Equal(value, bulkValue(i, "new")) {
				t.Fatalf("Get(%d) after Flush: stale value", i)
			}
		}
	})
}

// TestBulkRestoresExplicitTransactionSemantics verifies that after a bulk
// ends (Flush or Discard) a normal explicit transaction behaves with its
// usual FastRollback isolation: db.Get must not see the in-flight transaction
// changes
func TestBulkRestoresExplicitTransactionSemantics(t *testing.T) {
	withWriteModes(t, func(t *testing.T, writeMode string) {
		dbPath := testDBPath(".", "test_bulk_semantics.db", writeMode)
		cleanupTestFiles(dbPath)

		db := openTestDB(t, dbPath, writeMode)
		defer func() {
			db.Close()
			cleanupTestFiles(dbPath)
		}()

		if err := db.Set([]byte("k"), []byte("committed")); err != nil {
			t.Fatalf("seed Set: %v", err)
		}

		// A short bulk that never trips the internal commit threshold
		bulk, err := db.NewBulk()
		if err != nil {
			t.Fatalf("NewBulk: %v", err)
		}
		if err := bulk.Set([]byte("k"), []byte("bulk")); err != nil {
			t.Fatalf("bulk.Set: %v", err)
		}
		if err := bulk.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}

		// The next explicit transaction must be isolated again
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin after bulk: %v", err)
		}
		if err := tx.Set([]byte("k"), []byte("tx")); err != nil {
			t.Fatalf("tx.Set: %v", err)
		}
		value, err := db.Get([]byte("k"))
		if err != nil {
			t.Fatalf("Get during explicit transaction: %v", err)
		}
		if !bytes.Equal(value, []byte("bulk")) {
			t.Fatalf("Get during explicit transaction: in-flight value leaked (got %q)", value)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("tx.Rollback: %v", err)
		}
	})
}
