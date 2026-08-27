package hashtabledb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentAccess(t *testing.T) {
	// Create a test database
	dbPath := "test_concurrent.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Insert some initial data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		value := fmt.Sprintf("init-value-%d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Number of concurrent operations
	numOps := 100
	// Wait group to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(numOps * 3) // For readers, writers, and deleters

	// Track errors
	var errMutex sync.Mutex
	var errors []string

	// Concurrent readers
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			// Read some keys
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("init-key-%d", j)
				value, err := db.Get([]byte(key))
				if err != nil {
					// "key not found" is acceptable due to concurrent deletion
					if err.Error() != "key not found" {
						errMutex.Lock()
						errors = append(errors, fmt.Sprintf("Reader %d failed to get key %s: %v", id, key, err))
						errMutex.Unlock()
					}
				} else {
					expectedPrefix := "init-value-"
					if !bytes.HasPrefix(value, []byte(expectedPrefix)) {
						errMutex.Lock()
						errors = append(errors, fmt.Sprintf("Reader %d got unexpected value for key %s: %s", id, key, string(value)))
						errMutex.Unlock()
					}
				}

				// Brief pause to allow interleaving with other operations
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Concurrent writers
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			// Write some keys
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("writer-%d-key-%d", id, j)
				value := fmt.Sprintf("writer-%d-value-%d", id, j)
				if err := db.Set([]byte(key), []byte(value)); err != nil {
					errMutex.Lock()
					errors = append(errors, fmt.Sprintf("Writer %d failed to set key %s: %v", id, key, err))
					errMutex.Unlock()
				}

				// Brief pause to allow interleaving with other operations
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Concurrent deleters
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			// Delete some keys (both existing and non-existing)
			for j := 0; j < 5; j++ {
				var key string
				if j % 2 == 0 && id % 5 == 0 {
					// Occasionally try to delete an initial key
					key = fmt.Sprintf("init-key-%d", (id+j) % 10)
				} else {
					// Try to delete a key that might have been written by a writer
					key = fmt.Sprintf("writer-%d-key-%d", (id+j) % numOps, j)
				}

				if err := db.Delete([]byte(key)); err != nil {
					errMutex.Lock()
					errors = append(errors, fmt.Sprintf("Deleter %d failed to delete key %s: %v", id, key, err))
					errMutex.Unlock()
				}

				// Brief pause to allow interleaving with other operations
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Check if there were any errors
	if len(errors) > 0 {
		for _, err := range errors {
			t.Errorf("%s", err)
		}
		t.Fatalf("Encountered %d errors during concurrent operations", len(errors))
	}

	// Verify the database is still functional
	// Try to read initial keys - some might have been deleted by concurrent deleters
	keysFound := 0
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		value, err := db.Get([]byte(key))
		if err != nil {
			if err.Error() != "key not found" {
				t.Errorf("Unexpected error reading initial key %s after concurrent operations: %v", key, err)
			}
			// "key not found" is expected for some keys due to concurrent deletion
			continue
		}
		keysFound++
		expectedValue := fmt.Sprintf("init-value-%d", i)
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("Value mismatch for initial key %s: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// We should find at least some initial keys (not all should be deleted)
	if keysFound == 0 {
		t.Errorf("All initial keys were deleted - this is unexpected")
	}

	// Try to write and read a new key
	testKey := "final-test-key"
	testValue := "final-test-value"
	if err := db.Set([]byte(testKey), []byte(testValue)); err != nil {
		t.Fatalf("Failed to set test key after concurrent operations: %v", err)
	}

	value, err := db.Get([]byte(testKey))
	if err != nil {
		t.Fatalf("Failed to get test key after concurrent operations: %v", err)
	}
	if !bytes.Equal(value, []byte(testValue)) {
		t.Fatalf("Value mismatch for test key after concurrent operations: got %s, want %s", string(value), testValue)
	}
}

func TestExclusiveAccess(t *testing.T) {
	// Create a test database
	dbPath := "test_exclusive.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database connection
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open first database connection: %v", err)
	}
	defer func() {
		db1.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Insert some initial data
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db1.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Try to open a second connection to the same database
	// This should fail because the database only allows one connection at a time
	db2, err := Open(dbPath)

	// The second connection should fail
	if err == nil {
		db2.Close() // Make sure to close it if it somehow succeeded
		t.Fatalf("Expected second database connection to fail, but it succeeded")
	}

	// Verify the first connection still works
	value, err := db1.Get([]byte("key-0"))
	if err != nil {
		t.Fatalf("Failed to read from first connection after attempting second connection: %v", err)
	}
	if !bytes.Equal(value, []byte("value-0")) {
		t.Fatalf("Value mismatch: got %s, want %s", string(value), "value-0")
	}
}

func TestReadOnlyMode(t *testing.T) {
	// Create a test database
	dbPath := "test_readonly.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Create and populate the database
	writeDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}

	// Insert some data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := writeDB.Set([]byte(key), []byte(value)); err != nil {
			writeDB.Close()
			t.Fatalf("Failed to set data: %v", err)
		}
	}

	// Close the write database
	if err := writeDB.Close(); err != nil {
		t.Fatalf("Failed to close write database: %v", err)
	}

	// Open the database in read-only mode
	readDB, err := Open(dbPath, Options{"ReadOnly": true})
	if err != nil {
		t.Fatalf("Failed to open database in read-only mode: %v", err)
	}
	defer func() {
		readDB.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Verify we can read data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value, err := readDB.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to read key %s in read-only mode: %v", key, err)
		}
		expectedValue := fmt.Sprintf("value-%d", i)
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s in read-only mode: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// Try to write data (should fail)
	err = readDB.Set([]byte("new-key"), []byte("new-value"))
	if err == nil {
		t.Fatalf("Expected error when writing in read-only mode, but got nil")
	}

	// Try to delete data (should fail)
	err = readDB.Delete([]byte("key-0"))
	if err == nil {
		t.Fatalf("Expected error when deleting in read-only mode, but got nil")
	}

	// Try to open a second read-only connection (should fail due to exclusive access)
	readDB2, err := Open(dbPath, Options{"ReadOnly": true})
	if err == nil {
		readDB2.Close()
		t.Fatalf("Expected second read-only connection to fail, but it succeeded")
	}
}

func TestTransactionWaitsForPreviousToFinish(t *testing.T) {
	dbPath := "test_txn_wait.db"
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Start first transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}

	// Channel to signal when second transaction starts
	secondStarted := make(chan struct{})
	secondAcquired := make(chan struct{})
	secondDone := make(chan struct{})

	// Start second transaction in another goroutine
	go func() {
		close(secondStarted)
		tx2, err := db.Begin()
		if err != nil {
			t.Errorf("Second transaction failed to begin: %v", err)
			return
		}
		close(secondAcquired)
		// Do something in tx2
		err = tx2.Set([]byte("key2"), []byte("val2"))
		if err != nil {
			t.Errorf("Second transaction failed to set: %v", err)
		}
		err = tx2.Commit()
		if err != nil {
			t.Errorf("Second transaction failed to commit: %v", err)
		}
		close(secondDone)
	}()

	// Wait for goroutine to start and attempt Begin
	<-secondStarted
	// Sleep briefly to ensure goroutine is blocked on Begin
	time.Sleep(100 * time.Millisecond)

	select {
	case <-secondAcquired:
		t.Fatalf("Second transaction acquired lock before first committed!")
	default:
		// Expected: second transaction is blocked
	}

	// Commit first transaction
	err = tx1.Set([]byte("key1"), []byte("val1"))
	if err != nil {
		t.Fatalf("First transaction failed to set: %v", err)
	}
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("First transaction failed to commit: %v", err)
	}

	// Now second transaction should proceed
	select {
	case <-secondAcquired:
		// Good: second transaction acquired lock after first committed
	case <-time.After(1 * time.Second):
		t.Fatalf("Second transaction did not acquire lock after first committed")
	}

	<-secondDone

	// Check both keys are present
	val, err := db.Get([]byte("key1"))
	if err != nil || string(val) != "val1" {
		t.Fatalf("key1 not found or value mismatch: %v, %s", err, val)
	}
	val, err = db.Get([]byte("key2"))
	if err != nil || string(val) != "val2" {
		t.Fatalf("key2 not found or value mismatch: %v, %s", err, val)
	}
}

// TestConcurrentReadersDuringWrite tests that multiple readers can proceed concurrently while a writer is active
func TestConcurrentReadersDuringWrite(t *testing.T) {
	dbPath := "test_concurrent_readers_writers.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Insert initial data
	numInitialKeys := 1000
	for i := 0; i < numInitialKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Channels for synchronization
	writerStarted := make(chan struct{})
	writerFinished := make(chan struct{})
	readersStarted := make(chan struct{}, 10)
	readersFinished := make(chan struct{}, 10)

	// Track read operations that happened during write
	var concurrentReads atomic.Int64
	var totalReads atomic.Int64
	var readErrors atomic.Int64

	// Start a long-running writer that will block for a significant time
	go func() {
		defer close(writerFinished)
		close(writerStarted)

		// Perform multiple write operations to ensure readers have time to run concurrently
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("writer-key-%d", i)
			value := fmt.Sprintf("writer-value-%d", i)
			if err := db.Set([]byte(key), []byte(value)); err != nil {
				t.Errorf("Writer failed to set key %s: %v", key, err)
				return
			}
			// Small delay to allow readers to interleave
			time.Sleep(time.Millisecond)
		}
	}()

	// Wait for writer to start
	<-writerStarted

	// Start multiple concurrent readers
	numReaders := 10
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()
			defer func() { readersFinished <- struct{}{} }()

			readersStarted <- struct{}{}

			// Each reader performs multiple read operations
			for j := 0; j < 50; j++ {
				// Read from initial data
				keyIndex := (readerID*50 + j) % numInitialKeys
				key := fmt.Sprintf("key-%d", keyIndex)

				totalReads.Add(1)

				value, err := db.Get([]byte(key))
				if err != nil {
					readErrors.Add(1)
					t.Errorf("Reader %d failed to get key %s: %v", readerID, key, err)
					continue
				}

				expectedValue := fmt.Sprintf("value-%d", keyIndex)
				if !bytes.Equal(value, []byte(expectedValue)) {
					t.Errorf("Reader %d got unexpected value for key %s: got %s, want %s",
						readerID, key, string(value), expectedValue)
					continue
				}

				// Check if writer is still running (concurrent read)
				select {
				case <-writerFinished:
					// Writer finished, this is not a concurrent read
				default:
					// Writer is still running, this is a concurrent read
					concurrentReads.Add(1)
				}

				// Small delay to allow interleaving
				time.Sleep(500 * time.Microsecond)
			}
		}(i)
	}

	// Wait for all readers to start
	for i := 0; i < numReaders; i++ {
		<-readersStarted
	}

	// Wait for all readers to finish
	wg.Wait()

	// Wait for writer to finish
	<-writerFinished

	// Verify that we had concurrent reads
	totalReadsCount := totalReads.Load()
	concurrentReadsCount := concurrentReads.Load()
	readErrorsCount := readErrors.Load()

	t.Logf("Total reads: %d, Concurrent reads: %d, Read errors: %d",
		totalReadsCount, concurrentReadsCount, readErrorsCount)

	if concurrentReadsCount == 0 {
		t.Errorf("No concurrent reads detected - readers may be blocked by writer")
	}

	if readErrorsCount > 0 {
		t.Errorf("Read errors occurred during concurrent access: %d", readErrorsCount)
	}

	// Verify that concurrent reads represent a significant portion of total reads
	if float64(concurrentReadsCount)/float64(totalReadsCount) < 0.3 {
		t.Errorf("Too few concurrent reads (%d/%d = %.2f%%) - concurrency may not be working properly",
			concurrentReadsCount, totalReadsCount, float64(concurrentReadsCount)/float64(totalReadsCount)*100)
	}
}

// TestReaderWriterThroughput tests that concurrent readers don't significantly impact writer throughput
func TestReaderWriterThroughput(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_reader_writer_throughput.db")

	// Helper function to clean up database files
	cleanupDB := func() {
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}

	// Helper function to initialize database with reader data
	initializeDB := func() (*DB, error) {
		// Clean up any existing test database
		cleanupDB()

		// Open a new database. One main-index page so keys collide and
		// convertHybridSubPageToTablePage actually runs.
		db, err := Open(dbPath, Options{"HashTableSize": 1})
		if err != nil {
			return nil, err
		}

		// Insert initial data for readers
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("read-key-%d", i)
			value := fmt.Sprintf("read-value-%d", i)
			if err := db.Set([]byte(key), []byte(value)); err != nil {
				db.Close()
				return nil, err
			}
		}

		return db, nil
	}

	// Initialize database once
	db, err := initializeDB()
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	db.Close()

	defer cleanupDB()

	// Run multiple iterations interleaved to get stable averages
	const numIterations = 5
	soloTimes := make([]time.Duration, 0, numIterations)
	concurrentTimes := make([]time.Duration, 0, numIterations)

	for iteration := 0; iteration < numIterations; iteration++ {
		// Reopen database for solo test
		db, err := Open(dbPath, Options{"HashTableSize": 1})
		if err != nil {
			t.Fatalf("Failed to reopen database for solo test iteration %d: %v", iteration, err)
		}

		// Test writer performance without concurrent readers
		start := time.Now()
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("write-key-solo-%d-%d", iteration, i)
			value := fmt.Sprintf("write-value-solo-%d-%d", iteration, i)
			if err := db.Set([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to write during solo test iteration %d: %v", iteration, err)
			}
		}
		soloWriteTime := time.Since(start)
		soloTimes = append(soloTimes, soloWriteTime)

		db.Close()

		// Reopen database for concurrent test
		db, err = Open(dbPath, Options{"HashTableSize": 1})
		if err != nil {
			t.Fatalf("Failed to reopen database for concurrent test iteration %d: %v", iteration, err)
		}

		// Test writer performance with concurrent readers
		var wg sync.WaitGroup
		stopReaders := make(chan struct{})
		startReading := make(chan struct{})
		readersReady := make(chan struct{}, 5) // Buffered channel for reader readiness

		// Start multiple concurrent readers
		numReaders := 5
		wg.Add(numReaders)
		for i := 0; i < numReaders; i++ {
			go func(readerID int) {
				defer wg.Done()
				readCount := 0

				// Signal that this reader is ready
				readersReady <- struct{}{}

				// Wait for start signal
				<-startReading

				for {
					select {
					case <-stopReaders:
						return
					default:
						// Read different keys in sequence to create varied load
						keyIndex := (readerID*1000 + readCount) % 10000
						key := fmt.Sprintf("read-key-%d", keyIndex)
						_, err := db.Get([]byte(key))
						if err != nil {
							// Ignore "key not found" errors
							if err.Error() != "key not found" {
								t.Errorf("Reader %d failed to get key %s: %v", readerID, key, err)
							}
						}
						readCount++
					}
				}
			}(i)
		}

		// Wait for all readers to be ready
		for i := 0; i < numReaders; i++ {
			<-readersReady
		}

		// Signal all readers to start reading
		close(startReading)

		// Test writer performance with concurrent readers
		start = time.Now()
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("write-key-concurrent-%d-%d", iteration, i)
			value := fmt.Sprintf("write-value-concurrent-%d-%d", iteration, i)
			if err := db.Set([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to write during concurrent test iteration %d: %v", iteration, err)
			}
		}
		concurrentWriteTime := time.Since(start)
		concurrentTimes = append(concurrentTimes, concurrentWriteTime)

		// Stop readers
		close(stopReaders)
		wg.Wait()

		db.Close()
	}

	// Calculate averages
	var soloSum, concurrentSum time.Duration
	for i := 0; i < numIterations; i++ {
		soloSum += soloTimes[i]
		concurrentSum += concurrentTimes[i]
	}
	avgSoloTime := soloSum / numIterations
	avgConcurrentTime := concurrentSum / numIterations

	t.Logf("Average solo write time: %v (individual: %v)", avgSoloTime, soloTimes)
	t.Logf("Average concurrent write time: %v (individual: %v)", avgConcurrentTime, concurrentTimes)

	// Writer performance shouldn't degrade significantly due to concurrent
	// readers, but the ratio is a noisy, machine-load-sensitive measurement,
	// so we only report it (and fail only on a pathological >10x slowdown).
	slowdownRatio := float64(avgConcurrentTime.Nanoseconds()) / float64(avgSoloTime.Nanoseconds())
	t.Logf("Writer slowdown with concurrent readers: %.1fx (%v vs %v)",
		slowdownRatio, avgConcurrentTime, avgSoloTime)
	if slowdownRatio > 10.0 {
		t.Errorf("Writer performance degraded pathologically with concurrent readers: %v vs %v (%.1fx slower)",
			avgConcurrentTime, avgSoloTime, slowdownRatio)
	}
}

// TestMultipleReadersSingleWriter ensures multiple readers can run truly concurrently
func TestMultipleReadersSingleWriter(t *testing.T) {
	dbPath := "test_multiple_readers.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Insert initial data
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Track concurrent executions
	var activeReaders atomic.Int32
	var maxConcurrentReaders atomic.Int32
	var readCompletions atomic.Int64

	// Start multiple readers that will run concurrently
	numReaders := 20
	var wg sync.WaitGroup
	wg.Add(numReaders)

	startSignal := make(chan struct{})

	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()

			// Wait for start signal to ensure all readers start simultaneously
			<-startSignal

			// Track active readers
			current := activeReaders.Add(1)

			// Update max concurrent readers
			for {
				max := maxConcurrentReaders.Load()
				if current <= max || maxConcurrentReaders.CompareAndSwap(max, current) {
					break
				}
			}

			// Simulate some work to ensure readers overlap
			time.Sleep(10 * time.Millisecond)

			// Perform multiple reads
			for j := 0; j < 10; j++ {
				keyIndex := (readerID + j) % 50
				key := fmt.Sprintf("key-%d", keyIndex)

				_, err := db.Get([]byte(key))
				if err != nil {
					t.Errorf("Reader %d failed to get key %s: %v", readerID, key, err)
				}

				// Brief pause between reads
				time.Sleep(time.Millisecond)
			}

			activeReaders.Add(-1)
			readCompletions.Add(1)
		}(i)
	}

	// Start all readers simultaneously
	close(startSignal)

	// Wait for all readers to complete
	wg.Wait()

	maxConcurrent := maxConcurrentReaders.Load()
	completions := readCompletions.Load()

	t.Logf("Max concurrent readers: %d, Total completions: %d", maxConcurrent, completions)

	// Verify that multiple readers ran concurrently
	if maxConcurrent < int32(numReaders/2) {
		t.Errorf("Expected at least %d concurrent readers, but max was %d", numReaders/2, maxConcurrent)
	}

	if completions != int64(numReaders) {
		t.Errorf("Expected %d reader completions, got %d", numReaders, completions)
	}
}

// TestWriterBlocksOtherWriters ensures writers are still serialized
func TestWriterBlocksOtherWriters(t *testing.T) {
	dbPath := "test_writer_serialization.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	var writerOrder []int
	var orderMutex sync.Mutex

	numWriters := 5
	var wg sync.WaitGroup
	wg.Add(numWriters)

	startSignal := make(chan struct{})

	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()

			// Wait for start signal
			<-startSignal

			// Each writer performs a slow write operation
			key := fmt.Sprintf("writer-%d-key", writerID)
			value := fmt.Sprintf("writer-%d-value", writerID)

			// Record when this writer starts its operation
			orderMutex.Lock()
			writerOrder = append(writerOrder, writerID)
			orderMutex.Unlock()

			if err := db.Set([]byte(key), []byte(value)); err != nil {
				t.Errorf("Writer %d failed to set key: %v", writerID, err)
			}

			// Simulate some additional work
			time.Sleep(5 * time.Millisecond)
		}(i)
	}

	// Start all writers simultaneously
	close(startSignal)

	// Wait for all writers to complete
	wg.Wait()

	// Verify all writers completed
	if len(writerOrder) != numWriters {
		t.Errorf("Expected %d writers to execute, got %d", numWriters, len(writerOrder))
	}

	// Verify all keys were written correctly
	for i := 0; i < numWriters; i++ {
		key := fmt.Sprintf("writer-%d-key", i)
		expectedValue := fmt.Sprintf("writer-%d-value", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to read key written by writer %d: %v", i, err)
			continue
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("Incorrect value for writer %d: got %s, want %s", i, string(value), expectedValue)
		}
	}

	t.Logf("Writer execution order: %v", writerOrder)
}

// TestCloseWithBlockedTransactions tests that calling Close() while transactions
// are blocked waiting properly wakes them up and returns appropriate errors
func TestCloseWithBlockedTransactions(t *testing.T) {
	dbPath := "test_close_blocked_txn.db"
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Start first transaction that we'll keep open
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}

	// Channels to coordinate the test
	numBlockedTxns := 3
	txnStarted := make(chan int, numBlockedTxns)
	txnResults := make(chan error, numBlockedTxns)
	allTxnsStarted := make(chan struct{})

	// Start multiple goroutines that will try to begin transactions
	// These should all block waiting for tx1 to complete
	for i := 0; i < numBlockedTxns; i++ {
		go func(txnID int) {
			txnStarted <- txnID

			// This Begin() call should block until tx1 completes or DB is closed
			tx, err := db.Begin()

			if err != nil {
				// Expected: database is closed error
				txnResults <- err
				return
			}

			// If we get here, the transaction should fail because DB is closed
			defer func() {
				if tx != nil {
					tx.Rollback() // Clean up if needed
				}
			}()

			// Try to do something with the transaction
			err = tx.Set([]byte(fmt.Sprintf("key%d", txnID)), []byte(fmt.Sprintf("val%d", txnID)))
			if err != nil {
				txnResults <- err
				return
			}

			err = tx.Commit()
			txnResults <- err
		}(i)
	}

	// Wait for all goroutines to start and attempt Begin()
	for i := 0; i < numBlockedTxns; i++ {
		txnID := <-txnStarted
		t.Logf("Transaction %d started and should be blocking", txnID)
	}
	close(allTxnsStarted)

	// Give goroutines time to actually block on the condition variable
	time.Sleep(200 * time.Millisecond)

	// Verify none of the blocked transactions have completed yet
	select {
	case result := <-txnResults:
		t.Fatalf("A blocked transaction completed unexpectedly with result: %v", result)
	default:
		// Expected: all transactions are still blocked
		t.Log("Confirmed: all transactions are properly blocked")
	}

	// Now close the database while transactions are blocked
	t.Log("Closing database while transactions are blocked...")
	closeErr := db.Close()
	if closeErr != nil {
		t.Errorf("Database close failed: %v", closeErr)
	}

	// Collect results from all blocked transactions
	var results []error
	for i := 0; i < numBlockedTxns; i++ {
		select {
		case result := <-txnResults:
			results = append(results, result)
			t.Logf("Transaction %d result: %v", i, result)
		case <-time.After(2 * time.Second):
			t.Fatalf("Transaction %d did not complete after database close", i)
		}
	}

	// Verify all transactions received appropriate errors
	for i, result := range results {
		if result == nil {
			t.Errorf("Transaction %d should have failed but succeeded", i)
		} else if !strings.Contains(result.Error(), "closed") {
			t.Errorf("Transaction %d got unexpected error (should mention 'closed'): %v", i, result)
		}
	}

	// Clean up the first transaction (should be safe to rollback after close)
	rollbackErr := tx1.Rollback()
	if rollbackErr != nil {
		t.Logf("Expected rollback error after close: %v", rollbackErr)
	}

	t.Logf("Test completed successfully: %d blocked transactions properly handled during close", numBlockedTxns)
}

// TestConcurrentWritersSerialization tests that concurrent Set() operations are properly
// serialized by the write mutex, ensuring data integrity under concurrent access
func TestConcurrentWritersSerialization(t *testing.T) {
	dbPath := "test_concurrent_writers_serialization.db"
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Test multiple concurrent writers to ensure proper serialization
	numWriters := 10
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Channel to start all writers simultaneously
	startSignal := make(chan struct{})

	// Error collection
	var errors []error
	var errorMutex sync.Mutex

	// Start multiple writers concurrently
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()

			// Wait for start signal to maximize concurrency
			<-startSignal

			// Create unique key and value for this writer
			key := fmt.Sprintf("writer-%d", writerID)
			value := fmt.Sprintf("value-from-writer-%d", writerID)

			// Perform the write operation
			err := db.Set([]byte(key), []byte(value))
			if err != nil {
				errorMutex.Lock()
				errors = append(errors, fmt.Errorf("writer %d failed: %v", writerID, err))
				errorMutex.Unlock()
			}
		}(i)
	}

	// Start all writers simultaneously
	close(startSignal)

	// Wait for all writers to complete
	wg.Wait()

	// Check for any errors during concurrent writes
	if len(errors) > 0 {
		for _, err := range errors {
			t.Errorf("Concurrent write error: %v", err)
		}
		t.Fatalf("Some writers failed - write mutex may not be working correctly")
	}

	// Verify that all data was written correctly
	for i := 0; i < numWriters; i++ {
		key := fmt.Sprintf("writer-%d", i)
		expectedValue := fmt.Sprintf("value-from-writer-%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to read key written by writer %d: %v", i, err)
			continue
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("Data corruption detected: writer %d key has incorrect value", i)
			t.Errorf("  Expected: %s", expectedValue)
			t.Errorf("  Got: %s", string(value))
			t.Errorf("This indicates the write mutex is not properly serializing operations")
		}
	}

	t.Logf("SUCCESS: All %d concurrent writers completed successfully with correct data", numWriters)
	t.Log("This demonstrates that the write mutex properly serializes concurrent Set() operations")
}

// TestTransactionBlocksOtherWriters tests that a long-running transaction with many writes
// properly blocks other writers until the transaction commits
func TestTransactionBlocksOtherWriters(t *testing.T) {
	dbPath := "test_transaction_blocks_writers.db"
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Channels to track when Begin() calls return
	firstBeginReturned := make(chan struct{})
	secondBeginReturned := make(chan struct{})
	thirdBeginReturned := make(chan struct{})
	firstCommitDone := make(chan struct{})
	secondCommitDone := make(chan struct{})
	thirdCommitDone := make(chan struct{})

	// Start first transaction
	go func() {
		t.Log("First transaction: Calling db.Begin()...")
		tx1, err := db.Begin()
		if err != nil {
			t.Errorf("First transaction Begin() failed: %v", err)
			return
		}

		t.Log("First transaction: db.Begin() returned successfully")
		close(firstBeginReturned)

		// Perform many writes to make this transaction run longer
		numWrites := 1000
		for i := 0; i < numWrites; i++ {
			key := fmt.Sprintf("txn1-key-%d", i)
			value := fmt.Sprintf("txn1-value-%d", i)
			err := tx1.Set([]byte(key), []byte(value))
			if err != nil {
				t.Errorf("First transaction write %d failed: %v", i, err)
				tx1.Rollback()
				return
			}
		}

		t.Log("First transaction: Calling Commit()...")
		err = tx1.Commit()
		if err != nil {
			t.Errorf("First transaction Commit() failed: %v", err)
			return
		}

		t.Log("First transaction: Commit() completed")
		close(firstCommitDone)
	}()

	// Start second transaction (should be blocked)
	go func() {
		// Wait for first transaction to acquire the lock
		<-firstBeginReturned

		t.Log("Second transaction: Calling db.Begin() (should block)...")
		tx2, err := db.Begin()
		if err != nil {
			t.Errorf("Second transaction Begin() failed: %v", err)
			return
		}

		t.Log("Second transaction: db.Begin() returned (was unblocked)")
		close(secondBeginReturned)

		// Perform many writes like the first transaction
		numWrites := 1000
		for i := 0; i < numWrites; i++ {
			key := fmt.Sprintf("txn2-key-%d", i)
			value := fmt.Sprintf("txn2-value-%d", i)
			err := tx2.Set([]byte(key), []byte(value))
			if err != nil {
				t.Errorf("Second transaction write %d failed: %v", i, err)
				tx2.Rollback()
				return
			}
		}

		err = tx2.Commit()
		if err != nil {
			t.Errorf("Second transaction Commit() failed: %v", err)
		}
		close(secondCommitDone)
	}()

	// Start third transaction (should also be blocked initially)
	go func() {
		// Wait for first transaction to acquire the lock
		<-firstBeginReturned

		t.Log("Third transaction: Calling db.Begin() (should block)...")
		tx3, err := db.Begin()
		if err != nil {
			t.Errorf("Third transaction Begin() failed: %v", err)
			return
		}

		t.Log("Third transaction: db.Begin() returned (was unblocked)")
		close(thirdBeginReturned)

		// Perform many writes like the other transactions
		numWrites := 1000
		for i := 0; i < numWrites; i++ {
			key := fmt.Sprintf("txn3-key-%d", i)
			value := fmt.Sprintf("txn3-value-%d", i)
			err := tx3.Set([]byte(key), []byte(value))
			if err != nil {
				t.Errorf("Third transaction write %d failed: %v", i, err)
				tx3.Rollback()
				return
			}
		}

		err = tx3.Commit()
		if err != nil {
			t.Errorf("Third transaction Commit() failed: %v", err)
		}
		close(thirdCommitDone)
	}()

	// Test Check 1: After first Begin() returns, second should still be blocked
	<-firstBeginReturned
	t.Log("CHECK 1: First transaction has acquired lock")

	// Immediately check if second and third transactions are blocked (no sleep needed)
	select {
	case <-secondBeginReturned:
		t.Fatal("FAIL: Second transaction Begin() returned immediately - not blocked!")
	default:
		t.Log("PASS: Second transaction is properly blocked")
	}

	select {
	case <-thirdBeginReturned:
		t.Fatal("FAIL: Third transaction Begin() returned immediately - not blocked!")
	default:
		t.Log("PASS: Third transaction is properly blocked")
	}

	// Test Check 2: After first Commit(), second Begin() should be released
	<-firstCommitDone
	t.Log("CHECK 2: First transaction has committed")

	select {
	case <-secondBeginReturned:
		t.Log("PASS: Second transaction Begin() was released after first transaction committed")
	case <-time.After(2 * time.Second):
		t.Fatal("FAIL: Second transaction Begin() was not released within 2 seconds after first commit")
	}

	select {
	case <-thirdBeginReturned:
		t.Log("PASS: Third transaction Begin() was released")
	case <-time.After(2 * time.Second):
		t.Fatal("FAIL: Third transaction Begin() was not released within 2 seconds")
	}

	// Wait for all transactions to complete before checking data
	select {
	case <-secondCommitDone:
		t.Log("Second transaction has completed")
	case <-time.After(5 * time.Second):
		t.Fatal("FAIL: Second transaction did not complete within 5 seconds")
	}

	select {
	case <-thirdCommitDone:
		t.Log("Third transaction has completed")
	case <-time.After(5 * time.Second):
		t.Fatal("FAIL: Third transaction did not complete within 5 seconds")
	}

	// Verify data integrity - check all keys written by first transaction
	numWrites := 1000
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("txn1-key-%d", i)
		expectedValue := fmt.Sprintf("txn1-value-%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to read first transaction data for key %s: %v", key, err)
		} else if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("First transaction data corrupted for key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}

	// Verify data integrity - check all keys written by second transaction
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("txn2-key-%d", i)
		expectedValue := fmt.Sprintf("txn2-value-%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to read second transaction data for key %s: %v", key, err)
		} else if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("Second transaction data corrupted for key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}

	// Verify data integrity - check all keys written by third transaction
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("txn3-key-%d", i)
		expectedValue := fmt.Sprintf("txn3-value-%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to read third transaction data for key %s: %v", key, err)
		} else if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("Third transaction data corrupted for key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}

	t.Log("SUCCESS: Transaction blocking behavior works correctly!")
}

// TestCallerSyncConcurrentReadersDuringWrite reproduces a page-version race
// under CallerThread_* write modes: concurrent Get() while another goroutine
// commits new keys used to fail with "sub-page with index N not found"
//
// Root cause: Get() registers maxReadSequence, then walks parent→child. CallerThread
// flushIndexToDisk after Commit called writeIndexPage → breakPageChain, dropping
// older versions. Mid-Get the child snapshot vanished; getPage loaded a newer
// on-disk page whose SubPages no longer matched the parent pointer
//
// Fix: readerSequences under seqMutex; flush/cleaner use oldestReaderSequence as
// the reclaim watermark
func TestCallerSyncConcurrentReadersDuringWrite(t *testing.T) {
	const (
		numSeedKeys = 5000
		keySize     = 33
		valueSize   = 750
		itemsPerTx  = 50
		numWriteTx  = 100
		numReaders  = 4
	)

	modes := []string{
		CallerThread_WAL_Sync,
		CallerThread_WAL_NoSync,
		WorkerThread_WAL,
	}

	for _, mode := range modes {
		mode := mode
		t.Run(writeModeName(mode), func(t *testing.T) {
			dbPath := testDBPath(".", "sync_concurrent_rw.db", mode)
			cleanupTestFiles(dbPath)
			defer cleanupTestFiles(dbPath)

			db := openTestDB(t, dbPath, mode)
			defer db.Close()

			seedKeys := make([][]byte, numSeedKeys)
			for i := 0; i < numSeedKeys; i++ {
				seedKeys[i] = generateDeterministicBytes(i, keySize)
				val := generateDeterministicBytes(i+7777, valueSize)
				if err := db.Set(seedKeys[i], val); err != nil {
					t.Fatalf("seed Set(%d): %v", i, err)
				}
			}

			var (
				stop       = make(chan struct{})
				startCh    = make(chan struct{})
				ready      = make(chan struct{}, numReaders)
				wg         sync.WaitGroup
				subPageErr atomic.Value
				readOps    atomic.Int64
			)

			wg.Add(numReaders)
			for r := 0; r < numReaders; r++ {
				go func(readerID int) {
					defer wg.Done()
					ready <- struct{}{}
					<-startCh
					n := 0
					for {
						select {
						case <-stop:
							return
						default:
							key := seedKeys[(readerID*1000+n)%numSeedKeys]
							_, err := db.Get(key)
							readOps.Add(1)
							if err != nil && strings.Contains(err.Error(), "sub-page with index") {
								subPageErr.Store(err.Error())
								return
							}
							n++
						}
					}
				}(r)
			}
			for i := 0; i < numReaders; i++ {
				<-ready
			}
			close(startCh)

			writeBase := numSeedKeys
			for tx := 0; tx < numWriteTx; tx++ {
				if v := subPageErr.Load(); v != nil {
					break
				}
				txn, err := db.Begin()
				if err != nil {
					t.Fatalf("Begin: %v", err)
				}
				for i := 0; i < itemsPerTx; i++ {
					idx := writeBase + tx*itemsPerTx + i
					key := generateDeterministicBytes(idx, keySize)
					val := generateDeterministicBytes(idx+7777, valueSize)
					if err := txn.Set(key, val); err != nil {
						_ = txn.Rollback()
						t.Fatalf("tx Set(%d): %v", idx, err)
					}
				}
				if err := txn.Commit(); err != nil {
					t.Fatalf("Commit: %v", err)
				}
			}

			close(stop)
			wg.Wait()

			if v := subPageErr.Load(); v != nil {
				t.Fatalf("concurrent Get failed after %d reads: %v", readOps.Load(), v)
			}
			t.Logf("ok: %d concurrent reads during %d write txns", readOps.Load(), numWriteTx)
		})
	}
}

// TestCallerSyncConcurrentReadersStress is a tighter loop aimed at the same race
func TestCallerSyncConcurrentReadersStress(t *testing.T) {
	const (
		numSeedKeys = 3000
		keySize     = 33
		valueSize   = 750
		itemsPerTx  = 25
		numWriteTx  = 200
		numReaders  = 8
	)

	dbPath := testDBPath(".", "sync_concurrent_stress.db", CallerThread_WAL_Sync)
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, CallerThread_WAL_Sync)
	defer db.Close()

	keys := make([][]byte, numSeedKeys)
	for i := 0; i < numSeedKeys; i++ {
		keys[i] = generateDeterministicBytes(i, keySize)
		if err := db.Set(keys[i], generateDeterministicBytes(i+1, valueSize)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var (
		stop    = make(chan struct{})
		startCh = make(chan struct{})
		wg      sync.WaitGroup
		failed  atomic.Value
	)

	wg.Add(numReaders)
	for r := 0; r < numReaders; r++ {
		go func(id int) {
			defer wg.Done()
			<-startCh
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					_, err := db.Get(keys[(id*997+i)%numSeedKeys])
					if err != nil && strings.Contains(err.Error(), "sub-page") {
						failed.Store(err.Error())
						return
					}
				}
			}
		}(r)
	}
	close(startCh)

	for tx := 0; tx < numWriteTx; tx++ {
		if failed.Load() != nil {
			break
		}
		txn, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		for i := 0; i < itemsPerTx; i++ {
			idx := numSeedKeys + tx*itemsPerTx + i
			_ = txn.Set(generateDeterministicBytes(idx, keySize), generateDeterministicBytes(idx+9, valueSize))
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	close(stop)
	wg.Wait()

	if v := failed.Load(); v != nil {
		t.Fatalf("sub-page race: %v", v)
	}
}

// TestCallerSyncConcurrentIteratorsDuringWrite is the iterator counterpart of
// TestCallerSyncConcurrentReadersDuringWrite: a long-lived NewIterator snapshot
// must stay registered so flush cannot reclaim page versions mid-walk
func TestCallerSyncConcurrentIteratorsDuringWrite(t *testing.T) {
	const (
		numSeedKeys = 5000
		keySize     = 33
		valueSize   = 750
		itemsPerTx  = 50
		numWriteTx  = 100
		numIters    = 4
	)

	modes := []string{
		CallerThread_WAL_Sync,
		CallerThread_WAL_NoSync,
		WorkerThread_WAL,
	}

	for _, mode := range modes {
		mode := mode
		t.Run(writeModeName(mode), func(t *testing.T) {
			dbPath := testDBPath(".", "sync_concurrent_iter.db", mode)
			cleanupTestFiles(dbPath)
			defer cleanupTestFiles(dbPath)

			db := openTestDB(t, dbPath, mode)
			defer db.Close()

			for i := 0; i < numSeedKeys; i++ {
				key := generateDeterministicBytes(i, keySize)
				val := generateDeterministicBytes(i+7777, valueSize)
				if err := db.Set(key, val); err != nil {
					t.Fatalf("seed Set(%d): %v", i, err)
				}
			}

			var (
				stop      = make(chan struct{})
				startCh   = make(chan struct{})
				ready     = make(chan struct{}, numIters)
				wg        sync.WaitGroup
				iterErr   atomic.Value
				iterPasses atomic.Int64
			)

			wg.Add(numIters)
			for r := 0; r < numIters; r++ {
				go func() {
					defer wg.Done()
					ready <- struct{}{}
					<-startCh
					for {
						select {
						case <-stop:
							return
						default:
							it := db.NewIterator()
							count := 0
							for ; it.Valid(); it.Next() {
								count++
								if it.Key() == nil {
									iterErr.Store("iterator returned nil key while Valid")
									it.Close()
									return
								}
							}
							it.Close()
							if count < numSeedKeys {
								iterErr.Store(fmt.Sprintf("iterator saw %d keys, want >= %d", count, numSeedKeys))
								return
							}
							iterPasses.Add(1)
						}
					}
				}()
			}
			for i := 0; i < numIters; i++ {
				<-ready
			}
			close(startCh)

			writeBase := numSeedKeys
			for tx := 0; tx < numWriteTx; tx++ {
				if v := iterErr.Load(); v != nil {
					break
				}
				txn, err := db.Begin()
				if err != nil {
					t.Fatalf("Begin: %v", err)
				}
				for i := 0; i < itemsPerTx; i++ {
					idx := writeBase + tx*itemsPerTx + i
					key := generateDeterministicBytes(idx, keySize)
					val := generateDeterministicBytes(idx+7777, valueSize)
					if err := txn.Set(key, val); err != nil {
						_ = txn.Rollback()
						t.Fatalf("tx Set(%d): %v", idx, err)
					}
				}
				if err := txn.Commit(); err != nil {
					t.Fatalf("Commit: %v", err)
				}
			}

			close(stop)
			wg.Wait()

			if v := iterErr.Load(); v != nil {
				t.Fatalf("concurrent iterator failed after %d full scans: %v", iterPasses.Load(), v)
			}
			t.Logf("ok: %d full iterator scans during %d write txns", iterPasses.Load(), numWriteTx)
		})
	}
}
// ---------------------------------------------------------------------------
// WAL checkpoint / durable index integrity under concurrent readers
// ---------------------------------------------------------------------------

// walkDurableIndex walks the index from the main hash table and reports every
// pointer that references a sub-page id which is not live on the target hybrid
// page. Such a pointer makes Get fail with "sub-page with index N not found".
func walkDurableIndex(t *testing.T, db *DB) (dangling int, reports []string) {
	t.Helper()

	var tablePages, hybridSubs, dataEntries, loadErrors int
	visited := make(map[uint64]bool)

	addReport := func(format string, args ...interface{}) {
		if len(reports) < 25 {
			reports = append(reports, fmt.Sprintf(format, args...))
		}
	}

	describe := func(p *HybridPage) string {
		live := liveHybridSubPageIDs(p)
		maxLive := -1
		if len(live) > 0 {
			maxLive = live[len(live)-1]
		}
		return fmt.Sprintf("numSub=%d contentSize=%d liveCount=%d maxLive=%d", p.NumSubPages, p.ContentSize, len(live), maxLive)
	}

	var walkTable func(pn uint32, from string, depth int)
	var walkHybrid func(pn uint32, sub uint8, from string, depth int)

	checkChild := func(childPage uint32, childSub uint8, from string, depth int) {
		page, err := db.getPage(childPage)
		if err != nil {
			loadErrors++
			addReport("LOAD ERROR %s -> page %d: %v", from, childPage, err)
			return
		}
		switch page.pageType {
		case ContentTypeTable:
			walkTable(childPage, from, depth+1)
		case ContentTypeHybrid:
			if int(childSub) >= len(page.SubPages) || !hybridSubPageLive(page.SubPages[childSub]) {
				dangling++
				addReport("DANGLING %s -> hybrid page %d sub %d (%s)", from, childPage, childSub, describe(page))
				return
			}
			walkHybrid(childPage, childSub, from, depth+1)
		default:
			addReport("BAD TYPE %s -> page %d type %c", from, childPage, page.pageType)
		}
	}

	walkTable = func(pn uint32, from string, depth int) {
		if depth > 64 {
			return
		}
		key := uint64(pn) << 9
		if visited[key] {
			return
		}
		visited[key] = true
		tp, err := db.getTablePage(pn)
		if err != nil {
			loadErrors++
			addReport("LOAD ERROR table %d from %s: %v", pn, from, err)
			return
		}
		tablePages++
		for slot := 0; slot < TableEntries; slot++ {
			childPage, childSub, dataOffset := db.getTableEntry(tp, slot)
			if childPage == 0 && dataOffset == 0 {
				continue
			}
			if dataOffset != 0 {
				dataEntries++
				continue
			}
			checkChild(childPage, childSub, fmt.Sprintf("table %d slot %d", pn, slot), depth)
		}
	}

	walkHybrid = func(pn uint32, sub uint8, from string, depth int) {
		if depth > 64 {
			return
		}
		key := uint64(pn)<<9 | uint64(sub) + 1
		if visited[key] {
			return
		}
		visited[key] = true
		hp, err := db.getHybridPage(pn)
		if err != nil {
			loadErrors++
			addReport("LOAD ERROR hybrid %d from %s: %v", pn, from, err)
			return
		}
		hybridSubs++
		type ptr struct {
			page uint32
			sub  uint8
			slot int
		}
		var children []ptr
		err = db.iterateHybridSubPageEntries(hp, sub, func(_ int, _ int, slot int, isSubPage bool, value uint64, dataSize uint16) bool {
			if isSubPage {
				children = append(children, ptr{page: uint32((value >> 8) & 0xFFFFFFFF), sub: uint8(value & 0xFF), slot: slot})
			} else {
				dataEntries++
			}
			return true
		})
		if err != nil {
			loadErrors++
			addReport("ITERATE ERROR hybrid %d sub %d from %s: %v", pn, sub, from, err)
			return
		}
		for _, c := range children {
			checkChild(c.page, c.sub, fmt.Sprintf("hybrid page %d sub %d slot %d", pn, sub, c.slot), depth)
		}
	}

	for pn := 1; pn <= db.mainIndexPages; pn++ {
		walkTable(uint32(pn), "main", 0)
	}

	t.Logf("index walk: tablePages=%d hybridSubPages=%d dataEntries=%d dangling=%d loadErrors=%d",
		tablePages, hybridSubs, dataEntries, dangling, loadErrors)
	return dangling, reports
}

// TestIntegrityWalk checks an existing database directory for dangling
// sub-page pointers. Set HT_CHECK_DIR to a database directory to run it.
func TestIntegrityWalk(t *testing.T) {
	dir := os.Getenv("HT_CHECK_DIR")
	if dir == "" {
		t.Skip("HT_CHECK_DIR not set")
	}
	db, err := Open(filepath.Join(dir, "data.db"), Options{
		"ReadOnly":             true,
		"CacheSizeThreshold":   65536,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	dangling, reports := walkDurableIndex(t, db)
	for _, r := range reports {
		t.Log(r)
	}
	if dangling > 0 {
		t.Fatalf("durable index has %d dangling sub-page pointers", dangling)
	}
}

func stressKey(i int) []byte {
	k := make([]byte, 16)
	binary.BigEndian.PutUint64(k, uint64(i)*0x9E3779B97F4A7C15)
	binary.BigEndian.PutUint64(k[8:], uint64(i))
	return k
}

func stressValue(i int, size int) []byte {
	v := make([]byte, size)
	binary.BigEndian.PutUint64(v, uint64(i))
	for j := 8; j < size; j++ {
		v[j] = byte(i + j)
	}
	return v
}

// TestConcurrentReadersDuringWrites runs a writer committing small transactions
// while reader goroutines call Get on the DB handle, with a tiny page cache to
// force constant eviction and page version churn. It then reopens the database
// and walks the index to check that nothing was lost durably.
//
// Regression test for a checkpoint bug: copyWALPagesToIndexFile cleared isWAL
// only on the newest WAL version of each page, so an older version retained as
// a reader snapshot kept the flag and the next checkpoint copied it over the
// newer image, reverting pages on disk while parents pointed at newer contents.
func TestConcurrentReadersDuringWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running stress test")
	}

	dir := os.Getenv("HT_STRESS_DIR")
	if dir == "" {
		dir = t.TempDir()
	} else {
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0755)
	}

	preload := 200_000
	if v := os.Getenv("HT_STRESS_PRELOAD"); v != "" {
		fmt.Sscanf(v, "%d", &preload)
	}
	seconds := 30
	if v := os.Getenv("HT_STRESS_SECONDS"); v != "" {
		fmt.Sscanf(v, "%d", &seconds)
	}
	readers := 3
	if v := os.Getenv("HT_STRESS_READERS"); v != "" {
		fmt.Sscanf(v, "%d", &readers)
	}
	cachePages := 2048
	if v := os.Getenv("HT_STRESS_CACHE"); v != "" {
		fmt.Sscanf(v, "%d", &cachePages)
	}
	writeDelayUs := 0
	if v := os.Getenv("HT_STRESS_WRITE_DELAY_US"); v != "" {
		fmt.Sscanf(v, "%d", &writeDelayUs)
	}

	db, err := Open(filepath.Join(dir, "data.db"), Options{
		"WriteMode":            WorkerThread_WAL,
		"HashTableSize":        32 * 1024,
		"CacheSizeThreshold":   cachePages,
		"CheckpointThreshold":  int64(16 << 20),
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			db.Close()
		}
	}()

	valSize := 200
	t.Logf("preloading %d keys", preload)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < preload; i++ {
		if err := tx.Set(stressKey(i), stressValue(i, valSize)); err != nil {
			t.Fatalf("preload set %d: %v", i, err)
		}
		if (i+1)%50_000 == 0 {
			if err := tx.Commit(); err != nil {
				t.Fatalf("preload commit %d: %v", i, err)
			}
			if tx, err = db.Begin(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("final preload commit: %v", err)
	}
	t.Logf("preload done")

	var (
		stop      atomic.Bool
		firstErr  atomic.Value
		reads     atomic.Int64
		writes    atomic.Int64
		wg        sync.WaitGroup
	)
	fail := func(where string, err error) {
		if err == nil {
			return
		}
		firstErr.CompareAndSwap(nil, fmt.Errorf("%s: %w", where, err))
		stop.Store(true)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			x := uint64(seed*7919 + 13)
			for !stop.Load() {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				i := int(x % uint64(preload))
				v, err := db.Get(stressKey(i))
				if err != nil {
					fail(fmt.Sprintf("reader Get(%d)", i), err)
					return
				}
				if len(v) < 8 || binary.BigEndian.Uint64(v) != uint64(i) {
					fail(fmt.Sprintf("reader Get(%d)", i), fmt.Errorf("wrong value (len=%d)", len(v)))
					return
				}
				reads.Add(1)
			}
		}(r)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		next := preload
		for !stop.Load() {
			tx, err := db.Begin()
			if err != nil {
				fail("writer Begin", err)
				return
			}
			for j := 0; j < 10; j++ {
				if err := tx.Set(stressKey(next), stressValue(next, valSize)); err != nil {
					fail(fmt.Sprintf("writer Set(%d)", next), err)
					tx.Rollback()
					return
				}
				next++
			}
			if err := tx.Commit(); err != nil {
				fail("writer Commit", err)
				return
			}
			writes.Add(10)
			if writeDelayUs > 0 {
				time.Sleep(time.Duration(writeDelayUs) * time.Microsecond)
			}
		}
	}()

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for time.Now().Before(deadline) && !stop.Load() {
		time.Sleep(200 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()

	t.Logf("reads=%d writes=%d", reads.Load(), writes.Load())
	if e := firstErr.Load(); e != nil {
		t.Fatalf("%v", e.(error))
	}

	// Reopen and verify that no index pointer was left dangling on disk
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	closed = true
	reopened, err := Open(filepath.Join(dir, "data.db"), Options{
		"ReadOnly":             true,
		"CacheSizeThreshold":   65536,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	dangling, reports := walkDurableIndex(t, reopened)
	for _, r := range reports {
		t.Log(r)
	}
	if dangling > 0 {
		t.Fatalf("durable index has %d dangling sub-page pointers after concurrent reads", dangling)
	}
}
