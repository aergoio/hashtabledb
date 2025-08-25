package hashtabledb

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestMultiProcessAccess tests database access from multiple processes
// This test verifies that only one process can access the database at a time
func TestMultiProcessAccess(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping multi-process test in short mode")
	}

	// Create a temporary directory for our test
	tempDir, err := os.MkdirTemp("", "multiprocess_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Path to the database file
	dbPath := filepath.Join(tempDir, "multiprocess.db")

	// Create and initialize the database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Add some initial data
	initialKeys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, key := range initialKeys {
		value := fmt.Sprintf("value%d", i+1)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			db.Close()
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Create a reader program
	readerPath := filepath.Join(tempDir, "reader.go")
	if err := createReaderProgram(readerPath, dbPath); err != nil {
		t.Fatalf("Failed to create reader program: %v", err)
	}

	// Create a writer program
	writerPath := filepath.Join(tempDir, "writer.go")
	if err := createWriterProgram(writerPath, dbPath); err != nil {
		t.Fatalf("Failed to create writer program: %v", err)
	}

	// Start a reader process first
	readerCmd := exec.Command("go", "run", readerPath)
	readerOutput := &bytes.Buffer{}
	readerCmd.Stdout = readerOutput
	readerCmd.Stderr = readerOutput
	if err := readerCmd.Start(); err != nil {
		t.Fatalf("Failed to start reader process: %v", err)
	}

	// Give the reader a moment to acquire the connection
	time.Sleep(100 * time.Millisecond)

	// Try to start a second process (writer) while the first one is running
	// This should fail because the database only allows one connection at a time
	writerCmd := exec.Command("go", "run", writerPath)
	writerOutput := &bytes.Buffer{}
	writerCmd.Stdout = writerOutput
	writerCmd.Stderr = writerOutput
	if err := writerCmd.Start(); err != nil {
		t.Fatalf("Failed to start writer process: %v", err)
	}

	// Wait for the writer to finish
	err = writerCmd.Wait()

	// Check if the writer failed as expected (it should fail to acquire a connection)
	if err == nil && !bytes.Contains(writerOutput.Bytes(), []byte("Failed to open database")) {
		t.Errorf("Writer unexpectedly succeeded while reader was active: %s", writerOutput.String())
	}

	// Wait for the reader to finish
	if err := readerCmd.Wait(); err != nil {
		t.Fatalf("Reader failed: %v\nOutput: %s", err, readerOutput.String())
	}

	// Now that the reader is done, start a new process which should succeed
	newWriterCmd := exec.Command("go", "run", writerPath)
	newWriterOutput := &bytes.Buffer{}
	newWriterCmd.Stdout = newWriterOutput
	newWriterCmd.Stderr = newWriterOutput
	if err := newWriterCmd.Start(); err != nil {
		t.Fatalf("Failed to start new writer process: %v", err)
	}

	// Wait for the new writer to finish
	if err := newWriterCmd.Wait(); err != nil {
		t.Fatalf("New writer failed: %v\nOutput: %s", err, newWriterOutput.String())
	}

	// Verify the writer succeeded
	if !bytes.Contains(newWriterOutput.Bytes(), []byte("Writer completed successfully")) {
		t.Errorf("New writer did not complete successfully: %s", newWriterOutput.String())
	}

	// Run multiple processes one after another
	for i := 0; i < 5; i++ {
		cmd := exec.Command("go", "run", writerPath)
		cmd.Stdout = &bytes.Buffer{}
		cmd.Stderr = &bytes.Buffer{}

		if err := cmd.Run(); err != nil {
			stdout := cmd.Stdout.(*bytes.Buffer).String()
			stderr := cmd.Stderr.(*bytes.Buffer).String()
			t.Errorf("Sequential writer %d failed: %v\nStdout: %s\nStderr: %s", i, err, stdout, stderr)
		} else {
			stdout := cmd.Stdout.(*bytes.Buffer).String()
			if !bytes.Contains([]byte(stdout), []byte("Writer completed successfully")) {
				t.Errorf("Sequential writer %d did not complete successfully: %s", i, stdout)
			}
		}
	}

	// Verify the database is still intact
	finalDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database for final verification: %v", err)
	}
	defer finalDB.Close()

	// Check that the original keys still exist
	for i, key := range initialKeys {
		value, err := finalDB.Get([]byte(key))
		if err != nil {
			// Key might have been deleted during operations - this is expected
			continue
		}

		expectedValue := fmt.Sprintf("value%d", i+1)
		if !bytes.Equal(value, []byte(expectedValue)) && !bytes.Equal(value, []byte("updated-value")) {
			t.Fatalf("Value mismatch for key %s in final verification: got %s, want either %s or updated-value",
				key, string(value), expectedValue)
		}
	}

	// Check if the writer-key exists (at least one writer should have succeeded)
	newValue, err := finalDB.Get([]byte("writer-key"))
	if err != nil {
		t.Errorf("No writer succeeded in writing writer-key")
	} else if !bytes.Contains(newValue, []byte("writer-value")) {
		t.Fatalf("Value mismatch for writer-key: %s", string(newValue))
	}
}

// createReaderProgram creates a Go program that reads from the database
func createReaderProgram(filePath, dbPath string) error {
	programContent := fmt.Sprintf(`package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aergoio/hashtabledb"
)

func main() {
	// Open the database
	db, err := hashtabledb.Open(%q)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %%v\n", err)
		os.Exit(1)
	}

	// Hold the database connection for a moment to simulate work
	// This ensures the connection stays open while we try to open another one
	time.Sleep(500 * time.Millisecond)

	// Read some keys
	keys := []string{"key1", "key2", "key3", "key4", "key5", "writer-key"}
	for _, key := range keys {
		value, err := db.Get([]byte(key))
		if err != nil {
			// Don't fail on writer-key as it might not exist yet
			if key != "writer-key" {
				fmt.Fprintf(os.Stderr, "Failed to get key %%s: %%v\n", key, err)
			}
		} else {
			fmt.Printf("Read key %%s: %%s\n", key, string(value))
		}
	}

	// Close the database
	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to close database: %%v\n", err)
		os.Exit(1)
	}

	fmt.Println("Reader completed successfully")
}
`, dbPath)

	return os.WriteFile(filePath, []byte(programContent), 0644)
}

// createWriterProgram creates a Go program that writes to the database
func createWriterProgram(filePath, dbPath string) error {
	programContent := fmt.Sprintf(`package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aergoio/hashtabledb"
)

func main() {
	// Open the database
	db, err := hashtabledb.Open(%q)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %%v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Write a new key
	timestamp := time.Now().UnixNano()
	value := fmt.Sprintf("writer-value-%%d", timestamp)

	if err := db.Set([]byte("writer-key"), []byte(value)); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write key: %%v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Wrote writer-key: %%s\n", value)

	// Update an existing key
	if err := db.Set([]byte("key1"), []byte("updated-value")); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to update key1: %%v\n", err)
		os.Exit(1)
	}

	fmt.Println("Updated key1 to 'updated-value'")

	// Small delay to simulate work
	time.Sleep(200 * time.Millisecond)

	fmt.Println("Writer completed successfully")
}
`, dbPath)

	return os.WriteFile(filePath, []byte(programContent), 0644)
}

// TestCrashRecovery tests database recovery after being killed during write operations
func TestCrashRecovery(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping crash recovery test in short mode")
	}

	// Test different write modes
	writeModes := []string{
		CallerThread_WAL_Sync,
		CallerThread_WAL_NoSync,
		WorkerThread_WAL,
	}

	for _, writeMode := range writeModes {
		t.Run(writeMode, func(t *testing.T) {
			testCrashRecoveryWithWriteMode(t, writeMode)
		})
	}
}

// testCrashRecoveryWithWriteMode tests crash recovery with a specific write mode
func testCrashRecoveryWithWriteMode(t *testing.T, writeMode string) {
	t.Logf("Testing crash recovery with WriteMode: %s", writeMode)

	// Create a temporary directory for our test
	tempDir, err := os.MkdirTemp("", "crash_recovery_test_"+writeMode)
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Path to the database file
	dbPath := filepath.Join(tempDir, "crash_recovery.db")

	// Create and initialize the database with some baseline data
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Add some initial baseline data
	baselineKeys := []string{"baseline1", "baseline2", "baseline3", "baseline4", "baseline5"}
	for i, key := range baselineKeys {
		value := fmt.Sprintf("baseline_value_%d", i+1)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			db.Close()
			t.Fatalf("Failed to set baseline data: %v", err)
		}
	}
	db.Close()

	// Check initial record count after baseline setup
	initialDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database for initial count: %v", err)
	}
	initialCount := 0
	iter := initialDB.NewIterator()
	for iter.Valid() {
		initialCount++
		iter.Next()
	}
	iter.Close()
	initialDB.Close()
	t.Logf("Initial database contains %d records after baseline setup", initialCount)

	// Create a continuous writer program that performs many write operations
	writerPath := filepath.Join(tempDir, "crash_writer.go")
	if err := createCrashWriterProgram(writerPath, dbPath, writeMode); err != nil {
		t.Fatalf("Failed to create crash writer program: %v", err)
	}

	// Compile the crash writer program to avoid go run wrapper process issues
	writerBinary := filepath.Join(tempDir, "crash_writer")
	compileCmd := exec.Command("go", "build", "-o", writerBinary, writerPath)
	if err := compileCmd.Run(); err != nil {
		t.Fatalf("Failed to compile crash writer program: %v", err)
	}

	// Run crash-recovery cycle 10 times
	const cycles = 10
	for cycle := 0; cycle < cycles; cycle++ {
		t.Logf("Starting crash recovery cycle %d/%d", cycle+1, cycles)

		// Start the writer process (using compiled binary)
		writerCmd := exec.Command(writerBinary, strconv.Itoa(cycle))
		writerOutput := &bytes.Buffer{}
		writerCmd.Stdout = writerOutput
		writerCmd.Stderr = writerOutput

		if err := writerCmd.Start(); err != nil {
			t.Fatalf("Failed to start writer process in cycle %d: %v", cycle, err)
		}

		// Wait for the database to be opened and ready for writes
		// We'll check the output periodically until we see the ready signal
		dbReady := false
		for attempts := 0; attempts < 100 && !dbReady; attempts++ {
			time.Sleep(10 * time.Millisecond)
			if bytes.Contains(writerOutput.Bytes(), []byte("DB_OPENED_READY_FOR_WRITES")) {
				dbReady = true
				break
			}
		}

		if !dbReady {
			writerCmd.Process.Kill()
			t.Fatalf("Writer process didn't signal ready state in cycle %d. Output: %s", cycle, writerOutput.String())
		}

		t.Logf("Database opened and ready for writes in cycle %d", cycle+1)

		// Let the writer perform some operations (random between 50-100ms)
		sleepTime := time.Duration(50+cycle*5) * time.Millisecond
		time.Sleep(sleepTime)

		// Kill the process abruptly while it's writing
		t.Logf("Attempting to kill writer process in cycle %d (PID: %d)", cycle+1, writerCmd.Process.Pid)
		if err := writerCmd.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatalf("Failed to kill writer process in cycle %d: %v", cycle, err)
		}

		// Wait for the process to be killed with a timeout
		t.Logf("Waiting for writer process to be killed in cycle %d", cycle+1)
		done := make(chan error, 1)
		go func() {
			done <- writerCmd.Wait()
		}()

		select {
		case err := <-done:
			t.Logf("Writer process in cycle %d exited with: %v", cycle+1, err)
		case <-time.After(2 * time.Second):
			t.Logf("Writer process in cycle %d didn't exit after SIGKILL, force killing", cycle+1)
			writerCmd.Process.Kill()
			// Wait a bit more
			select {
			case err := <-done:
				t.Logf("Writer process in cycle %d finally exited with: %v", cycle+1, err)
			case <-time.After(1 * time.Second):
				t.Errorf("Writer process in cycle %d never exited even after Kill()", cycle+1)
				return // Exit the test
			}
		}

		t.Logf("Killed writer process in cycle %d after %v", cycle+1, sleepTime)

		// Now try to open the database again and verify it's functional
		recoveryDB, err := Open(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database for recovery verification in cycle %d: %v", cycle, err)
		}

		// Verify baseline data is still intact
		for i, key := range baselineKeys {
			value, err := recoveryDB.Get([]byte(key))
			if err != nil {
				t.Errorf("Failed to get baseline key %s in cycle %d: %v", key, cycle, err)
				continue
			}
			expectedValue := fmt.Sprintf("baseline_value_%d", i+1)
			if string(value) != expectedValue {
				t.Errorf("Baseline data corruption in cycle %d: key %s, got %s, want %s",
					cycle, key, string(value), expectedValue)
			}
		}

		// Perform some new write operations to ensure the database is fully functional
		testKey := fmt.Sprintf("recovery_test_%d", cycle)
		testValue := fmt.Sprintf("recovery_value_%d_%d", cycle, time.Now().UnixNano())

		if err := recoveryDB.Set([]byte(testKey), []byte(testValue)); err != nil {
			t.Errorf("Failed to write recovery test data in cycle %d: %v", cycle, err)
		}

		// Verify we can read the data back
		readValue, err := recoveryDB.Get([]byte(testKey))
		if err != nil {
			t.Errorf("Failed to read recovery test data in cycle %d: %v", cycle, err)
		} else if string(readValue) != testValue {
			t.Errorf("Recovery test data mismatch in cycle %d: got %s, want %s",
				cycle, string(readValue), testValue)
		}

		// Perform an iterator test to ensure database structure is intact
		count := 0
		iter := recoveryDB.NewIterator()
		for iter.Valid() {
			count++
			iter.Next()
		}
		iter.Close()

		if count == 0 {
			t.Errorf("Iterator found no records in cycle %d - database may be corrupted", cycle)
		}

		recoveryDB.Close()
		t.Logf("Successfully recovered and verified database in cycle %d (found %d total records)", cycle+1, count)
	}

	// Final verification - open the database one more time and check everything
	finalDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database for final verification: %v", err)
	}
	defer finalDB.Close()

	// Verify baseline data is still intact after all cycles
	for i, key := range baselineKeys {
		value, err := finalDB.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get baseline key %s in final verification: %v", key, err)
			continue
		}
		expectedValue := fmt.Sprintf("baseline_value_%d", i+1)
		if string(value) != expectedValue {
			t.Errorf("Final baseline data corruption: key %s, got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Count total records in final state
	finalCount := 0
	finalIter := finalDB.NewIterator()
	for finalIter.Valid() {
		finalCount++
		finalIter.Next()
	}
	finalIter.Close()

	t.Logf("Crash recovery test completed successfully. Final database contains %d records", finalCount)
}

// createCrashWriterProgram creates a Go program that performs continuous write operations
func createCrashWriterProgram(filePath, dbPath, writeMode string) error {
	programContent := fmt.Sprintf(`package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aergoio/hashtabledb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: crash_writer <cycle_number>\n")
		os.Exit(1)
	}

	cycle := os.Args[1]

	// Open the database with specified write mode for crash testing
	options := hashtabledb.Options{
		"WriteMode": %q,
	}
	db, err := hashtabledb.Open(%q, options)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %%v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Signal that database is opened and ready for writes
	fmt.Printf("DB_OPENED_READY_FOR_WRITES\n")

	fmt.Printf("Crash writer started for cycle %%s\n", cycle)

	// Perform many write operations continuously
	for i := 0; i < 1000000; i++ {
		key := fmt.Sprintf("crash_test_%%s_%%d", cycle, i)
		value := fmt.Sprintf("crash_value_%%s_%%d_%%d", cycle, i, time.Now().UnixNano())

		if err := db.Set([]byte(key), []byte(value)); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write key %%s: %%v\n", key, err)
			os.Exit(1)
		}

		// Occasionally read some data to mix operations
		if i%%10 == 0 {
			readKey := fmt.Sprintf("crash_test_%%s_%%d", cycle, i/2)
			_, _ = db.Get([]byte(readKey)) // Ignore errors as key might not exist
		}

		// Print progress every 10000 records to avoid too much output
		if i%%10000 == 0 {
			fmt.Printf("Wrote record %%d in cycle %%s\n", i, cycle)
		}
	}

	fmt.Printf("Crash writer completed cycle %%s\n", cycle)
}
`, writeMode, dbPath)

	return os.WriteFile(filePath, []byte(programContent), 0644)
}
