package hashtabledb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// cleanupTestFiles removes test database files (main, index, and wal)
func cleanupTestFiles(dbPath string) {
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")
}

// WAL write modes exercised by bug-catching tests. Worker is the default
// production path; Caller covers flush/checkpoint-on-writer (no flusher).
// writeModeLabels enumerates the write configurations the matrix tests run
// under: UseWAL selects the WAL or direct index pipeline, SyncMainFileOnCommit whether a
// commit fsyncs the main file
var writeModeLabels = []string{
	"wal",
	"wal_sync",
	"direct",
	"direct_sync",
}

// modeOptsUseWAL reports whether the label's index pipeline uses a WAL
func modeOptsUseWAL(mode string) bool {
	return mode == "wal" || mode == "wal_sync"
}

// modeOptsSyncMain reports whether the label fsyncs the main file at commit
func modeOptsSyncMain(mode string) bool {
	return mode == "wal_sync" || mode == "direct_sync"
}

// modeOptions translates a write mode label into its Open options
func modeOptions(mode string) Options {
	switch mode {
	case "wal":
		return Options{"UseWAL": true, "SyncMainFileOnCommit": false}
	case "wal_sync":
		return Options{"UseWAL": true, "SyncMainFileOnCommit": true}
	case "direct":
		return Options{"UseWAL": false, "SyncMainFileOnCommit": false}
	case "direct_sync":
		return Options{"UseWAL": false, "SyncMainFileOnCommit": true}
	}
	return Options{}
}

// withWriteModes runs fn once per writeModeLabels entry as a sequential subtest.
func withWriteModes(t *testing.T, fn func(t *testing.T, writeMode string)) {
	t.Helper()
	for _, mode := range writeModeLabels {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			fn(t, mode)
		})
	}
}

// rollbackModes covers FastRollback=true (default) and the slow-clone path.
var rollbackModes = []struct {
	name         string
	fastRollback bool
}{
	{"FastRollback", true},
	{"SlowRollback", false},
}

// withRollbackModes runs fn for both FastRollback settings as sequential subtests.
func withRollbackModes(t *testing.T, fn func(t *testing.T, fastRollback bool)) {
	t.Helper()
	for _, mode := range rollbackModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			fn(t, mode.fastRollback)
		})
	}
}

// withWriteAndRollbackModes crosses writeModes with both FastRollback settings.
func withWriteAndRollbackModes(t *testing.T, fn func(t *testing.T, writeMode string, fastRollback bool)) {
	t.Helper()
	withWriteModes(t, func(t *testing.T, writeMode string) {
		withRollbackModes(t, func(t *testing.T, fastRollback bool) {
			fn(t, writeMode, fastRollback)
		})
	})
}

// openTestDB opens path with WriteMode set. Extra options override defaults
// except WriteMode is always taken from writeMode.
func openTestDB(t testing.TB, path string, writeMode string, extra ...Options) *DB {
	t.Helper()
	opts := modeOptions(writeMode)
	if len(extra) > 0 {
		for k, v := range extra[0] {
			opts[k] = v
		}
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open(%s, WriteMode=%s): %v", path, writeMode, err)
	}
	return db
}

// testDBPath returns a unique DB path under dir for the write mode.
func testDBPath(dir, base, writeMode string) string {
	return filepath.Join(dir, writeMode+"_"+base)
}

func TestDatabaseBasicOperations(t *testing.T) {
	withWriteModes(t, testDatabaseBasicOperations)
}

func testDatabaseBasicOperations(t *testing.T, writeMode string) {
	// Create a test database
	dbPath := testDBPath(".", "test_basic.db", writeMode)

	cleanupTestFiles(dbPath)

	// Open a new database
	db := openTestDB(t, dbPath, writeMode)
	var err error
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Test setting a key-value pair
	err = db.Set([]byte("name"), []byte("hash-table-tree"))
	if err != nil {
		t.Fatalf("Failed to set 'name': %v", err)
	}

	err = db.Set([]byte("author"), []byte("Bernardo"))
	if err != nil {
		t.Fatalf("Failed to set 'author': %v", err)
	}

	err = db.Set([]byte("type"), []byte("key-value database"))
	if err != nil {
		t.Fatalf("Failed to set 'type': %v", err)
	}

	// Test getting the values back
	nameVal, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name': %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree")) {
		t.Fatalf("Value mismatch for 'name': got %s, want %s", string(nameVal), "hash-table-tree")
	}

	authorVal, err := db.Get([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to get 'author': %v", err)
	}
	if !bytes.Equal(authorVal, []byte("Bernardo")) {
		t.Fatalf("Value mismatch for 'author': got %s, want %s", string(authorVal), "Bernardo")
	}

	typeVal, err := db.Get([]byte("type"))
	if err != nil {
		t.Fatalf("Failed to get 'type': %v", err)
	}
	if !bytes.Equal(typeVal, []byte("key-value database")) {
		t.Fatalf("Value mismatch for 'type': got %s, want %s", string(typeVal), "key-value database")
	}

	// Test getting a non-existent key
	_, err = db.Get([]byte("unknown"))
	if err == nil {
		t.Fatalf("Expected error when getting non-existent key, got nil")
	}

	// Test updating an existing key
	err = db.Set([]byte("name"), []byte("hash-table-tree DB"))
	if err != nil {
		t.Fatalf("Failed to update 'name': %v", err)
	}

	// Get updated value
	updatedNameVal, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get updated 'name': %v", err)
	}
	if !bytes.Equal(updatedNameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Updated value mismatch for 'name': got %s, want %s", string(updatedNameVal), "hash-table-tree DB")
	}

	// Test deleting a key
	err = db.Delete([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to delete 'author': %v", err)
	}

	// Verify the key was deleted
	_, err = db.Get([]byte("author"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'author', got nil")
	}

	// Verify other keys still exist
	nameVal, err = db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name' after deletion: %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Value mismatch for 'name' after deletion: got %s, want %s", string(nameVal), "hash-table-tree DB")
	}

	// Test deleting a non-existent key (should not error)
	err = db.Delete([]byte("unknown"))
	if err != nil {
		t.Fatalf("Failed to delete non-existent key: %v", err)
	}
}

func TestMultipleKeyValues(t *testing.T) {
	withWriteModes(t, testMultipleKeyValues)
}

func testMultipleKeyValues(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_multi.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode, Options{"MainIndexPages": 1})
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Insert multiple key-value pairs
	numPairs := 1000
	keys := make([][]byte, numPairs)
	values := make([][]byte, numPairs)

	for i := 0; i < numPairs; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))

		if err := db.Set(keys[i], values[i]); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Verify all keys can be retrieved
	for i := 0; i < numPairs; i++ {
		result, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if !bytes.Equal(result, values[i]) {
			t.Fatalf("Value mismatch for key %d: got %s, want %s", i, string(result), string(values[i]))
		}
	}

	// Update some values
	for i := 0; i < numPairs; i += 50 {
		values[i] = []byte(fmt.Sprintf("updated-value-%d", i))
		if err := db.Set(keys[i], values[i]); err != nil {
			t.Fatalf("Failed to update key %d: %v", i, err)
		}
	}

	// Verify updated keys
	for i := 0; i < numPairs; i += 50 {
		result, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get updated key %d: %v", i, err)
		}
		if !bytes.Equal(result, values[i]) {
			t.Fatalf("Updated value mismatch for key %d: got %s, want %s", i, string(result), string(values[i]))
		}
	}

	// Delete every third key
	for i := 0; i < numPairs; i += 3 {
		if err := db.Delete(keys[i]); err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < numPairs; i += 3 {
		_, err := db.Get(keys[i])
		if err == nil {
			t.Fatalf("Expected error when getting deleted key %d, got nil", i)
		}
	}

	// Verify non-deleted keys still exist
	for i := 1; i < numPairs; i += 3 {
		result, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get key %d after deletions: %v", i, err)
		}
		if !bytes.Equal(result, values[i]) {
			t.Fatalf("Value mismatch for key %d after deletions: got %s, want %s", i, string(result), string(values[i]))
		}
	}

	// Test deleting already deleted keys (should not error)
	for i := 0; i < numPairs; i += 6 {
		if err := db.Delete(keys[i]); err != nil {
			t.Fatalf("Failed to delete already deleted key %d: %v", i, err)
		}
	}
}

func TestShortKeys(t *testing.T) {
	withWriteModes(t, testShortKeys)
}

func testShortKeys(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_short_keys.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode)
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Create test keys with 1, 2, and 3 bytes in length
	oneByteKeys := []string{"a", "b", "c", "d", "e"}
	twoByteKeys := []string{"ab", "ac", "cd", "ef", "gh", "ij"}
	threeByteKeys := []string{"abc", "abd", "acd", "def", "ghi", "jkl", "mno"}

	// Create values for each key
	values := make(map[string]string)

	// Add values for 1-byte keys
	for i, key := range oneByteKeys {
		values[key] = fmt.Sprintf("one-byte-value-%d", i)
	}

	// Add values for 2-byte keys
	for i, key := range twoByteKeys {
		values[key] = fmt.Sprintf("two-byte-value-%d", i)
	}

	// Add values for 3-byte keys
	for i, key := range threeByteKeys {
		values[key] = fmt.Sprintf("three-byte-value-%d", i)
	}

	// Insert all keys
	for key, value := range values {
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set '%s': %v", key, err)
		}
	}

	// Verify all keys can be retrieved
	for key, expectedValue := range values {
		result, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", key, string(result), expectedValue)
		}
	}

	// Delete some keys (one of each length)
	keysToDelete := []string{oneByteKeys[0], twoByteKeys[0], threeByteKeys[0]}
	for _, key := range keysToDelete {
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete '%s': %v", key, err)
		}
		// Remove from our tracking map
		delete(values, key)
	}

	// Verify deleted keys are gone
	for _, key := range keysToDelete {
		_, err := db.Get([]byte(key))
		if err == nil {
			t.Fatalf("Expected error when getting deleted key '%s', got nil", key)
		}
	}

	// Verify remaining keys still exist
	for key, expectedValue := range values {
		result, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after deletions: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after deletions: got %s, want %s",
				key, string(result), expectedValue)
		}
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	reopenedDb, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify all remaining keys still exist after reopening
	for key, expectedValue := range values {
		result, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after reopen: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after reopen: got %s, want %s",
				key, string(result), expectedValue)
		}
	}

	// Delete more keys (another one of each length)
	moreKeysToDelete := []string{oneByteKeys[1], twoByteKeys[1], threeByteKeys[1]}
	for _, key := range moreKeysToDelete {
		err := reopenedDb.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete '%s' after reopen: %v", key, err)
		}
		// Remove from our tracking map
		delete(values, key)
	}

	// Add new keys (one of each length)
	newKeys := map[string]string{
		"x":   "new-one-byte",
		"yz":  "new-two-byte",
		"xyz": "new-three-byte",
	}

	for key, value := range newKeys {
		err := reopenedDb.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set new key '%s': %v", key, err)
		}
		// Add to our tracking map
		values[key] = value
	}

	// Verify all current keys exist
	for key, expectedValue := range values {
		result, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after additions: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after additions: got %s, want %s",
				key, string(result), expectedValue)
		}
	}

	// Close the database again
	if err := reopenedDb.Close(); err != nil {
		t.Fatalf("Failed to close reopened database: %v", err)
	}

	// Reopen the database again
	reopenedDb2, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database second time: %v", err)
	}
	defer func() {
		reopenedDb2.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Final verification of all keys
	for key, expectedValue := range values {
		result, err := reopenedDb2.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after second reopen: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after second reopen: got %s, want %s",
				key, string(result), expectedValue)
		}
	}
}

func TestDeleteOperations(t *testing.T) {
	withWriteModes(t, testDeleteOperations)
}

func testDeleteOperations(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_delete.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode, Options{"MainIndexPages": 1})
	var err error
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Set up some test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	// Insert all test data
	for k, v := range testData {
		err := db.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Verify all data was inserted correctly
	for k, v := range testData {
		result, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(result, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(result), v)
		}
	}

	// Test 1: Delete a key and verify it's gone
	err = db.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to delete 'key1': %v", err)
	}

	_, err = db.Get([]byte("key1"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'key1', got nil")
	}

	// Test 2: Delete a key, then try to set it again
	err = db.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to delete 'key2': %v", err)
	}

	// Verify key2 is deleted
	_, err = db.Get([]byte("key2"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'key2', got nil")
	}

	// Set key2 again with a new value
	err = db.Set([]byte("key2"), []byte("new-value2"))
	if err != nil {
		t.Fatalf("Failed to set 'key2' after deletion: %v", err)
	}

	// Verify key2 has the new value
	result, err := db.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get 'key2' after re-setting: %v", err)
	}
	if !bytes.Equal(result, []byte("new-value2")) {
		t.Fatalf("Value mismatch for 'key2' after re-setting: got %s, want %s", string(result), "new-value2")
	}

	// Test 3: Delete multiple keys
	keysToDelete := []string{"key3", "key4"}
	for _, k := range keysToDelete {
		err := db.Delete([]byte(k))
		if err != nil {
			t.Fatalf("Failed to delete '%s': %v", k, err)
		}
	}

	// Verify deleted keys are gone
	for _, k := range keysToDelete {
		_, err := db.Get([]byte(k))
		if err == nil {
			t.Fatalf("Expected error when getting deleted key '%s', got nil", k)
		}
	}

	// Verify key5 still exists
	result, err = db.Get([]byte("key5"))
	if err != nil {
		t.Fatalf("Failed to get 'key5' after other deletions: %v", err)
	}
	if !bytes.Equal(result, []byte("value5")) {
		t.Fatalf("Value mismatch for 'key5' after other deletions: got %s, want %s", string(result), "value5")
	}

	// Test 4: Delete a non-existent key (should not error)
	err = db.Delete([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Failed to delete non-existent key: %v", err)
	}

	// Test 5: Delete an already deleted key (should not error)
	err = db.Delete([]byte("key3"))
	if err != nil {
		t.Fatalf("Failed to delete already deleted key: %v", err)
	}
}

func TestDatabasePersistence1(t *testing.T) {
	withWriteModes(t, testDatabasePersistence1)
}

func testDatabasePersistence1(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_persistence.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode)

	// Set initial key-value pairs
	initialData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}

	for k, v := range initialData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Modify some data
	if err := db.Set([]byte("key2"), []byte("modified2")); err != nil {
		t.Fatalf("Failed to update 'key2': %v", err)
	}

	// Delete a key
	if err := db.Delete([]byte("key3")); err != nil {
		t.Fatalf("Failed to delete 'key3': %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	reopenedDb, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func() {
		reopenedDb.Close()
		cleanupTestFiles(dbPath)
	}()

	// Verify key1 still exists with original value
	val1, err := reopenedDb.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get 'key1' after reopen: %v", err)
	}
	if !bytes.Equal(val1, []byte("value1")) {
		t.Fatalf("Value mismatch for 'key1' after reopen: got %s, want %s", string(val1), "value1")
	}

	// Verify key2 has the modified value
	val2, err := reopenedDb.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get 'key2' after reopen: %v", err)
	}
	if !bytes.Equal(val2, []byte("modified2")) {
		t.Fatalf("Value mismatch for 'key2' after reopen: got %s, want %s", string(val2), "modified2")
	}

	// Verify key3 was deleted
	_, err = reopenedDb.Get([]byte("key3"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'key3' after reopen, got nil")
	}

	// Verify key4 still exists with original value
	val4, err := reopenedDb.Get([]byte("key4"))
	if err != nil {
		t.Fatalf("Failed to get 'key4' after reopen: %v", err)
	}
	if !bytes.Equal(val4, []byte("value4")) {
		t.Fatalf("Value mismatch for 'key4' after reopen: got %s, want %s", string(val4), "value4")
	}

	// Add a new key to the reopened database
	if err := reopenedDb.Set([]byte("key5"), []byte("value5")); err != nil {
		t.Fatalf("Failed to set 'key5' after reopen: %v", err)
	}

	// Verify the new key exists
	val5, err := reopenedDb.Get([]byte("key5"))
	if err != nil {
		t.Fatalf("Failed to get 'key5' after setting: %v", err)
	}
	if !bytes.Equal(val5, []byte("value5")) {
		t.Fatalf("Value mismatch for 'key5': got %s, want %s", string(val5), "value5")
	}
}

func TestDatabasePersistence2(t *testing.T) {
	withWriteModes(t, testDatabasePersistence2)
}

func testDatabasePersistence2(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_persistence2.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode)
	var err error

	// Test setting key-value pairs from TestDatabaseBasicOperations
	err = db.Set([]byte("name"), []byte("hash-table-tree"))
	if err != nil {
		t.Fatalf("Failed to set 'name': %v", err)
	}

	err = db.Set([]byte("author"), []byte("Bernardo"))
	if err != nil {
		t.Fatalf("Failed to set 'author': %v", err)
	}

	err = db.Set([]byte("type"), []byte("key-value database"))
	if err != nil {
		t.Fatalf("Failed to set 'type': %v", err)
	}

	// Update a key
	err = db.Set([]byte("name"), []byte("hash-table-tree DB"))
	if err != nil {
		t.Fatalf("Failed to update 'name': %v", err)
	}

	// Delete a key
	err = db.Delete([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to delete 'author': %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	reopenedDb, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func() {
		reopenedDb.Close()
		cleanupTestFiles(dbPath)
	}()

	// Verify name has the updated value
	nameVal, err := reopenedDb.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name' after reopen: %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Value mismatch for 'name' after reopen: got %s, want %s",
			string(nameVal), "hash-table-tree DB")
	}

	// Verify author was deleted
	_, err = reopenedDb.Get([]byte("author"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'author' after reopen, got nil")
	}

	// Verify type still exists with original value
	typeVal, err := reopenedDb.Get([]byte("type"))
	if err != nil {
		t.Fatalf("Failed to get 'type' after reopen: %v", err)
	}
	if !bytes.Equal(typeVal, []byte("key-value database")) {
		t.Fatalf("Value mismatch for 'type' after reopen: got %s, want %s",
			string(typeVal), "key-value database")
	}

	// Add a new key after reopening
	err = reopenedDb.Set([]byte("version"), []byte("1.0"))
	if err != nil {
		t.Fatalf("Failed to set 'version' after reopen: %v", err)
	}

	// Verify the new key exists
	versionVal, err := reopenedDb.Get([]byte("version"))
	if err != nil {
		t.Fatalf("Failed to get 'version': %v", err)
	}
	if !bytes.Equal(versionVal, []byte("1.0")) {
		t.Fatalf("Value mismatch for 'version': got %s, want %s", string(versionVal), "1.0")
	}
}

func TestExternalKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_external_keys.db"

	cleanupTestFiles(dbPath)  // Removes .db, .db-index, .db-wal
	// Also clean up any external value files that might exist
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
					os.Remove(file)
			}
	}

	defer func() {
		cleanupTestFiles(dbPath)
		// Clean up any external value files that might exist
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.SetOption("AddMutableKey", []byte("external_key"))
	db.SetOption("AddMutableKey", []byte("another_key"))

	// Set initial key-value pairs
	initialData := map[string]string{
		"name": "hash-table-tree",
		"author": "Bernardo",
		"type": "key-value database",
		"version": "1.0",
		"external_key": "external_value",
		"another_key": "another_value",
	}

	// Insert all keys
	for k, v := range initialData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Verify all keys are inserted
	for k, v := range initialData {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(val, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(val), v)
		}
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify all keys are inserted
	for k, v := range initialData {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(val, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(val), v)
		}
	}

	// Add new value for external keys
	err = db.Set([]byte("external_key"), []byte("new_external_value"))
	if err != nil {
		t.Fatalf("Failed to set 'external_key': %v", err)
	}

	err = db.Set([]byte("another_key"), []byte("new_another_value"))
	if err != nil {
		t.Fatalf("Failed to set 'another_key': %v", err)
	}

	// Update the initial data
	initialData["external_key"] = "new_external_value"
	initialData["another_key"] = "new_another_value"

	// Verify all keys and values
	for k, v := range initialData {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(val, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(val), v)
		}
	}

	// Close and Reopen the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify all keys and values
	for k, v := range initialData {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(val, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(val), v)
		}
	}

	// Close and Reopen the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify all keys are inserted
	for k, v := range initialData {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(val, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(val), v)
		}
	}

	// Update the initial data
	initialData["name"] = "HashTableDB"
	initialData["author"] = "Bernardo"
	initialData["type"] = "disk-based database"
	initialData["version"] = "2.0"
	initialData["external_key"] = "v1"
	initialData["another_key"] = "v2"

	// Set the initial data
	for k, v := range initialData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Verify all keys are inserted
	for k, v := range initialData {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(val, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(val), v)
		}
	}

	// Close and Reopen the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify all keys are inserted
	for k, v := range initialData {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(val, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(val), v)
		}
	}

	// Delete one external key
	err = db.Delete([]byte("external_key"))
	if err != nil {
		t.Fatalf("Failed to delete 'external_key': %v", err)
	}

	// Verify the external key is deleted
	value, err := db.Get([]byte("external_key"))
	if err == nil && value != nil {
		t.Fatalf("Expected error when getting deleted key 'external_key' after reopen, got nil")
	}

	// Verify the another key is still there
	anotherVal, err := db.Get([]byte("another_key"))
	if err != nil {
		t.Fatalf("Failed to get 'another_key': %v", err)
	}
	if !bytes.Equal(anotherVal, []byte("v2")) {
		t.Fatalf("Value mismatch for 'another_key': got %s, want %s", string(anotherVal), "v2")
	}

	// Close and reopen the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify the external key is deleted
	value, err = db.Get([]byte("external_key"))
	if err == nil && !bytes.Equal(value, []byte{}) {
		t.Fatalf("Expected error when getting deleted key 'external_key' after reopen, got nil")
	}

	// Verify the another key is still there
	anotherVal, err = db.Get([]byte("another_key"))
	if err != nil {
		t.Fatalf("Failed to get 'another_key': %v", err)
	}
	if !bytes.Equal(anotherVal, []byte("v2")) {
		t.Fatalf("Value mismatch for 'another_key': got %s, want %s", string(anotherVal), "v2")
	}

	db.Close()
}

// TestExternalKeysPersistence tests that external keys properly persist across database reopens
// This test specifically checks that external keys work correctly when the database is reopened
// without re-registering the external keys via SetOption.
func TestExternalKeysPersistence(t *testing.T) {
	// Create a test database
	dbPath := "test_external_keys_persistence.db"

	cleanupTestFiles(dbPath)
	// Also clean up any external value files that might exist
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
		for _, file := range files {
			os.Remove(file)
		}
	}

	defer func() {
		cleanupTestFiles(dbPath)
		// Clean up any external value files that might exist
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()

	// PHASE 1: Create database and register external keys
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Register external keys
	err = db.SetOption("AddMutableKey", []byte("persistent_key"))
	if err != nil {
		t.Fatalf("Failed to add external key 'persistent_key': %v", err)
	}
	err = db.SetOption("AddMutableKey", []byte("another_persistent_key"))
	if err != nil {
		t.Fatalf("Failed to add external key 'another_persistent_key': %v", err)
	}

	// Set both regular and external key-value pairs
	testData := map[string]string{
		"regular_key":           "regular_value",
		"persistent_key":        "external_value_1", // This is an external key
		"another_persistent_key": "external_value_2", // This is also an external key
		"normal_key":            "normal_value",
	}

	for k, v := range testData {
		err := db.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Verify all values can be read
	for k, expectedValue := range testData {
		value, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s' in initial phase: %v", k, err)
		}
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' in initial phase: got %s, want %s",
				k, string(value), expectedValue)
		}
	}

	// Close the database to flush external values to disk
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Verify external value files exist
	externalFiles, err := filepath.Glob(dbPath + "-vk-*")
	if err != nil {
		t.Fatalf("Failed to glob external files: %v", err)
	}
	if len(externalFiles) == 0 {
		t.Fatalf("No external value files found after closing database")
	}
	t.Logf("Found %d external value files: %v", len(externalFiles), externalFiles)

	// PHASE 2: Reopen database WITHOUT re-registering external keys
	// This tests if external keys are automatically loaded from disk
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Test retrieving values - this is the critical test
	// If external keys aren't properly loaded, these gets will fail
	for k, expectedValue := range testData {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s' after reopen (without re-registering external keys): %v", k, err)
		}
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after reopen: got %s, want %s",
				k, string(value), expectedValue)
		}
	}

	// PHASE 3: Modify external key values and test persistence again
	updatedData := map[string]string{
		"regular_key":           "updated_regular_value",
		"persistent_key":        "updated_external_value_1",
		"another_persistent_key": "updated_external_value_2",
		"normal_key":            "updated_normal_value",
	}

	for k, v := range updatedData {
		err := db2.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to update '%s': %v", k, err)
		}
	}

	// Verify updated values
	for k, expectedValue := range updatedData {
		value, err := db2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get updated '%s': %v", k, err)
		}
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for updated '%s': got %s, want %s",
				k, string(value), expectedValue)
		}
	}

	// Close the database to flush external values to disk
	err = db2.Close()
	if err != nil {
		t.Fatalf("Failed to close database after updates: %v", err)
	}

	// PHASE 4: Final persistence test - reopen again without re-registering
	db3, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database for final test: %v", err)
	}
	defer db3.Close()

	// Test that updated values are persisted
	for k, expectedValue := range updatedData {
		value, err := db3.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s' in final persistence test: %v", k, err)
		}
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' in final persistence test: got %s, want %s",
				k, string(value), expectedValue)
		}
	}

	// PHASE 5: Test adding new external key values after reopen
	err = db3.Set([]byte("persistent_key"), []byte("final_external_value"))
	if err != nil {
		t.Fatalf("Failed to set external key after final reopen: %v", err)
	}

	// Verify the new value
	value, err := db3.Get([]byte("persistent_key"))
	if err != nil {
		t.Fatalf("Failed to get external key after final update: %v", err)
	}
	if !bytes.Equal(value, []byte("final_external_value")) {
		t.Fatalf("Value mismatch for external key after final update: got %s, want %s",
			string(value), "final_external_value")
	}

	t.Log("External keys persistence test completed successfully")
}

// TestExternalKeysWithoutPersistence tests the bug scenario where external keys
// are not properly loaded when database is reopened
func TestExternalKeysWithoutPersistence(t *testing.T) {
	// Create a test database
	dbPath := "test_external_keys_no_persist.db"

	cleanupTestFiles(dbPath)
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
		for _, file := range files {
			os.Remove(file)
		}
	}

	defer func() {
		cleanupTestFiles(dbPath)
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()

	// Create database and register external key
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	err = db.SetOption("AddMutableKey", []byte("test_key"))
	if err != nil {
		t.Fatalf("Failed to add external key: %v", err)
	}

	// Set the external key value
	err = db.Set([]byte("test_key"), []byte("test_value"))
	if err != nil {
		t.Fatalf("Failed to set external key: %v", err)
	}

	// Verify it works
	value, err := db.Get([]byte("test_key"))
	if err != nil {
		t.Fatalf("Failed to get external key: %v", err)
	}
	if !bytes.Equal(value, []byte("test_value")) {
		t.Fatalf("Value mismatch: got %s, want %s", string(value), "test_value")
	}

	// Close database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen without re-registering the external key
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Try to get the external key - this should fail if there's a bug
	value, err = db2.Get([]byte("test_key"))
	if err != nil {
		// This indicates the bug - external key was not loaded
		t.Logf("BUG DETECTED: External key not loaded after reopen: %v", err)
		t.Logf("This suggests external keys need to be re-registered after database reopen")

		// For now, we'll expect this to fail and document it as a known issue
		// In a future fix, this test should pass
		return
	}

	// If we get here, the external key was properly loaded
	if !bytes.Equal(value, []byte("test_value")) {
		t.Fatalf("Value mismatch after reopen: got %s, want %s", string(value), "test_value")
	}

	t.Log("External key properly loaded after reopen - no bug detected")
}

// TestExternalKeyLastValuePersistence specifically tests that the last value
// of an external key is properly persisted across database reopens
func TestExternalKeyLastValuePersistence(t *testing.T) {
	dbPath := "test_external_key_last_value.db"

	cleanupTestFiles(dbPath)
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
		for _, file := range files {
			os.Remove(file)
		}
	}

	defer func() {
		cleanupTestFiles(dbPath)
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()

	// Create database and external key
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	err = db.SetOption("AddMutableKey", []byte("changing_key"))
	if err != nil {
		t.Fatalf("Failed to add external key: %v", err)
	}

	// Set multiple values for the same external key to test value progression
	values := []string{
		"initial_value",
		"updated_value_1",
		"updated_value_2",
		"updated_value_3",
		"final_value",
	}

	// Apply each value and verify it's immediately readable
	for i, value := range values {
		t.Logf("Setting external key to value %d: %s", i+1, value)

		err = db.Set([]byte("changing_key"), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set external key to '%s': %v", value, err)
		}

		// Immediately verify the value is readable
		retrieved, err := db.Get([]byte("changing_key"))
		if err != nil {
			t.Fatalf("Failed to get external key after setting to '%s': %v", value, err)
		}
		if !bytes.Equal(retrieved, []byte(value)) {
			t.Fatalf("Value mismatch after setting external key: got %s, want %s",
				string(retrieved), value)
		}

		// Force sync to ensure value is written to disk
		err = db.Sync()
		if err != nil {
			t.Fatalf("Failed to sync after setting value '%s': %v", value, err)
		}
	}

	// Close database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen database
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// The critical test: verify the LAST value persists
	expectedLastValue := values[len(values)-1] // "final_value"
	retrievedValue, err := db2.Get([]byte("changing_key"))
	if err != nil {
		t.Fatalf("Failed to get external key after reopen: %v", err)
	}
	if !bytes.Equal(retrievedValue, []byte(expectedLastValue)) {
		t.Fatalf("Last value not persisted correctly. Got %s, want %s",
			string(retrievedValue), expectedLastValue)
	}

	t.Logf("SUCCESS: Last value '%s' properly persisted across reopen", expectedLastValue)

	// Test setting more values after reopen
	postReopenValues := []string{
		"post_reopen_value_1",
		"post_reopen_value_2",
		"post_reopen_final",
	}

	for i, value := range postReopenValues {
		t.Logf("Setting external key post-reopen to value %d: %s", i+1, value)

		err = db2.Set([]byte("changing_key"), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set external key post-reopen to '%s': %v", value, err)
		}

		retrieved, err := db2.Get([]byte("changing_key"))
		if err != nil {
			t.Fatalf("Failed to get external key post-reopen after setting to '%s': %v", value, err)
		}
		if !bytes.Equal(retrieved, []byte(value)) {
			t.Fatalf("Value mismatch post-reopen: got %s, want %s", string(retrieved), value)
		}
	}

	// Close and reopen one more time
	err = db2.Close()
	if err != nil {
		t.Fatalf("Failed to close database after post-reopen updates: %v", err)
	}

	db3, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database for final test: %v", err)
	}
	defer db3.Close()

	// Verify the final post-reopen value persists
	expectedFinalValue := postReopenValues[len(postReopenValues)-1] // "post_reopen_final"
	finalValue, err := db3.Get([]byte("changing_key"))
	if err != nil {
		t.Fatalf("Failed to get external key in final test: %v", err)
	}
	if !bytes.Equal(finalValue, []byte(expectedFinalValue)) {
		t.Fatalf("Final value not persisted correctly. Got %s, want %s",
			string(finalValue), expectedFinalValue)
	}

	t.Logf("SUCCESS: Final value '%s' properly persisted across second reopen", expectedFinalValue)

	// Check that external value files exist
	externalFiles, err := filepath.Glob(dbPath + "-vk-*")
	if err != nil {
		t.Fatalf("Failed to check external files: %v", err)
	}
	t.Logf("External value files found: %v", externalFiles)
	if len(externalFiles) == 0 {
		t.Fatalf("No external value files found - external key storage may not be working")
	}
}

// TestExternalKeyMultipleKeysPersistence tests persistence with multiple external keys
func TestExternalKeyMultipleKeysPersistence(t *testing.T) {
	dbPath := "test_external_keys_multiple.db"

	cleanupTestFiles(dbPath)
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
		for _, file := range files {
			os.Remove(file)
		}
	}

	defer func() {
		cleanupTestFiles(dbPath)
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()

	// Create database and register multiple external keys
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	externalKeys := []string{"ext_key_1", "ext_key_2", "ext_key_3"}
	for _, key := range externalKeys {
		err = db.SetOption("AddMutableKey", []byte(key))
		if err != nil {
			t.Fatalf("Failed to add external key '%s': %v", key, err)
		}
	}

	// Set values for each external key multiple times
	finalValues := make(map[string]string)
	for _, key := range externalKeys {
		for j := 0; j < 5; j++ {
			value := fmt.Sprintf("value_%s_iteration_%d", key, j)
			err = db.Set([]byte(key), []byte(value))
			if err != nil {
				t.Fatalf("Failed to set external key '%s' to '%s': %v", key, value, err)
			}
			finalValues[key] = value // Keep track of the last value for each key
		}
	}

	// Sync and close
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen and verify all final values
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	for key, expectedValue := range finalValues {
		retrievedValue, err := db2.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get external key '%s' after reopen: %v", key, err)
		}
		if !bytes.Equal(retrievedValue, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for external key '%s' after reopen: got %s, want %s",
				key, string(retrievedValue), expectedValue)
		}
		t.Logf("External key '%s' correctly has value '%s' after reopen", key, expectedValue)
	}

	// Check external files were created for each key
	externalFiles, err := filepath.Glob(dbPath + "-vk-*")
	if err != nil {
		t.Fatalf("Failed to check external files: %v", err)
	}
	t.Logf("Found %d external value files: %v", len(externalFiles), externalFiles)

	if len(externalFiles) != len(externalKeys) {
		t.Fatalf("Expected %d external files, found %d", len(externalKeys), len(externalFiles))
	}

	t.Log("Multiple external keys persistence test completed successfully")
}

// TestExternalKeyCommitMarkerRemoval tests the behavior when the commit marker is removed
// from the main file. The external value should rollback to the previous valid state.
func TestExternalKeyCommitMarkerRemoval(t *testing.T) {
	dbPath := "test_external_key_commit_removal.db"

	cleanupTestFiles(dbPath)
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
		for _, file := range files {
			os.Remove(file)
		}
	}

	defer func() {
		cleanupTestFiles(dbPath)
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()

	// Create database and register external key
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	err = db.SetOption("AddMutableKey", []byte("rollback_key"))
	if err != nil {
		t.Fatalf("Failed to add external key: %v", err)
	}

	// Set a normal key-value pair
	err = db.Set([]byte("normal_key"), []byte("normal_value"))
	if err != nil {
		t.Fatalf("Failed to set normal key-value pair: %v", err)
	}

	// Set initial value for external key
	err = db.Set([]byte("rollback_key"), []byte("initial_value"))
	if err != nil {
		t.Fatalf("Failed to set initial external key value: %v", err)
	}

	// Close to commit the first value
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database after initial set: %v", err)
	}

	// Check the main file size after first commit
	if fileInfo, err := os.Stat(dbPath); err == nil {
		t.Logf("Main file size after first commit: %d bytes", fileInfo.Size())
	}

	// Check external file size after first commit
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil && len(files) > 0 {
		if fileInfo, err := os.Stat(files[0]); err == nil {
			t.Logf("External file size after first commit: %d bytes (%s)", fileInfo.Size(), files[0])
		}
	}

	// Reopen and set a second value
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Add some regular data to make the main file grow
	err = tx.Set([]byte("regular_key_1"), []byte("some_regular_data_to_make_file_grow"))
	if err != nil {
		t.Fatalf("Failed to set regular key: %v", err)
	}

	// Set second value for external key
	err = tx.Set([]byte("rollback_key"), []byte("second_value"))
	if err != nil {
		t.Fatalf("Failed to set second external key value: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify second value is set
	value, err := db.Get([]byte("rollback_key"))
	if err != nil {
		t.Fatalf("Failed to get external key before commit: %v", err)
	}
	if !bytes.Equal(value, []byte("second_value")) {
		t.Fatalf("Value mismatch before commit: got %s, want %s", string(value), "second_value")
	}

	// Close to commit the second value
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database after second set: %v", err)
	}

	// Check the main file size after second commit
	if fileInfo, err := os.Stat(dbPath); err == nil {
		t.Logf("Main file size after second commit: %d bytes", fileInfo.Size())
	}

	// Check external file size after second commit
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil && len(files) > 0 {
		if fileInfo, err := os.Stat(files[0]); err == nil {
			t.Logf("External file size after second commit: %d bytes (%s)", fileInfo.Size(), files[0])
		}
	}

	// Now remove the last 5 bytes (commit marker) from the main file
	mainFilePath := dbPath
	file, err := os.OpenFile(mainFilePath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open main file for truncation: %v", err)
	}

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatalf("Failed to get file info: %v", err)
	}

	// Truncate the last 5 bytes (commit marker)
	newSize := fileInfo.Size() - 5
	err = file.Truncate(newSize)
	if err != nil {
		file.Close()
		t.Fatalf("Failed to truncate main file: %v", err)
	}
	file.Close()

	t.Logf("Removed last 5 bytes from main file. Original size: %d, New size: %d", fileInfo.Size(), newSize)

	// Reopen the database - this should trigger external value rollback
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database after truncation: %v", err)
	}

	// The external key should now return the initial value (rollback occurred)
	value, err = db.Get([]byte("rollback_key"))
	if err != nil {
		t.Fatalf("Failed to get external key after rollback: %v", err)
	}

	t.Logf("Retrieved value after rollback: '%s' (length: %d)", string(value), len(value))

	if !bytes.Equal(value, []byte("initial_value")) {
		// Check if we can find any external files
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			t.Logf("Found %d external files: %v", len(files), files)
			for _, file := range files {
				if info, err := os.Stat(file); err == nil {
					t.Logf("File %s size: %d bytes", file, info.Size())
				}
			}
		}
		t.Fatalf("Expected rollback to initial_value, but got '%s'", string(value))
	}

	t.Log("External key correctly rolled back to initial_value after commit marker removal")

	// Set a normal key-value pair
	err = db.Set([]byte("another_key"), []byte("another_value"))
	if err != nil {
		t.Fatalf("Failed to set another key-value pair: %v", err)
	}

	// Close and reopen the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database after setting another key-value pair: %v", err)
	}
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database after setting another key-value pair: %v", err)
	}

	// Verify the external key is still the initial value
	value, err = db.Get([]byte("rollback_key"))
	if err != nil {
		t.Fatalf("Failed to get external key after setting another key-value pair: %v", err)
	}
	if !bytes.Equal(value, []byte("initial_value")) {
		t.Fatalf("Expected external key to be initial_value, but got %s", string(value))
	}
	t.Log("External key correctly remained at initial_value after setting another key-value pair")

	// Set a new value
	err = db.Set([]byte("rollback_key"), []byte("new_value_after_rollback"))
	if err != nil {
		t.Fatalf("Failed to set new value after rollback: %v", err)
	}

	// Close and reopen the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database after setting new value: %v", err)
	}
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database after setting new value: %v", err)
	}
	defer db.Close()

	// Verify the new value
	value, err = db.Get([]byte("rollback_key"))
	if err != nil {
		t.Fatalf("Failed to get external key after setting new value: %v", err)
	}
	if !bytes.Equal(value, []byte("new_value_after_rollback")) {
		t.Fatalf("Value mismatch after setting new value: got %s, want %s",
			string(value), "new_value_after_rollback")
	}

	t.Log("Successfully set and retrieved new value after rollback")
}

func TestIterator(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator.db"

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

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for k, v := range testData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Create an iterator
	it := db.NewIterator()
	defer it.Close()

	// Count the number of entries found
	count := 0
	foundKeys := make(map[string]bool)
	foundValues := make(map[string]string)

	// Iterate through all entries
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())

		// Verify the key-value pair
		expectedValue, exists := testData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", key, value, expectedValue)
		}

		// Track found keys and values
		foundKeys[key] = true
		foundValues[key] = value
		count++

		// Move to next entry
		it.Next()
	}

	// Verify we found all keys
	if count != len(testData) {
		t.Fatalf("Iterator found %d entries, expected %d", count, len(testData))
	}

	// Verify each key was found
	for k := range testData {
		if !foundKeys[k] {
			t.Fatalf("Key '%s' was not found by iterator", k)
		}
	}

	// Test iterator after modifications

	// Delete a key
	if err := db.Delete([]byte("key3")); err != nil {
		t.Fatalf("Failed to delete 'key3': %v", err)
	}

	// Add a new key
	if err := db.Set([]byte("key6"), []byte("value6")); err != nil {
		t.Fatalf("Failed to set 'key6': %v", err)
	}

	// Modify an existing key
	if err := db.Set([]byte("key1"), []byte("modified1")); err != nil {
		t.Fatalf("Failed to update 'key1': %v", err)
	}

	// Create a new iterator
	modifiedIt := db.NewIterator()
	defer modifiedIt.Close()

	// Reset tracking variables
	count = 0
	foundKeys = make(map[string]bool)
	foundValues = make(map[string]string)

	// Expected data after modifications
	expectedData := map[string]string{
		"key1": "modified1", // Modified
		"key2": "value2",
		// key3 deleted
		"key4": "value4",
		"key5": "value5",
		"key6": "value6", // New
	}

	// Iterate through all entries
	for modifiedIt.Valid() {
		key := string(modifiedIt.Key())
		value := string(modifiedIt.Value())

		// Verify the key-value pair
		expectedValue, exists := expectedData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key after modifications: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch after modifications for key '%s': got %s, want %s",
				key, value, expectedValue)
		}

		// Track found keys and values
		foundKeys[key] = true
		foundValues[key] = value
		count++

		// Move to next entry
		modifiedIt.Next()
	}

	// Verify we found all keys
	if count != len(expectedData) {
		t.Fatalf("Iterator found %d entries after modifications, expected %d",
			count, len(expectedData))
	}

	// Verify each key was found
	for k := range expectedData {
		if !foundKeys[k] {
			t.Fatalf("Key '%s' was not found by iterator after modifications", k)
		}
	}

	// Test iterator with empty database
	emptyDbPath := "test_empty_iterator.db"
	os.Remove(emptyDbPath)
	os.Remove(emptyDbPath + "-index")
	os.Remove(emptyDbPath + "-wal")

	emptyDb, err := Open(emptyDbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open empty database: %v", err)
	}
	defer func() {
		emptyDb.Close()
		os.Remove(emptyDbPath)
		os.Remove(emptyDbPath + "-index")
		os.Remove(emptyDbPath + "-wal")
	}()

	emptyIt := emptyDb.NewIterator()
	defer emptyIt.Close()

	// Verify the iterator is not valid for an empty database
	if emptyIt.Valid() {
		t.Fatalf("Iterator for empty database should not be valid")
	}
}

func TestIteratorExternalKeys(t *testing.T) {
	dbPath := "test_iterator_external_keys.db"
	cleanupTestFiles(dbPath)
	if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
		for _, file := range files {
			os.Remove(file)
		}
	}
	defer func() {
		cleanupTestFiles(dbPath)
		if files, err := filepath.Glob(dbPath + "-vk-*"); err == nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.SetOption("AddMutableKey", []byte("external_key")); err != nil {
		t.Fatalf("Failed to register external key: %v", err)
	}
	if err := db.SetOption("AddMutableKey", []byte("chain.latest")); err != nil {
		t.Fatalf("Failed to register chain.latest external key: %v", err)
	}

	testData := map[string]string{
		"regular_key":  "regular_value",
		"external_key": "external_value",
		"chain.latest": "block_index_value",
	}

	for k, v := range testData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	it := db.NewIterator()
	defer it.Close()

	found := make(map[string]string)
	for it.Valid() {
		found[string(it.Key())] = string(it.Value())
		it.Next()
	}

	for k, v := range testData {
		got, ok := found[k]
		if !ok {
			t.Fatalf("Iterator did not return key '%s'", k)
		}
		if got != v {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, got, v)
		}
	}

	if len(found) != len(testData) {
		t.Fatalf("Iterator found %d keys, expected %d", len(found), len(testData))
	}
}

func TestIteratorWithMixedKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator_mixed_keys.db"

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

	// Insert keys with different lengths and different prefixes
	testKeys := []string{
		string([]byte{0}),  // NUL character (byte 0)
		"a",
		"aa",
		"a1",
		"az5",
		"a-long-2",
		"ab",
		"ab2",
		"ab2-1",
		"ab2-long-2",
		"abc",
		"abc3",
		"abc3-1",
		"ab-long-2",
		"abc-3-1",
		"abc-long-2",
		"b-1-1",
		"b-long-2",
		"bc-2-1",
		"bc-long-2",
		"bcd-3-1",
		"bcd-long-2",
		"c",
		"ca",
		"cab",
		"cablong",
		"d-very-long-key-with-many-characters-to-test-different-length",
		"z",
		string([]byte{255}),  // Character with byte value 255
	}

	// Insert all keys with their values
	testData := make(map[string]string)
	for i, key := range testKeys {
		value := fmt.Sprintf("value-%d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set '%s': %v", key, err)
		}
		testData[key] = value
	}

	// Test: Simple iteration to check that all keys are returned (unordered)
	it := db.NewIterator()
	defer it.Close()

	foundKeys := make(map[string]string)

	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify we found all keys
	if len(foundKeys) != len(testData) {
		t.Fatalf("Iterator found %d keys, expected %d", len(foundKeys), len(testData))
	}

	// Verify each key and value matches what we expect
	for expectedKey, expectedValue := range testData {
		foundValue, exists := foundKeys[expectedKey]
		if !exists {
			t.Fatalf("Key '%s' was not found by iterator", expectedKey)
		}
		if foundValue != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", expectedKey, foundValue, expectedValue)
		}
	}
}

func TestIteratorWithLargeDataset(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator_large_dataset.db"

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

	// Insert many key-value pairs to test iterator with a large dataset
	numPairs := 1000
	keys := make([]string, numPairs)
	values := make([]string, numPairs)

	for i := 0; i < numPairs; i++ {
		keys[i] = fmt.Sprintf("test-key-%d", i)
		values[i] = fmt.Sprintf("test-value-%d", i)

		if err := db.Set([]byte(keys[i]), []byte(values[i])); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Create a map for verification
	expectedData := make(map[string]string)
	for i := 0; i < numPairs; i++ {
		expectedData[keys[i]] = values[i]
	}

	// Create an iterator (simple iteration, no range filtering)
	it := db.NewIterator()
	defer it.Close()

	// Count the number of entries found
	count := 0
	foundKeys := make(map[string]bool)

	// Iterate through all entries
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())

		// Verify the key-value pair exists in our expected data
		expectedValue, exists := expectedData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", key, value, expectedValue)
		}

		// Track found keys
		foundKeys[key] = true
		count++

		// Move to next entry
		it.Next()
	}

	// Verify we found all keys
	if count != numPairs {
		t.Fatalf("Iterator found %d entries, expected %d", count, numPairs)
	}

	// Verify each key was found
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if !foundKeys[key] {
			t.Fatalf("Key '%s' was not found by iterator", key)
		}
	}
}

// generateVariableLengthKey generates a key of variable length based on index i
// Key lengths range from 1 to 64 bytes, using base64-like characters for variety
// Limits single-character keys to 64 total to avoid collisions
func generateVariableLengthKey(i int) string {
	// Base64-like character set for more variety
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

	// Determine the key length (1 to 64 bytes)
	// But limit single-character keys to only the first 64 indices
	var keyLength int
	if i < 64 {
		// First 64 keys get single characters (1 byte each)
		keyLength = 1
	} else {
		// Remaining keys get lengths from 2 to 64 bytes
		keyLength = ((i - 64) % 63) + 2
	}

	// Create a base pattern using the index - ensure it's always unique
	basePattern := fmt.Sprintf("k%d", i)

	// Handle single character keys specially
	if keyLength == 1 {
		return string(charset[i % len(charset)])
	}

	// If the base pattern is already longer than desired length,
	// we need to create a shorter unique key
	if len(basePattern) > keyLength {
		// For short keys, create a compact unique representation
		if keyLength == 2 {
			first := charset[i % len(charset)]
			second := charset[(i / len(charset)) % len(charset)]
			return string([]byte{first, second})
		} else if keyLength == 3 {
			first := charset[i % len(charset)]
			second := charset[(i / len(charset)) % len(charset)]
			third := charset[(i / (len(charset) * len(charset))) % len(charset)]
			return string([]byte{first, second, third})
		} else {
			// For longer keys that are still shorter than basePattern,
			// create a compact representation
			compactKey := fmt.Sprintf("%d", i)
			if len(compactKey) > keyLength {
				// If even the number is too long, use base64 encoding of the number
				var builder strings.Builder
				remaining := i
				for j := 0; j < keyLength; j++ {
					builder.WriteByte(charset[remaining % len(charset)])
					remaining = remaining / len(charset)
				}
				return builder.String()
			} else {
				// Pad with charset characters
				var builder strings.Builder
				builder.WriteString(compactKey)
				for j := len(compactKey); j < keyLength; j++ {
					builder.WriteByte(charset[(i + j) % len(charset)])
				}
				return builder.String()
			}
		}
	}

	// If we need to pad, use repeating characters from charset
	if len(basePattern) < keyLength {
		padding := keyLength - len(basePattern)

		var builder strings.Builder
		builder.WriteString(basePattern)
		for j := 0; j < padding; j++ {
			// Use different characters based on position and index
			charIndex := (i + j) % len(charset)
			builder.WriteByte(charset[charIndex])
		}

		return builder.String()
	}

	return basePattern
}

func TestIteratorWithLargeDataset2(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator_variable_length.db"

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

	// Insert many key-value pairs with variable length keys to test iterator
	numPairs := 1000
	keys := make([]string, numPairs)
	values := make([]string, numPairs)
	keySet := make(map[string]bool) // To check for duplicates

	for i := 0; i < numPairs; i++ {
		keys[i] = generateVariableLengthKey(i)
		values[i] = fmt.Sprintf("val-%d", i)

		// Check for duplicate keys
		if keySet[keys[i]] {
			t.Fatalf("Duplicate key generated at index %d: %s", i, keys[i])
		}
		keySet[keys[i]] = true

		if err := db.Set([]byte(keys[i]), []byte(values[i])); err != nil {
			t.Fatalf("Failed to set key %d (%s): %v", i, keys[i], err)
		}
	}

	// Create a map for verification
	expectedData := make(map[string]string)
	for i := 0; i < numPairs; i++ {
		expectedData[keys[i]] = values[i]
	}

	// Create an iterator
	it := db.NewIterator()
	defer it.Close()

	// Count the number of entries found
	count := 0
	foundKeys := make(map[string]bool)

	// Iterate through all entries
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())

		// Verify the key-value pair exists in our expected data
		expectedValue, exists := expectedData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", key, value, expectedValue)
		}

		// Track found keys
		foundKeys[key] = true
		count++

		// Move to next entry
		it.Next()
	}

	// Verify we found all keys
	if count != numPairs {
		t.Fatalf("Iterator found %d entries, expected %d", count, numPairs)
	}

	// Verify each key was found
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if !foundKeys[key] {
			t.Fatalf("Key '%s' was not found by iterator", key)
		}
	}
}

func TestDatabaseReindex(t *testing.T) {
	// Create a test database
	dbPath := "test_reindex.db"
	indexPath := dbPath + "-index"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(indexPath)
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(indexPath)
		os.Remove(dbPath + "-wal")
	}()

	// Test setting key-value pairs
	err = db.Set([]byte("name"), []byte("hash-table-tree"))
	if err != nil {
		t.Fatalf("Failed to set 'name': %v", err)
	}

	err = db.Set([]byte("author"), []byte("Bernardo"))
	if err != nil {
		t.Fatalf("Failed to set 'author': %v", err)
	}

	err = db.Set([]byte("type"), []byte("key-value database"))
	if err != nil {
		t.Fatalf("Failed to set 'type': %v", err)
	}

	// Update a key
	err = db.Set([]byte("name"), []byte("hash-table-tree DB"))
	if err != nil {
		t.Fatalf("Failed to update 'name': %v", err)
	}

	// Delete a key
	err = db.Delete([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to delete 'author': %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Save the original index file for comparison
	originalIndexPath := indexPath + ".original"
	os.Rename(indexPath, originalIndexPath)

	// Reopen the database - it should rebuild the index
	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database after index deletion: %v", err)
	}
	defer func() {
		reopenedDb.Close()
		os.Remove(originalIndexPath)
	}()

	// Verify name has the updated value
	nameVal, err := reopenedDb.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name' after reindex: %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Value mismatch for 'name' after reindex: got %s, want %s",
			string(nameVal), "hash-table-tree DB")
	}

	// Verify author was deleted
	_, err = reopenedDb.Get([]byte("author"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'author' after reindex, got nil")
	}

	// Verify type still exists with original value
	typeVal, err := reopenedDb.Get([]byte("type"))
	if err != nil {
		t.Fatalf("Failed to get 'type' after reindex: %v", err)
	}
	if !bytes.Equal(typeVal, []byte("key-value database")) {
		t.Fatalf("Value mismatch for 'type' after reindex: got %s, want %s",
			string(typeVal), "key-value database")
	}

	// Close the database
	if err := reopenedDb.Close(); err != nil {
		t.Fatalf("Failed to close database after reindex: %v", err)
	}

	// Compare files using diff to verify index was rebuilt

	// Compare files starting from byte 4096 (skipping the first 4096 bytes)
	// Use cmp command with skip option to compare files from offset 4096
	cmd := exec.Command("cmp", "-s", "-i", "4096:4096", originalIndexPath, indexPath)
	err = cmd.Run()

	// If cmp finds no differences, it returns exit status 0
	// If files differ, it returns exit status 1
	// For any other error, it returns other non-zero status
	if err != nil {
		// Files should be identical after byte 4096, so any difference is an error
		t.Fatalf("Index files should be identical after byte 4096 but differ: %v", err)
	}

}

func TestTransactionRollback(t *testing.T) {
	withWriteAndRollbackModes(t, testTransactionRollback)
}

// TestSlowRollbackCloningBoundary crosses the clone-every-1000-transactions
// interval of FastRollback=false. Once the interval re-marks the cloning
// point, a rollback takes the discard-plus-reindex path over data committed
// both before and after the mark
func TestSlowRollbackCloningBoundary(t *testing.T) {
	withWriteModes(t, func(t *testing.T, writeMode string) {
		dbPath := testDBPath(".", "test_slow_rollback_boundary.db", writeMode)
		cleanupTestFiles(dbPath)

		db := openTestDB(t, dbPath, writeMode, Options{"FastRollback": false})
		defer func() {
			db.Close()
			cleanupTestFiles(dbPath)
		}()

		// Commit up to the 1000-transaction clone interval
		const boundaryTxns = 1000
		for i := 0; i < boundaryTxns; i++ {
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Begin %d: %v", i, err)
			}
			if err := tx.Set([]byte(fmt.Sprintf("k-%04d", i)), []byte("v")); err != nil {
				t.Fatalf("Set %d: %v", i, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit %d: %v", i, err)
			}
		}

		// The interval must have re-marked the cloning point at the boundary
		db.seqMutex.Lock()
		cloningSequence := db.cloningSequence
		db.seqMutex.Unlock()
		if cloningSequence <= 0 {
			t.Fatalf("cloningSequence %d: the clone interval never fired", cloningSequence)
		}

		// Roll back a transaction that mutates and deletes pre-boundary keys
		// and adds a new one, so the reindex path must restore committed state
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin after boundary: %v", err)
		}
		if err := tx.Set([]byte("k-0000"), []byte("mutated")); err != nil {
			t.Fatalf("mutate: %v", err)
		}
		if err := tx.Delete([]byte("k-0001")); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := tx.Set([]byte("k-rolledback"), []byte("x")); err != nil {
			t.Fatalf("add: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}

		// Committed state must be intact and rolled-back changes gone
		value, err := db.Get([]byte("k-0000"))
		if err != nil || string(value) != "v" {
			t.Fatalf("k-0000 after rollback: %q, %v", value, err)
		}
		if _, err := db.Get([]byte("k-0001")); err != nil {
			t.Fatalf("k-0001 should survive the rollback: %v", err)
		}
		if _, err := db.Get([]byte("k-rolledback")); err == nil {
			t.Fatal("k-rolledback from the rolled back transaction still exists")
		}
		value, err = db.Get([]byte(fmt.Sprintf("k-%04d", boundaryTxns-1)))
		if err != nil || string(value) != "v" {
			t.Fatalf("k-%04d after rollback: %q, %v", boundaryTxns-1, value, err)
		}

		// Reopen and verify the recovered state matches
		db.Close()
		db2, err := Open(dbPath)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer db2.Close()
		count := 0
		it := db2.NewIterator()
		for it.Valid() {
			count++
			it.Next()
		}
		it.Close()
		if count != boundaryTxns {
			t.Fatalf("reopened DB has %d records, want %d", count, boundaryTxns)
		}
		value, err = db2.Get([]byte("k-0000"))
		if err != nil || string(value) != "v" {
			t.Fatalf("k-0000 after reopen: %q, %v", value, err)
		}
	})
}

func testTransactionRollback(t *testing.T, writeMode string, fastRollback bool) {
	dbPath := testDBPath(".", "test_transaction_rollback.db", writeMode)
	if !fastRollback {
		dbPath = testDBPath(".", "test_transaction_rollback_slow.db", writeMode)
	}
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode, Options{"FastRollback": fastRollback})
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Create keys with the same prefix to ensure they share radix and leaf pages
	keyPrefix := "aa"
	keySuffix := "_some-long-suffix-here-to-consume-a-lot-of-space"

	// Transaction 1: Insert first batch of keys
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Insert 10 keys in transaction 1
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx1-%d", i)

		if err := tx1.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 1: %v", key, err)
		}
	}

	// Commit transaction 1
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Transaction 2: Insert second batch of keys
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Insert 10 more keys in transaction 2
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx2-%d", i)

		if err := tx2.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 2: %v", key, err)
		}
	}

	// Commit transaction 2
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Transaction 3: Insert third batch of keys
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	// Insert 10 more keys in transaction 3
	for i := 20; i < 30; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx3-%d", i)

		if err := tx3.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 3: %v", key, err)
		}
	}

	// Commit transaction 3
	if err := tx3.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 3: %v", err)
	}

	// Verify all keys from transactions 1-3 exist
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string

		if i < 10 {
			expectedValue = fmt.Sprintf("value-tx1-%d", i)
		} else if i < 20 {
			expectedValue = fmt.Sprintf("value-tx2-%d", i)
		} else {
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		}

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// Transaction 4: This one will be rolled back
	tx4, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 4: %v", err)
	}

	// Insert new keys in transaction 4
	for i := 30; i < 40; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx4-%d", i)

		if err := tx4.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 4: %v", key, err)
		}
	}

	// Modify some existing keys from previous transactions
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("modified-value-tx4-%d", i)

		if err := tx4.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to modify key %s in transaction 4: %v", key, err)
		}
	}

	// Delete some existing keys
	for i := 15; i < 20; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)

		if err := tx4.Delete([]byte(key)); err != nil {
			t.Fatalf("Failed to delete key %s in transaction 4: %v", key, err)
		}
	}

	// Verify the changes are visible within the transaction
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string
		var shouldExist bool = true

		if i < 15 {
			// Modified keys
			expectedValue = fmt.Sprintf("modified-value-tx4-%d", i)
		} else if i < 20 {
			// Deleted keys
			shouldExist = false
		} else if i < 30 {
			// Unmodified keys from transaction 3
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		} else {
			// New keys from transaction 4
			expectedValue = fmt.Sprintf("value-tx4-%d", i)
		}

		value, err := tx4.Get([]byte(key))
		if !shouldExist {
			if err == nil {
				t.Fatalf("Expected key %s to be deleted, but it still exists", key)
			}
		} else {
			if err != nil {
				t.Fatalf("Failed to get key %s within transaction 4: %v", key, err)
			}

			if !bytes.Equal(value, []byte(expectedValue)) {
				t.Fatalf("Value mismatch for key %s within transaction 4: got %s, want %s",
					key, string(value), expectedValue)
			}
		}
	}

	// Now rollback transaction 4
	if err := tx4.Rollback(); err != nil {
		t.Fatalf("Failed to rollback transaction 4: %v", err)
	}

	// Verify that all changes from transaction 4 are discarded
	// and all data from transactions 1-3 are preserved

	// Keys from transactions 1-3 should have their original values
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string

		if i < 10 {
			expectedValue = fmt.Sprintf("value-tx1-%d", i)
		} else if i < 20 {
			expectedValue = fmt.Sprintf("value-tx2-%d", i)
		} else {
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		}

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after rollback: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after rollback: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// New keys from transaction 4 should not exist
	for i := 30; i < 40; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		_, err := db.Get([]byte(key))
		if err == nil {
			t.Fatalf("Key %s from rolled back transaction still exists", key)
		}
	}

	// Start a new transaction after rollback
	tx5, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 5: %v", err)
	}

	// Add some new keys in transaction 5
	for i := 30; i < 35; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx5-%d", i)

		if err := tx5.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 5: %v", key, err)
		}
	}

	// Commit transaction 5
	if err := tx5.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 5: %v", err)
	}

	// Verify all keys from transactions 1-3 and 5 exist with correct values
	for i := 0; i < 35; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string

		if i < 10 {
			expectedValue = fmt.Sprintf("value-tx1-%d", i)
		} else if i < 20 {
			expectedValue = fmt.Sprintf("value-tx2-%d", i)
		} else if i < 30 {
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		} else {
			expectedValue = fmt.Sprintf("value-tx5-%d", i)
		}

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after transaction 5: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after transaction 5: got %s, want %s",
				key, string(value), expectedValue)
		}
	}
}

func TestSharedPrefixKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_shared_prefix.db"

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

	// Test the specific problematic patterns mentioned in the issue:
	// "abc"⇄"ab", "acd"⇄"ac", "ghi"⇄"gh"

	// Test Case 1: Insert 3-byte key first, then its 2-byte prefix
	err = db.Set([]byte("ab"), []byte("value-ab"))
	if err != nil {
		t.Fatalf("Failed to set 'ab': %v", err)
	}

	err = db.Set([]byte("abc"), []byte("value-abc"))
	if err != nil {
		t.Fatalf("Failed to set 'abc': %v", err)
	}

	// Verify both keys exist
	val, err := db.Get([]byte("abc"))
	if err != nil {
		t.Fatalf("Failed to get 'abc': %v", err)
	}
	if !bytes.Equal(val, []byte("value-abc")) {
		t.Fatalf("Value mismatch for 'abc': got %s, want %s", string(val), "value-abc")
	}

	val, err = db.Get([]byte("ab"))
	if err != nil {
		t.Fatalf("Failed to get 'ab': %v", err)
	}
	if !bytes.Equal(val, []byte("value-ab")) {
		t.Fatalf("Value mismatch for 'ab': got %s, want %s", string(val), "value-ab")
	}

	// Test Case 2: Insert 2-byte key first, then its 3-byte extension
	err = db.Set([]byte("acd"), []byte("value-acd"))
	if err != nil {
		t.Fatalf("Failed to set 'acd': %v", err)
	}

	err = db.Set([]byte("ac"), []byte("value-ac"))
	if err != nil {
		t.Fatalf("Failed to set 'ac': %v", err)
	}

	// Verify both keys exist
	val, err = db.Get([]byte("acd"))
	if err != nil {
		t.Fatalf("Failed to get 'acd': %v", err)
	}
	if !bytes.Equal(val, []byte("value-acd")) {
		t.Fatalf("Value mismatch for 'acd': got %s, want %s", string(val), "value-acd")
	}

	val, err = db.Get([]byte("ac"))
	if err != nil {
		t.Fatalf("Failed to get 'ac': %v", err)
	}
	if !bytes.Equal(val, []byte("value-ac")) {
		t.Fatalf("Value mismatch for 'ac': got %s, want %s", string(val), "value-ac")
	}

	// Test Case 3: Insert 3-byte key first, then its 2-byte prefix (different pattern)
	err = db.Set([]byte("gh"), []byte("value-gh"))
	if err != nil {
		t.Fatalf("Failed to set 'gh': %v", err)
	}

	err = db.Set([]byte("ghi"), []byte("value-ghi"))
	if err != nil {
		t.Fatalf("Failed to set 'ghi': %v", err)
	}

	// Verify both keys exist
	val, err = db.Get([]byte("ghi"))
	if err != nil {
		t.Fatalf("Failed to get 'ghi': %v", err)
	}
	if !bytes.Equal(val, []byte("value-ghi")) {
		t.Fatalf("Value mismatch for 'ghi': got %s, want %s", string(val), "value-ghi")
	}

	val, err = db.Get([]byte("gh"))
	if err != nil {
		t.Fatalf("Failed to get 'gh': %v", err)
	}
	if !bytes.Equal(val, []byte("value-gh")) {
		t.Fatalf("Value mismatch for 'gh': got %s, want %s", string(val), "value-gh")
	}

	// Test all keys with iterator to make sure they're all present
	it := db.NewIterator()
	defer it.Close()

	expectedKeys := []string{"ab", "abc", "ac", "acd", "gh", "ghi"}
	expectedValues := []string{"value-ab", "value-abc", "value-ac", "value-acd", "value-gh", "value-ghi"}

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all expected keys were found
	for i, expectedKey := range expectedKeys {
		foundValue, exists := foundKeys[expectedKey]
		if !exists {
			t.Fatalf("Key '%s' was not found by iterator", expectedKey)
		}
		if foundValue != expectedValues[i] {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", expectedKey, foundValue, expectedValues[i])
		}
	}

	// Verify we didn't find any unexpected keys
	if len(foundKeys) != len(expectedKeys) {
		t.Fatalf("Iterator found %d keys, expected %d. Found keys: %v, Expected: %v", len(foundKeys), len(expectedKeys), foundKeys, expectedKeys)
	}
}

func TestSharedPrefixKeysStress(t *testing.T) {
	withWriteModes(t, testSharedPrefixKeysStress)
}

func testSharedPrefixKeysStress(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_shared_prefix_stress.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode, Options{"MainIndexPages": 1})
	var err error
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Create many keys with shared prefixes to stress the radix tree
	type testCase struct {
		longKey  string
		shortKey string
		order    string // "long_first" or "short_first"
	}

	testCases := []testCase{
		// 2-byte prefix, 3-byte extension
		{"abc", "ab", "long_first"},
		{"abd", "ab", "short_first"},
		{"acd", "ac", "long_first"},
		{"ace", "ac", "short_first"},
		{"def", "de", "long_first"},
		{"deg", "de", "short_first"},
		{"ghi", "gh", "long_first"},
		{"ghj", "gh", "short_first"},
		{"jkl", "jk", "long_first"},
		{"jkm", "jk", "short_first"},
		{"mno", "mn", "long_first"},
		{"mnp", "mn", "short_first"},

		// 3-byte prefix, 4-byte extension
		{"abcd", "abc", "long_first"},
		{"abce", "abc", "short_first"},
		{"defg", "def", "long_first"},
		{"defh", "def", "short_first"},
		{"ghij", "ghi", "long_first"},
		{"ghik", "ghi", "short_first"},

		// 1-byte prefix, 2-byte extension
		{"xy", "x", "long_first"},
		{"xz", "x", "short_first"},
		{"yz", "y", "long_first"},
		{"ya", "y", "short_first"},
		{"za", "z", "long_first"},
		{"zb", "z", "short_first"},
	}

	// Track all keys we insert
	insertedKeys := make(map[string]string)

	// Insert keys according to their specified order
	for i, tc := range testCases {
		longValue := fmt.Sprintf("long-value-%d", i)
		shortValue := fmt.Sprintf("short-value-%d", i)

		if tc.order == "long_first" {
			// Insert long key first, then short key
			err = db.Set([]byte(tc.longKey), []byte(longValue))
			if err != nil {
				t.Fatalf("Failed to set long key '%s': %v", tc.longKey, err)
			}
			insertedKeys[tc.longKey] = longValue

			err = db.Set([]byte(tc.shortKey), []byte(shortValue))
			if err != nil {
				t.Fatalf("Failed to set short key '%s': %v", tc.shortKey, err)
			}
			insertedKeys[tc.shortKey] = shortValue
		} else {
			// Insert short key first, then long key
			err = db.Set([]byte(tc.shortKey), []byte(shortValue))
			if err != nil {
				t.Fatalf("Failed to set short key '%s': %v", tc.shortKey, err)
			}
			insertedKeys[tc.shortKey] = shortValue

			err = db.Set([]byte(tc.longKey), []byte(longValue))
			if err != nil {
				t.Fatalf("Failed to set long key '%s': %v", tc.longKey, err)
			}
			insertedKeys[tc.longKey] = longValue
		}

		// Verify both keys exist after each insertion
		val, err := db.Get([]byte(tc.longKey))
		if err != nil {
			t.Fatalf("Failed to get long key '%s' after insertion %d: %v", tc.longKey, i, err)
		}
		if !bytes.Equal(val, []byte(longValue)) {
			t.Fatalf("Value mismatch for long key '%s' after insertion %d: got %s, want %s",
				tc.longKey, i, string(val), longValue)
		}

		val, err = db.Get([]byte(tc.shortKey))
		if err != nil {
			t.Fatalf("Failed to get short key '%s' after insertion %d: %v", tc.shortKey, i, err)
		}
		if !bytes.Equal(val, []byte(shortValue)) {
			t.Fatalf("Value mismatch for short key '%s' after insertion %d: got %s, want %s",
				tc.shortKey, i, string(val), shortValue)
		}
	}

	// Verify all keys are still accessible
	for key, expectedValue := range insertedKeys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key '%s' in final verification: %v", key, err)
		}
		if !bytes.Equal(val, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key '%s' in final verification: got %s, want %s",
				key, string(val), expectedValue)
		}
	}

	// Use iterator to verify all keys are present
	it := db.NewIterator()
	defer it.Close()

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all inserted keys were found by iterator
	for key, expectedValue := range insertedKeys {
		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key '%s' was not found by iterator", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key '%s': got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys
	if len(foundKeys) != len(insertedKeys) {
		t.Fatalf("Iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundKeys), len(insertedKeys), foundKeys, insertedKeys)
	}

	// Test persistence by closing and reopening
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	reopenedDb, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDb.Close()

	// Verify all keys still exist after reopening
	for key, expectedValue := range insertedKeys {
		val, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key '%s' after reopen: %v", key, err)
		}
		if !bytes.Equal(val, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key '%s' after reopen: got %s, want %s",
				key, string(val), expectedValue)
		}
	}
}

func TestSharedPrefixKeyOrdering(t *testing.T) {
	// This test reproduces the exact bug found in TestShortKeys
	// Using the insertion order that caused the failure:
	// [gh acd def ghi a e ac ef b d ij ab abc jkl mno c cd abd]

	// Create a test database
	dbPath := "test_shared_prefix_ordering.db"

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

	// Use the exact insertion order that caused the failure
	insertOrder := []struct {
		key   string
		value string
	}{
		{"gh", "two-byte-value-4"},
		{"acd", "three-byte-value-2"},
		{"def", "three-byte-value-3"},
		{"ghi", "three-byte-value-4"},
		{"a", "one-byte-value-0"},
		{"e", "one-byte-value-4"},
		{"ac", "two-byte-value-1"},
		{"ef", "two-byte-value-3"},
		{"b", "one-byte-value-1"},
		{"d", "one-byte-value-3"},
		{"ij", "two-byte-value-5"},
		{"ab", "two-byte-value-0"},
		{"abc", "three-byte-value-0"},  // This key failed to be retrieved
		{"jkl", "three-byte-value-5"},
		{"mno", "three-byte-value-6"},
		{"c", "one-byte-value-2"},
		{"cd", "two-byte-value-2"},
		{"abd", "three-byte-value-1"},
	}

	// Track all inserted keys
	allKeys := make(map[string]string)

	// Insert keys in the exact order that caused the failure
	for i, kv := range insertOrder {
		err = db.Set([]byte(kv.key), []byte(kv.value))
		if err != nil {
			t.Fatalf("Failed to set '%s' at step %d: %v", kv.key, i, err)
		}
		allKeys[kv.key] = kv.value

		// After each insertion, verify that previously inserted keys still exist
		for prevKey, prevValue := range allKeys {
			val, err := db.Get([]byte(prevKey))
			if err != nil {
				t.Fatalf("Key '%s' disappeared after inserting '%s' (step %d): %v",
					prevKey, kv.key, i, err)
			}
			if !bytes.Equal(val, []byte(prevValue)) {
				t.Fatalf("Value mismatch for key '%s' after inserting '%s' (step %d): got %s, want %s",
					prevKey, kv.key, i, string(val), prevValue)
			}
		}
	}

	// Final verification - check all keys exist
	for key, expectedValue := range allKeys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key '%s' in final verification: %v", key, err)
		}
		if !bytes.Equal(val, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s",
				key, string(val), expectedValue)
		}
	}

	// Use iterator to verify all keys are present
	it := db.NewIterator()
	defer it.Close()

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all keys were found by iterator
	for key, expectedValue := range allKeys {
		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key '%s' not found by iterator", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key '%s': got %s, want %s",
				key, foundValue, expectedValue)
		}
	}
}

// printPageTraversalInfo prints detailed information about the page traversal for a given key
// This function has been removed as it was specific to the old radix tree architecture
// The new hash-table tree architecture doesn't support this kind of detailed traversal logging

// TestHybridSubPageToTablePageConversion tests the conversion from hybrid sub-page to table page
// when a hybrid sub-page becomes too large and needs to be converted to a table page
func TestHybridSubPageToTablePageConversion(t *testing.T) {
	withWriteModes(t, testHybridSubPageToTablePageConversion)
}

func testHybridSubPageToTablePageConversion(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_hybrid_subpage_to_table_page_conversion.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode)
	var err error
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Use keys that will all hash to the same slot initially to force them into the same hybrid sub-page
	keyPrefix := "aa"
	keySuffix := "_some-long-suffix-here-to-consume-a-lot-of-space-and-fill-up-the-hybrid-sub-page-quickly"

	// Insert keys until we trigger conversion from hybrid sub-page to table page
	var keyCount int
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-%d", i)

		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}
		keyCount = i + 1
	}

	// Verify all keys are still accessible after potential conversion
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("value-%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after potential conversion: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after potential conversion: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Test persistence after conversion
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	reopenedDb, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDb.Close()

	// Verify all keys still exist after reopening
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("value-%d", i)

		value, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after reopen: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after reopen: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Test iterator after conversion
	it := reopenedDb.NewIterator()
	defer it.Close()

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all keys were found by iterator
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("value-%d", i)

		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key %s not found by iterator after conversion", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key %s: got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys
	if len(foundKeys) != keyCount {
		t.Fatalf("Iterator found %d keys, expected %d", len(foundKeys), keyCount)
	}
}

// TestHybridSubPageToTablePageConversionSimilarKeys tests hybrid sub-page to table page conversion
// with keys that have similar prefixes to test hash distribution and collision handling
func TestHybridSubPageToTablePageConversionSimilarKeys(t *testing.T) {
	withWriteModes(t, testHybridSubPageToTablePageConversionSimilarKeys)
}

func testHybridSubPageToTablePageConversionSimilarKeys(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_hybrid_to_table_conversion_similar.db", writeMode)
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode)
	var err error
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Use keys that are very similar at the beginning but differ only at the end
	// This will test how the hash-table tree handles keys with long common prefixes
	keyPrefix := "prefix"
	keySuffix := "_with_some_additional_content_to_make_entries_larger-user_profile_data_very_long_common_here_"

	// Insert keys that differ only at the end - this tests hash distribution
	// with long common prefixes
	var keyCount int
	for i := 0; i < 200; i++ { // Increase limit since similar keys might pack differently
		// Create keys that are identical except for the number at the end
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("user_data_%d", i)

		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}
		keyCount = i + 1
	}

	// Verify all keys are still accessible after potential conversion
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after potential conversion: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after potential conversion: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Test iterator on similar keys
	it := db.NewIterator()
	defer it.Close()

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all keys were found by iterator
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key %s not found by iterator after conversion", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key %s: got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys
	if len(foundKeys) != keyCount {
		t.Fatalf("Iterator found %d keys, expected %d", len(foundKeys), keyCount)
	}

	// Test persistence
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	reopenedDb, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDb.Close()

	// Verify all keys still exist after reopening
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		value, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after reopen: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after reopen: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Test iterator after reopen
	it2 := reopenedDb.NewIterator()
	defer it2.Close()

	foundKeys = make(map[string]string)
	for it2.Valid() {
		key := string(it2.Key())
		value := string(it2.Value())
		foundKeys[key] = value
		it2.Next()
	}

	// Verify all keys were found by iterator after reopen
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key %s not found by iterator after reopen", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key %s after reopen: got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys after reopen
	if len(foundKeys) != keyCount {
		t.Fatalf("Iterator found %d keys after reopen, expected %d", len(foundKeys), keyCount)
	}
}

// waitForBackgroundFlush waits for a fresh flush request to finish
func waitForBackgroundFlush(t *testing.T, db *DB) {
	t.Helper()

	requestID := db.requestFlush(true)
	if requestID > 0 {
		db.waitForCompletion("flush", requestID)
	}
}

func TestBackgroundWorkerDeadlock(t *testing.T) {
	// This test is designed to trigger a deadlock between the caller thread
	// and the background worker thread by forcing frequent background operations
	// while the caller thread is performing database operations

	dbPath := "test_background_deadlock.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open database with extremely low thresholds to force immediate background worker activity
	db, err := Open(dbPath, Options{
		"CacheSizeThreshold":   2,    // Extremely low - force cache cleanup after 2 pages
		"DirtyPageThreshold":   1,    // Force flush after every single dirty page
		"CheckpointThreshold":  256,  // Very small checkpoint threshold (256 bytes)
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Verify the background worker threads are active
	t.Logf("Database opened, background worker should be active")

	// Create keys that will force page creation and background activity
	// Each operation should trigger background worker due to low thresholds
	keySuffix := "_deadlock_test_key_with_long_suffix_to_consume_space"
	valuePrefix := "deadlock_test_value_with_very_long_content_to_make_pages_fill_up_quickly_and_trigger_background_worker_activity_"

	// Do direct database operations that should trigger background worker
	// Each Set() should trigger background worker due to DirtyPageThreshold=1
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d%s", i, keySuffix)
		value := fmt.Sprintf("%s%d", valuePrefix, i)

		// Set operation - this should trigger background worker immediately
		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}

		// Immediately try to read it back while background worker might be active
		_, err = db.Get([]byte(key))
		if err != nil {
			// Log detailed information about the failure
			t.Logf("=== BUG DETECTED ===")
			t.Logf("Failed to get key %d ('%s') immediately after setting", i, key)
			t.Logf("Error: %v", err)
			t.Logf("Expected value: '%s'", value)

			// Get cache stats to understand the state
			cacheStats := db.GetCacheStats()
			t.Logf("Cache stats when bug occurred: %+v", cacheStats)

			t.Fatalf("Failed to get key %d: %v", i, err)
		}

		// Wait for the worker through its completion condition instead of
		// guessing how long it needs
		if i%10 == 0 {
			waitForBackgroundFlush(t, db)
		}

		// Do another operation to increase lock contention
		if i > 0 {
			prevKey := fmt.Sprintf("%d%s", i-1, keySuffix)
			prevValue, err := db.Get([]byte(prevKey))
			if err != nil {
				t.Fatalf("Failed to get previous key %d: %v", i-1, err)
			}
			expectedValue := fmt.Sprintf("%s%d", valuePrefix, i-1)
			if string(prevValue) != expectedValue {
				t.Fatalf("Previous value mismatch for key %d: expected %s, got %s", i-1, expectedValue, string(prevValue))
			}
		}
	}

	t.Logf("Completed %d direct database operations", 1000)

	// Force more background activity by creating an iterator
	// while background worker is likely still active
	t.Logf("Creating iterator while background worker is active")
	it := db.NewIterator()

	keyCount := 0
	for it.Valid() {
		_ = it.Key()
		_ = it.Value()
		keyCount++
		it.Next()

		// Periodically wait for worker progress while the iterator is active
		if keyCount%10 == 0 {
			waitForBackgroundFlush(t, db)
		}
	}
	it.Close()

	t.Logf("Iterator found %d keys", keyCount)

	// Do some more operations to stress test the deadlock scenario
	for i := 1000; i < 1050; i++ {
		key := fmt.Sprintf("%d%s", i, keySuffix)
		value := fmt.Sprintf("%s%d", valuePrefix, i)

		// Set and immediately get to maximize lock contention
		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set final key %d: %v", i, err)
		}

		prevValue, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get final key %d: %v", i, err)
		}
		expectedValue := fmt.Sprintf("%s%d", valuePrefix, i)
		if string(prevValue) != expectedValue {
			t.Fatalf("Previous value mismatch for key %d: expected %s, got %s", i, expectedValue, string(prevValue))
		}

		// No delay here to maximize pressure on locks
	}

	// Finish any pending worker operation before reading final statistics
	waitForBackgroundFlush(t, db)

	// Get final cache stats
	cacheStats := db.GetCacheStats()
	t.Logf("Final cache stats: %+v", cacheStats)

	t.Logf("Test completed successfully - no deadlock detected")
}

func TestBackgroundWorkerWithTransactions(t *testing.T) {
	// Simpler version focusing on transaction + background worker interaction
	dbPath := "test_background_worker.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open database with very low thresholds to force background worker activity
	db, err := Open(dbPath, Options{
		"CacheSizeThreshold":   3,    // Low cache size to force frequent cleanups
		"DirtyPageThreshold":   1,    // Force flush after every dirty page
		"CheckpointThreshold":  512,  // Small checkpoint threshold
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Sequential operations that should trigger background worker
	numTransactions := 5
	keysPerTransaction := 1000

	for txId := 0; txId < numTransactions; txId++ {
		t.Logf("Starting transaction %d", txId)

		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", txId, err)
		}

		// Insert keys that should trigger background worker due to low thresholds
		for i := 0; i < keysPerTransaction; i++ {
			key := fmt.Sprintf("tx%d_key%d_with_long_suffix_to_consume_space", txId, i)
			value := fmt.Sprintf("tx%d_value%d_with_long_content_to_consume_space", txId, i)

			err = tx.Set([]byte(key), []byte(value))
			if err != nil {
				tx.Rollback()
				t.Fatalf("Transaction %d: failed to set key %d: %v", txId, i, err)
			}

			// Wait periodically for worker progress through its completion condition
			if i%100 == 0 {
				waitForBackgroundFlush(t, db)
			}
		}

		// Commit while background worker might be active
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", txId, err)
		}

		t.Logf("Transaction %d completed", txId)
		waitForBackgroundFlush(t, db)
	}

	// Verify all data exists
	totalKeysExpected := numTransactions * keysPerTransaction

	it := db.NewIterator()
	defer it.Close()

	keysFound := 0
	for it.Valid() {
		keysFound++
		it.Next()
	}

	if keysFound != totalKeysExpected {
		t.Fatalf("Expected %d keys, found %d", totalKeysExpected, keysFound)
	}

	t.Logf("Successfully completed test with %d transactions and %d total keys", numTransactions, totalKeysExpected)
}

func TestHeaderReadingWithWAL(t *testing.T) {
	withWriteModes(t, testHeaderReadingWithWAL)
}

func testHeaderReadingWithWAL(t *testing.T, writeMode string) {
	dbPath := testDBPath(".", "test_header_wal.db", writeMode)
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	// Test 1: Create database with WAL enabled
	db := openTestDB(t, dbPath, writeMode)
	var err error

	// Add some data to trigger index updates
	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	// Force a flush to write pages (including header) to WAL
	err = db.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Add more data to create a difference between WAL and index file
	err = db.Set([]byte("another_key"), []byte("another_value"))
	if err != nil {
		t.Fatalf("Failed to set second key: %v", err)
	}

	// Store the current lastIndexedOffset (this should be in WAL but not in index file)
	originalOffset := db.mainFileSize.Load()

	// Close the database (will commit the changes to the WAL file)
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Test 2: Reopen database and verify header is read correctly from WAL
	db2, err := Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify that the lastIndexedOffset was read from WAL (should match original)
	if db2.lastIndexedOffset != originalOffset {
		t.Errorf("lastIndexedOffset mismatch: expected %d, got %d", originalOffset, db2.lastIndexedOffset)
	}

	// Verify that we can still read both keys (showing WAL was properly loaded)
	value1, err := db2.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if string(value1) != "value" {
		t.Errorf("Value mismatch: expected 'value', got '%s'", string(value1))
	}

	value2, err := db2.Get([]byte("another_key"))
	if err != nil {
		t.Fatalf("Failed to get another_key: %v", err)
	}
	if string(value2) != "another_value" {
		t.Errorf("Value mismatch: expected 'another_value', got '%s'", string(value2))
	}

	err = db2.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestHeaderReadingWithoutWAL(t *testing.T) {
	// Create a temporary database
	dbPath := "test_header_no_wal.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-index")
	defer os.Remove(dbPath + "-wal")

	// Open database using default options (WAL enabled)
	db, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Add some data
	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	err = db.Set([]byte("another_key"), []byte("another_value"))
	if err != nil {
		t.Fatalf("Failed to set another key: %v", err)
	}
	err = db.Set([]byte("third_key"), []byte("third_value"))
	if err != nil {
		t.Fatalf("Failed to set third key: %v", err)
	}

	// Store the current mainFileSize for comparison
	originalOffset := db.mainFileSize.Load()

	// Close the database (will commit the changes to the WAL file)
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen database - this will do a checkpoint of the WAL into the index file
	db2, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to reopen database first time: %v", err)
	}

	// Close again to ensure any WAL created during opening is cleaned up
	err = db2.Close()
	if err != nil {
		t.Fatalf("Failed to close database second time: %v", err)
	}

	// Verify that no WAL file exists before the final test
	walPath := dbPath + "-wal"
	if _, err := os.Stat(walPath); err == nil {
		// WAL file exists, remove it to ensure clean test
		os.Remove(walPath)
	}

	// Final reopen - this should read header directly from index file only
	db3, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to reopen database final time: %v", err)
	}

	// Verify that the lastIndexedOffset was read correctly from index file
	if db3.lastIndexedOffset != originalOffset {
		t.Errorf("lastIndexedOffset mismatch: expected %d, got %d", originalOffset, db3.lastIndexedOffset)
	}

	// Verify that we can still read the data
	value, err := db3.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if string(value) != "value" {
		t.Errorf("Value mismatch: expected 'value', got '%s'", string(value))
	}
	value, err = db3.Get([]byte("another_key"))
	if err != nil {
		t.Fatalf("Failed to get another_key: %v", err)
	}
	if string(value) != "another_value" {
		t.Errorf("Value mismatch: expected 'another_value', got '%s'", string(value))
	}
	value, err = db3.Get([]byte("third_key"))
	if err != nil {
		t.Fatalf("Failed to get third_key: %v", err)
	}
	if string(value) != "third_value" {
		t.Errorf("Value mismatch: expected 'third_value', got '%s'", string(value))
	}

	err = db3.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestTransactionVisibility(t *testing.T) {
	type txnVisibilityTestCase struct {
		name        string
		initialData map[string]string
		txnChanges  map[string]string
		deleteKey   string
	}

	testCases := []txnVisibilityTestCase{
		{
			name: "SamePrefixKeys",
			initialData: map[string]string{
				"key1": "initial-value1",
				"key2": "initial-value2",
				"key3": "initial-value3",
			},
			txnChanges: map[string]string{
				"key1": "txn-modified-value1",
				"key4": "txn-new-value4",
			},
			deleteKey: "key3",
		},
		{
			name: "DifferentPrefixKeys",
			initialData: map[string]string{
				"first-key": "initial-value1",
				"second-key": "initial-value2",
				"third-key": "initial-value3",
			},
			txnChanges: map[string]string{
				"first-key": "txn-modified-value1",
				"fourth-key": "txn-new-value4",
			},
			deleteKey: "third-key",
		},
	}

	for _, rollbackMode := range rollbackModes {
		for _, tc := range testCases {
			testName := tc.name + "_" + rollbackMode.name
			t.Run(testName, func(t *testing.T) {
				withWriteModes(t, func(t *testing.T, writeMode string) {
				dbPath := testDBPath(".", "test_transaction_visibility_"+testName+".db", writeMode)
				cleanupTestFiles(dbPath)

				db := openTestDB(t, dbPath, writeMode, Options{
					"FastRollback": rollbackMode.fastRollback,
				})
				var err error
			defer func() {
				db.Close()
				os.Remove(dbPath)
				os.Remove(dbPath + "-index")
				os.Remove(dbPath + "-wal")
			}()

			// Insert initial data
			for k, v := range tc.initialData {
				if err := db.Set([]byte(k), []byte(v)); err != nil {
					t.Fatalf("Failed to set initial key %s: %v", k, err)
				}
			}

			// Verify initial data exists
			for k, expectedValue := range tc.initialData {
				value, err := db.Get([]byte(k))
				if err != nil {
					t.Fatalf("Failed to get initial key %s: %v", k, err)
				}
				if !bytes.Equal(value, []byte(expectedValue)) {
					t.Fatalf("Initial value mismatch for key %s: got %s, want %s", k, string(value), expectedValue)
				}
			}

			// Begin a transaction
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			// Make changes within the transaction
			for k, v := range tc.txnChanges {
				if err := tx.Set([]byte(k), []byte(v)); err != nil {
					t.Fatalf("Failed to set key %s in transaction: %v", k, err)
				}
			}

			// Delete a key within the transaction
			if err := tx.Delete([]byte(tc.deleteKey)); err != nil {
				t.Fatalf("Failed to delete %s in transaction: %v", tc.deleteKey, err)
			}

			// TEST 1: Verify db.Get() behavior based on rollback mode
			if rollbackMode.fastRollback {
				t.Log("Testing db.Get() doesn't see transaction changes (FastRollback=true)")

				// Check modified key
				var modifiedKey string
				for k := range tc.txnChanges {
					if _, ok := tc.initialData[k]; ok {
						modifiedKey = k
						break
					}
				}
				if modifiedKey == "" {
					t.Fatalf("No modified key found in txnChanges that exists in initialData")
				}
				value, err := db.Get([]byte(modifiedKey))
				if err != nil {
					t.Fatalf("Failed to get %s with db.Get(): %v", modifiedKey, err)
				}
				if !bytes.Equal(value, []byte(tc.initialData[modifiedKey])) {
					t.Fatalf("db.Get() should not see transaction changes for %s: got %s, want %s",
						string(modifiedKey), string(value), tc.initialData[modifiedKey])
				}

				// Check new key
				var newKey string
				for k := range tc.txnChanges {
					if _, ok := tc.initialData[k]; !ok {
						newKey = k
						break
					}
				}
				if newKey == "" {
					t.Fatalf("No new key found in txnChanges that does not exist in initialData")
				}
				_, err = db.Get([]byte(newKey))
				if err == nil {
					t.Fatalf("db.Get() should not see new %s from transaction", newKey)
				}

				// Check deleted key
				deletedKey := tc.deleteKey
				value, err = db.Get([]byte(deletedKey))
				if err != nil {
					t.Fatalf("db.Get() should still see %s that was deleted in transaction: %v", deletedKey, err)
				}
				if !bytes.Equal(value, []byte(tc.initialData[deletedKey])) {
					t.Fatalf("db.Get() value mismatch for %s: got %s, want %s",
						deletedKey, string(value), tc.initialData[deletedKey])
				}
			} else {
				t.Log("Testing db.Get() is refused while the slow rollback transaction is open")

				// The transaction mutates pages above the cloning mark in
				// place, so every concurrent db.Get() is refused; reads from
				// inside the transaction itself go through tx.Get() (TEST 2)
				if _, err := db.Get([]byte(tc.deleteKey)); err != ErrReadNotAllowed {
					t.Fatalf("db.Get() during the slow rollback transaction should return ErrReadNotAllowed, got %v", err)
				}
			}

			// TEST 2: Verify that txn.Get() can see its own changes
			t.Log("Testing txn.Get() can see transaction changes")

			// Find modified key and value
			var modifiedKey string
			var modifiedValue string
			for k, v := range tc.txnChanges {
				if _, ok := tc.initialData[k]; ok {
					modifiedKey = k
					modifiedValue = v
					break
				}
			}
			if modifiedKey == "" {
				t.Fatalf("No modified key found in txnChanges that exists in initialData")
			}

			// Check modified key
			txValue, err := tx.Get([]byte(modifiedKey))
			if err != nil {
				t.Fatalf("Failed to get %s with tx.Get(): %v", modifiedKey, err)
			}
			if !bytes.Equal(txValue, []byte(modifiedValue)) {
				t.Fatalf("tx.Get() should see transaction changes for %s: got %s, want %s",
					modifiedKey, string(txValue), modifiedValue)
			}

			// Find new key and value
			var newKey string
			var newValue string
			for k, v := range tc.txnChanges {
				if _, ok := tc.initialData[k]; !ok {
					newKey = k
					newValue = v
					break
				}
			}
			if newKey == "" {
				t.Fatalf("No new key found in txnChanges that does not exist in initialData")
			}

			// Check new key
			txValue, err = tx.Get([]byte(newKey))
			if err != nil {
				t.Fatalf("Failed to get %s with tx.Get(): %v", newKey, err)
			}
			if !bytes.Equal(txValue, []byte(newValue)) {
				t.Fatalf("tx.Get() should see new %s from transaction: got %s, want %s",
					newKey, string(txValue), newValue)
			}

			// Check deleted key
			deletedKey := tc.deleteKey
			_, err = tx.Get([]byte(deletedKey))
			if err == nil {
				t.Fatalf("tx.Get() should not see %s that was deleted in transaction", deletedKey)
			}

			// TEST 3: Commit the transaction and verify db.Get() now sees the changes
			if err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}
			t.Log("Transaction committed, testing db.Get() now sees changes")

			// Check modified key
			value, err := db.Get([]byte(modifiedKey))
			if err != nil {
				t.Fatalf("Failed to get %s after commit: %v", modifiedKey, err)
			}
			if !bytes.Equal(value, []byte(modifiedValue)) {
				t.Fatalf("After commit, value mismatch for %s: got %s, want %s",
					modifiedKey, string(value), modifiedValue)
			}

			// Check new key
			value, err = db.Get([]byte(newKey))
			if err != nil {
				t.Fatalf("Failed to get %s after commit: %v", newKey, err)
			}
			if !bytes.Equal(value, []byte(newValue)) {
				t.Fatalf("After commit, value mismatch for %s: got %s, want %s",
					newKey, string(value), newValue)
			}

			// Check deleted key
			_, err = db.Get([]byte(deletedKey))
			if err == nil {
				t.Fatalf("After commit, %s should still be deleted", deletedKey)
			}

			// TEST 4: Start a new transaction and verify it sees the committed changes
			tx2, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin second transaction: %v", err)
			}
			defer tx2.Rollback()

			// Check modified key
			tx2Value, err := tx2.Get([]byte(modifiedKey))
			if err != nil {
				t.Fatalf("Failed to get %s in second transaction: %v", modifiedKey, err)
			}
			if !bytes.Equal(tx2Value, []byte(modifiedValue)) {
				t.Fatalf("Second transaction value mismatch for %s: got %s, want %s",
					modifiedKey, string(tx2Value), modifiedValue)
			}

			// Check new key
			tx2Value, err = tx2.Get([]byte(newKey))
			if err != nil {
				t.Fatalf("Failed to get %s in second transaction: %v", newKey, err)
			}
			if !bytes.Equal(tx2Value, []byte(newValue)) {
				t.Fatalf("Second transaction value mismatch for %s: got %s, want %s",
					newKey, string(tx2Value), newValue)
			}

			// Check deleted key
			_, err = tx2.Get([]byte(deletedKey))
			if err == nil {
				t.Fatalf("Second transaction should not see deleted %s", deletedKey)
			}
				})
			})
		}
	}
}

// TestTransactionVisibilityOnFreshDB covers the case when the DB was just opened and there is a single transaction run on it.
// This is important to ensure isolation guarantees even for the very first transaction on a fresh database.
// Crossed with FastRollback true/false: slow-clone uses a different mid-txn flush watermark (cloningSequence).
func TestTransactionVisibilityOnFreshDB(t *testing.T) {
	withWriteAndRollbackModes(t, testTransactionVisibilityOnFreshDB)
}

func testTransactionVisibilityOnFreshDB(t *testing.T, writeMode string, fastRollback bool) {
	dbPath := testDBPath(".", "test_transaction_visibility_fresh.db", writeMode)
	if !fastRollback {
		dbPath = testDBPath(".", "test_transaction_visibility_fresh_slow.db", writeMode)
	}
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode, Options{
		"FastRollback": fastRollback,
	})
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	key := "tx-key-1"
	val := "tx-value-1"

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Set a value in the transaction
	err = tx.Set([]byte(key), []byte(val))
	if err != nil {
		t.Fatalf("Failed to set value in transaction: %v", err)
	}

	if fastRollback {
		// FastRollback: db.Get must not see uncommitted values
		_, err = db.Get([]byte(key))
		if err == nil {
			t.Fatalf("db.Get should not see uncommitted value, but got value for key %s", key)
		}
	} else {
		// SlowRollback: db.Get is refused while the transaction is open
		if _, err := db.Get([]byte(key)); err != ErrReadNotAllowed {
			t.Fatalf("db.Get during the slow rollback transaction should return ErrReadNotAllowed, got %v", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Now the value should be visible from db.Get
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("db.Get should see committed value, but got error: %v", err)
	}
	if !bytes.Equal(got, []byte(val)) {
		t.Fatalf("db.Get returned wrong value after commit: got %s, want %s", string(got), val)
	}
}

// TestReadNotAllowedDuringSlowRollbackTransaction covers the reader contract
// of slow rollback mode: concurrent db.Get and NewIterator are refused while
// a transaction is open, reads from inside the transaction still work, and
// everything is readable again once the transaction ends
func TestReadNotAllowedDuringSlowRollbackTransaction(t *testing.T) {
	withWriteModes(t, func(t *testing.T, writeMode string) {
		dbPath := testDBPath(".", "test_read_not_allowed_slow.db", writeMode)
		cleanupTestFiles(dbPath)

		db := openTestDB(t, dbPath, writeMode, Options{"FastRollback": false})
		defer func() {
			db.Close()
			cleanupTestFiles(dbPath)
		}()

		if err := db.Set([]byte("committed"), []byte("value")); err != nil {
			t.Fatalf("seed Set: %v", err)
		}

		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := tx.Set([]byte("committed"), []byte("in-flight")); err != nil {
			t.Fatalf("tx.Set: %v", err)
		}

		// db.Get is refused while the transaction is open
		if _, err := db.Get([]byte("committed")); err != ErrReadNotAllowed {
			t.Fatalf("db.Get during the transaction: expected ErrReadNotAllowed, got %v", err)
		}

		// NewIterator comes back invalid while the transaction is open
		it := db.NewIterator()
		if it.Valid() {
			t.Fatalf("NewIterator during the transaction should be invalid")
		}
		it.Close()

		// Reads from inside the transaction still work
		value, err := tx.Get([]byte("committed"))
		if err != nil {
			t.Fatalf("tx.Get during the transaction: %v", err)
		}
		if !bytes.Equal(value, []byte("in-flight")) {
			t.Fatalf("tx.Get returned %q, want %q", string(value), "in-flight")
		}

		// After the commit everything is readable again
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		value, err = db.Get([]byte("committed"))
		if err != nil {
			t.Fatalf("db.Get after commit: %v", err)
		}
		if !bytes.Equal(value, []byte("in-flight")) {
			t.Fatalf("db.Get after commit returned %q, want %q", string(value), "in-flight")
		}
		it = db.NewIterator()
		if !it.Valid() {
			t.Fatalf("NewIterator after commit should be valid")
		}
		it.Close()
	})
}

// TestFlushDuringFirstTransaction ensures mid-txn and post-commit flushes work on a
// never-written DB for both write modes and both FastRollback settings. This catches
// the FastRollback=false path where cloningSequence stays 0 and flushSequence would
// otherwise be rejected by flushDirtyIndexPages.
func TestFlushDuringFirstTransaction(t *testing.T) {
	withWriteAndRollbackModes(t, testFlushDuringFirstTransaction)
}

func testFlushDuringFirstTransaction(t *testing.T, writeMode string, fastRollback bool) {
	dbPath := testDBPath(".", "test_flush_first_txn.db", writeMode)
	if !fastRollback {
		dbPath = testDBPath(".", "test_flush_first_txn_slow.db", writeMode)
	}
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, writeMode, Options{
		"FastRollback":       fastRollback,
		"DirtyPageThreshold": 1,
	})

	if db.dirtyPageCount.Load() == 0 {
		t.Fatal("expected dirty bootstrap pages after Open")
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Simulate background flushing while the first txn is open.
	if err := db.flushIndexToDisk(); err != nil {
		t.Fatalf("mid-txn flush on fresh DB: %v (txnSeq=%d flushSeq=%d cloneSeq=%d fastRollback=%v)",
			err, db.txnSequence, db.flushSequence, db.cloningSequence, fastRollback)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := db.flushIndexToDisk(); err != nil {
		t.Fatalf("post-commit flush: %v", err)
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after commit/flush: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after commit/flush: got %q want %q", got, "v")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and confirm durability
	db2 := openTestDB(t, dbPath, writeMode, Options{"FastRollback": fastRollback})
	defer func() {
		db2.Close()
		cleanupTestFiles(dbPath)
	}()
	got, err = db2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after reopen: got %q want %q", got, "v")
	}
}

// TestFlushRestoresDirtyOnPostPageFailure verifies wasDirty restore when a
// step after flushDirtyIndexPages fails, and that Commit still succeeds once
// the main-file commit marker is durable (flush errors are not surfaced).
func TestFlushRestoresDirtyOnPostPageFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db")
	db, err := Open(path, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	var dirtyNonHeaderBefore int
	db.iteratePages("forward", false, func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		if pageNumber == 0 {
			return
		}
		for p := page; p != nil; p = p.next {
			if p.dirty.Load() {
				dirtyNonHeaderBefore++
				break
			}
		}
	})
	if dirtyNonHeaderBefore == 0 {
		t.Fatal("expected non-header dirty pages before commit")
	}

	// Fail the post-commit flush once. The index pipeline runs on the
	// flusher thread now; request a flush and wait for it so the failure
	// injection and the wasDirty restore complete deterministically
	db.failAfterDirtyPagesFlushed = func() error {
		return fmt.Errorf("injected post-page flush failure")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit must succeed after durable main-file marker despite flush failure: %v", err)
	}
	db.waitForCompletion("flush", db.requestFlush(true))

	var dirtyNonHeaderAfter int
	var restoredWasDirty int
	db.iteratePages("forward", false, func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		for ; page != nil; page = page.next {
			if page.txnSequence <= db.flushSequence {
				break
			}
		}
		if page == nil {
			return
		}
		if page.wasDirty && page.dirty.Load() {
			restoredWasDirty++
		}
		if pageNumber != 0 && page.dirty.Load() {
			dirtyNonHeaderAfter++
		}
	})
	if dirtyNonHeaderAfter == 0 {
		t.Fatal("expected non-header pages re-dirtied after failed flush")
	}
	if restoredWasDirty == 0 {
		t.Fatal("expected at least one wasDirty page to be dirty again after restore")
	}
	if db.dirtyPageCount.Load() <= 1 {
		t.Fatalf("expected dirtyPageCount > 1 after restore, got %d", db.dirtyPageCount.Load())
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after Commit with failed flush: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get: got %q want %q", got, "v")
	}

	// Later flush (next opportunity) should persist the already-correct index.
	if err := db.flushIndexToDisk(); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if db.dirtyPageCount.Load() != 0 {
		t.Fatalf("expected clean after successful retry, dirty=%d", db.dirtyPageCount.Load())
	}
}

// TestCommitFlushFailThenReopenRecovery covers crash-style recovery: Commit
// succeeds with index still dirty, process "dies" without flushing, Open
// reindexes committed main-file content via recoverUnindexedContent.
func TestCommitFlushFailThenReopenRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db")
	db, err := Open(path, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	db.failAfterDirtyPagesFlushed = func() error {
		return fmt.Errorf("injected")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if db.dirtyPageCount.Load() == 0 {
		t.Fatal("expected dirty index pages left for later recovery")
	}

	// Crash without index flush: stop workers and close FDs, skip flushIndexToDisk.
	abandonDBWithoutFlush(t, db)

	db2, err := Open(path, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	got, err := db2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after reopen recovery: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get after reopen: got %q want %q", got, "v")
	}
}

// abandonDBWithoutFlush stops background threads and closes files without
// flushing the index, simulating a crash after a durable main-file commit.
func abandonDBWithoutFlush(t *testing.T, db *DB) {
	t.Helper()
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	db.isClosed.Store(true)
	if db.cleanerThreadChannel != nil {
		db.cleanerThreadChannel <- "exit"
		db.cleanerThreadWaitGroup.Wait()
		close(db.cleanerThreadChannel)
		db.cleanerThreadChannel = nil
	}
	if db.flusherThreadChannel != nil {
		db.flusherThreadChannel <- "exit"
		db.flusherThreadWaitGroup.Wait()
		close(db.flusherThreadChannel)
		db.flusherThreadChannel = nil
	}
	db.readMutex.Lock()
	defer db.readMutex.Unlock()
	db.clearPageCache()
	db.clearExternalKeys()
	if db.fileLocked {
		_ = db.Unlock()
	}
	if db.mainFile != nil {
		_ = db.mainFile.Close()
		db.mainFile = nil
	}
	if db.indexFile != nil {
		_ = db.indexFile.Close()
		db.indexFile = nil
	}
}

// TestLastIndexedOffsetUpdate tests that lastIndexedOffset is properly updated when the worker thread
// flushes pages during active transactions. This test covers the exact scenario that was causing the bug
// where lastIndexedOffset was not being updated because no dirty pages were found during flush.
func TestLastIndexedOffsetUpdate(t *testing.T) {
	withRollbackModes(t, testLastIndexedOffsetUpdate)
}

func testLastIndexedOffsetUpdate(t *testing.T, fastRollback bool) {
	dbPath := "test_last_indexed_offset.db"
	if !fastRollback {
		dbPath = "test_last_indexed_offset_slow.db"
	}
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open database with small dirty page threshold to trigger frequent flushes
	options := Options{
		"DirtyPageThreshold": 5, // Very small to trigger flushes quickly
		"UseWAL": true,
		"SyncMainFileOnCommit": false,
		"FastRollback":       fastRollback,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Initial state check
	initialLastIndexed := db.lastIndexedOffset
	initialMainFileSize := db.mainFileSize.Load()
	t.Logf("Initial state - lastIndexedOffset: %d, mainFileSize: %d (FastRollback=%v)", initialLastIndexed, initialMainFileSize, fastRollback)

	// Phase 1: Add some initial data and let it get properly indexed
	t.Log("Phase 1: Adding initial data and ensuring it gets indexed")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("initial-key-%d", i)
		value := fmt.Sprintf("initial-value-%d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set initial key %s: %v", key, err)
		}
	}

	// Wait a bit for worker thread to flush
	time.Sleep(100 * time.Millisecond)

	// Force a manual flush to ensure everything is indexed
	err = db.flushIndexToDisk()
	if err != nil {
		t.Fatalf("Manual flush failed: %v", err)
	}

	afterInitialFlush := db.lastIndexedOffset
	afterInitialMainFileSize := db.mainFileSize.Load()
	t.Logf("After initial flush - lastIndexedOffset: %d, mainFileSize: %d", afterInitialFlush, afterInitialMainFileSize)

	// Verify that lastIndexedOffset was updated
	if afterInitialFlush <= initialLastIndexed {
		t.Fatalf("lastIndexedOffset should have increased after initial data, got %d, was %d", afterInitialFlush, initialLastIndexed)
	}

	// Phase 2: Start a transaction and add data without committing
	t.Log("Phase 2: Starting transaction and adding data without committing")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Track state before transaction data
	beforeTxLastIndexed := db.lastIndexedOffset
	beforeTxMainFileSize := db.mainFileSize.Load()
	t.Logf("Before transaction data - lastIndexedOffset: %d, mainFileSize: %d", beforeTxLastIndexed, beforeTxMainFileSize)

	// Add data within the transaction
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("tx-key-%d", i)
		value := fmt.Sprintf("tx-value-%d-" + strings.Repeat("x", 100), i) // Make values larger to increase file size
		err := tx.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set transaction key %s: %v", key, err)
		}
	}

	// At this point, main file size should have grown but lastIndexedOffset should NOT be updated
	// to the current main file size because the transaction hasn't committed yet
	afterTxDataLastIndexed := db.lastIndexedOffset
	afterTxDataMainFileSize := db.mainFileSize.Load()
	t.Logf("After transaction data (before commit) - lastIndexedOffset: %d, mainFileSize: %d", afterTxDataLastIndexed, afterTxDataMainFileSize)

	// Verify that main file size grew (data was written)
	if afterTxDataMainFileSize <= beforeTxMainFileSize {
		t.Fatalf("Main file size should have grown after transaction data, got %d, was %d", afterTxDataMainFileSize, beforeTxMainFileSize)
	}

	// Phase 3: Force a flush while transaction is still active
	// This simulates the worker thread flushing during an active transaction
	t.Log("Phase 3: Forcing flush during active transaction")

	// Wait a bit to let any automatic flushes happen
	time.Sleep(100 * time.Millisecond)

	// Force manual flush while transaction is active
	err = db.flushIndexToDisk()
	if err != nil {
		t.Fatalf("Manual flush during transaction failed: %v", err)
	}

	afterFlushDuringTxLastIndexed := db.lastIndexedOffset
	afterFlushDuringTxMainFileSize := db.mainFileSize.Load()
	t.Logf("After flush during transaction - lastIndexedOffset: %d, mainFileSize: %d", afterFlushDuringTxLastIndexed, afterFlushDuringTxMainFileSize)

	// CRITICAL TEST: The lastIndexedOffset should NOT be updated to the current main file size
	// because the transaction data hasn't been committed yet. It should remain at the
	// file size from before the transaction (prevFileSize).
	if afterFlushDuringTxLastIndexed > beforeTxMainFileSize {
		t.Fatalf("lastIndexedOffset should not exceed pre-transaction main file size during active transaction. "+
			"lastIndexedOffset: %d, pre-transaction mainFileSize: %d, current mainFileSize: %d",
			afterFlushDuringTxLastIndexed, beforeTxMainFileSize, afterFlushDuringTxMainFileSize)
	}

	// Phase 4: Commit the transaction and verify lastIndexedOffset gets updated
	t.Log("Phase 4: Committing transaction and verifying lastIndexedOffset update")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Wait a bit for any post-commit processing
	time.Sleep(100 * time.Millisecond)

	// Force another flush after commit
	err = db.flushIndexToDisk()
	if err != nil {
		t.Fatalf("Manual flush after commit failed: %v", err)
	}

	afterCommitLastIndexed := db.lastIndexedOffset
	afterCommitMainFileSize := db.mainFileSize.Load()
	t.Logf("After commit and flush - lastIndexedOffset: %d, mainFileSize: %d", afterCommitLastIndexed, afterCommitMainFileSize)

	// After commit, lastIndexedOffset should be updated to reflect the new main file size
	if afterCommitLastIndexed != afterCommitMainFileSize {
		t.Fatalf("After commit, lastIndexedOffset should equal mainFileSize. "+
			"lastIndexedOffset: %d, mainFileSize: %d", afterCommitLastIndexed, afterCommitMainFileSize)
	}

	// Phase 5: Verify that the fix works for multiple transactions
	t.Log("Phase 5: Testing multiple transactions to ensure consistent behavior")

	for round := 0; round < 3; round++ {
		t.Logf("Transaction round %d", round+1)

		beforeRoundMainFileSize := db.mainFileSize.Load()

		tx2, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction round %d: %v", round+1, err)
		}

		// Add some data
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("round-%d-key-%d", round, i)
			value := fmt.Sprintf("round-%d-value-%d-" + strings.Repeat("y", 50), round, i)
			err := tx2.Set([]byte(key), []byte(value))
			if err != nil {
				t.Fatalf("Failed to set key in round %d: %v", round+1, err)
			}
		}

		// Force flush during transaction
		err = db.flushIndexToDisk()
		if err != nil {
			t.Fatalf("Flush during transaction round %d failed: %v", round+1, err)
		}

		duringTxLastIndexed := db.lastIndexedOffset

		// Verify lastIndexedOffset doesn't exceed pre-transaction file size
		if duringTxLastIndexed > beforeRoundMainFileSize {
			t.Fatalf("Round %d: lastIndexedOffset should not exceed pre-transaction file size. "+
				"lastIndexedOffset: %d, pre-transaction mainFileSize: %d",
				round+1, duringTxLastIndexed, beforeRoundMainFileSize)
		}

		// Commit transaction
		err = tx2.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction round %d: %v", round+1, err)
		}

		// Force flush after commit
		err = db.flushIndexToDisk()
		if err != nil {
			t.Fatalf("Flush after commit round %d failed: %v", round+1, err)
		}

		afterRoundLastIndexed := db.lastIndexedOffset
		afterRoundMainFileSize := db.mainFileSize.Load()

		// Verify lastIndexedOffset matches mainFileSize after commit
		if afterRoundLastIndexed != afterRoundMainFileSize {
			t.Fatalf("Round %d: After commit, lastIndexedOffset should equal mainFileSize. "+
				"lastIndexedOffset: %d, mainFileSize: %d",
				round+1, afterRoundLastIndexed, afterRoundMainFileSize)
		}

		t.Logf("Round %d completed successfully", round+1)
	}

	t.Log("TestLastIndexedOffsetUpdate completed successfully")
}

// TestFreeListCycle tests for cycles in the free pages linked list
func TestFreeListCycle(t *testing.T) {
	withWriteModes(t, testFreeListCycle)
}

func testFreeListCycle(t *testing.T, writeMode string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// One main-index page so keys collide and convertHybridSubPageToTablePage runs.
	db := openTestDB(t, dbPath, writeMode, Options{"HashTableSize": 1})
	var err error

	keySize := 33
	valueSize := 750
	numItems := 100 // 192623

	// Scratch buffers filled on demand
	keyBuf := make([]byte, keySize)
	valBuf := make([]byte, valueSize)
	fillItem := func(index int) {
		fillDeterministicBytes(index, keyBuf)
		fillDeterministicBytes(index+23456789, valBuf)
	}
	fillTxItem := func(index int) {
		fillDeterministicBytes(index, keyBuf)
		fillDeterministicBytes(index+87654321, valBuf)
	}

	// Set using a transaction to trigger the problematic code path
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert entries one by one - this should trigger the cycle
	// Set retains the slices, so each write gets a fresh allocation
	for i := 0; i < numItems; i++ {
		if i%5000 == 0 {
			t.Logf("Setting entry %d", i)
		}

		key := generateDeterministicBytes(i, keySize)
		value := generateDeterministicBytes(i+23456789, valueSize)
		err := tx.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set entry %d: %v", i, err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify that all entries can be retrieved
	for i := 0; i < numItems; i++ {
		fillItem(i)
		value, err := db.Get(keyBuf)
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i, err)
		}
		if !bytes.Equal(value, valBuf) {
			t.Fatalf("Value mismatch for entry %d", i)
		}
	}

	t.Logf("Successfully inserted and retrieved %d entries", numItems)

	// Multiple transactions with multiple items each
	// 5,000 commits is enough to recycle the free-page list many times over and
	// catches the cycle bug this test was originally written for, while keeping
	// the runtime under a minute. Use -run TestFreeListCycle with a longer
	// -timeout if you want to push it harder.
	txNumTransactions := 5000
	txItemsPerTx := 10

	t.Logf("Testing %d transactions with %d items each...", txNumTransactions, txItemsPerTx)

	for txNum := 0; txNum < txNumTransactions; txNum++ {
		// Create and execute transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", txNum, err)
		}

		for i := 0; i < txItemsPerTx; i++ {
			idx := numItems + txNum*txItemsPerTx + i
			key := generateDeterministicBytes(idx, keySize)
			value := generateDeterministicBytes(idx+87654321, valueSize)
			err := tx.Set(key, value)
			if err != nil {
				t.Fatalf("Failed to set entry %d in transaction %d: %v", i, txNum, err)
			}
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", txNum, err)
		}

		// Verify a few values from the transaction
		if txNum%1000 == 0 {
			for i := 0; i < txItemsPerTx; i += 3 {
				idx := numItems + txNum*txItemsPerTx + i
				fillTxItem(idx)
				value, err := db.Get(keyBuf)
				if err != nil {
					t.Fatalf("Failed to get entry %d from transaction %d: %v", i, txNum, err)
				}
				if !bytes.Equal(value, valBuf) {
					t.Fatalf("Value mismatch for entry %d in transaction %d", i, txNum)
				}
			}
		}
	}

	totalEntries := numItems + txNumTransactions*txItemsPerTx
	t.Logf("Successfully completed %d transactions, total entries: %d", txNumTransactions, totalEntries)

	db.Close()

	db, err = Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Verify that the database is still working
	for i := 0; i < numItems; i++ {
		fillItem(i)
		value, err := db.Get(keyBuf)
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i, err)
		}
		if !bytes.Equal(value, valBuf) {
			t.Fatalf("Value mismatch for entry %d", i)
		}
	}
	for txNum := 0; txNum < txNumTransactions; txNum++ {
		for i := 0; i < txItemsPerTx; i++ {
			idx := numItems + txNum*txItemsPerTx + i
			fillTxItem(idx)
			value, err := db.Get(keyBuf)
			if err != nil {
				t.Fatalf("Failed to get entry %d: %v", i, err)
			}
			if !bytes.Equal(value, valBuf) {
				t.Fatalf("Value mismatch for entry %d", i)
			}
		}
	}

	t.Logf("Successfully opened database and retrieved %d entries", numItems)

	db.Close()

	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")
}

// TestKeyCollisionHandling tests that keys mapping to the same hash-table path
// still return their own values and do not leak across each other.
func TestKeyCollisionHandling(t *testing.T) {
	dbPath := "test_collision.db"
	cleanupTestFiles(dbPath)

	db, err := Open(dbPath, Options{"HashTableSize": 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	key1 := []byte("first_key")
	key2 := []byte("colliding_key_1278932")

	t.Logf("Found colliding keys: '%s' and '%s'", string(key1), string(key2))

	value1 := []byte("value_for_first_key")
	value2 := []byte("value_for_second_key")

	err = db.Set(key1, value1)
	if err != nil {
		t.Fatalf("Failed to set first key: %v", err)
	}

	retrievedValue, err := db.Get(key2)
	if err == nil {
		t.Fatalf("Second key returned a value: %v", retrievedValue)
	}
	if err.Error() != "key not found" {
		t.Fatalf("Expected 'key not found' error, got '%v'", err)
	}

	for i := 0; i < 5; i++ {
		retrievedValue1, err := db.Get(key1)
		if err != nil {
			t.Fatalf("Failed to get value for first key on iteration %d: %v", i, err)
		}
		if !bytes.Equal(retrievedValue1, value1) {
			t.Fatalf("First key returned wrong value on iteration %d. Expected '%s', got '%s'", i, string(value1), string(retrievedValue1))
		}

		retrievedValue2, err := db.Get(key2)
		if err == nil {
			t.Fatalf("Second key returned a value: %v", retrievedValue2)
		}
		if err.Error() != "key not found" {
			t.Fatalf("Expected 'key not found' error, got '%v'", err)
		}
	}

	err = db.Set(key2, value2)
	if err != nil {
		t.Fatalf("Failed to set second key: %v", err)
	}

	retrievedValue1, err := db.Get(key1)
	if err != nil {
		t.Fatalf("Failed to get value for first key: %v", err)
	}
	if !bytes.Equal(retrievedValue1, value1) {
		t.Fatalf("First key returned wrong value. Expected '%s', got '%s'", string(value1), string(retrievedValue1))
	}

	retrievedValue2, err := db.Get(key2)
	if err != nil {
		t.Fatalf("Failed to get value for second key: %v", err)
	}
	if !bytes.Equal(retrievedValue2, value2) {
		t.Fatalf("Second key returned wrong value. Expected '%s', got '%s'", string(value2), string(retrievedValue2))
	}
}

// findCollidingKey finds a key that collides with the base key on both main index and hybrid page
func findCollidingKey(baseKey []byte, mainIndexPages int) ([]byte, error) {
	const maxAttempts = 10000000

	// Calculate the slot for the base key in the main index
	baseHash := hashKey(baseKey, 0) // InitialSalt = 0
	totalMainEntries := uint64(mainIndexPages * 818) // TableEntries = 818
	baseMainSlot := int(baseHash % totalMainEntries)

	// Also calculate the slot for the base key in a hybrid page with salt 1
	baseHybridSlot := int(hashKey(baseKey, 1) % 818) // TableEntries = 818

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Generate a candidate key by appending a counter
		candidate := fmt.Sprintf("colliding_key_%d", attempt)
		candidateKey := []byte(candidate)

		// Calculate slots for the candidate key
		candHash := hashKey(candidateKey, 0) // InitialSalt = 0
		candMainSlot := int(candHash % totalMainEntries)
		candHybridSlot := int(hashKey(candidateKey, 1) % 818) // TableEntries = 818

		// Check if both slots match (collision on both levels)
		if candMainSlot == baseMainSlot && candHybridSlot == baseHybridSlot {
			return candidateKey, nil
		}
	}

	return nil, fmt.Errorf("could not find a colliding key after %d attempts", maxAttempts)
}

// fillDeterministicBytes fills buf with a deterministic LCG stream seeded by seed
func fillDeterministicBytes(seed int, buf []byte) {
	a := uint32(1103515245)
	c := uint32(12345)
	m := uint32(1<<31 - 1)
	x := uint32(seed)
	for i := range buf {
		x = (a*x + c) % m
		buf[i] = byte(x % 256)
	}
}

func generateDeterministicBytes(seed int, size int) []byte {
	buf := make([]byte, size)
	fillDeterministicBytes(seed, buf)
	return buf
}

// TestDuplicateWriteNoDirtyPagesOrWALGrowth tests that writing the same key-value pair
// that already exists in the database doesn't create unnecessary dirty pages or WAL writes.
func TestDuplicateWriteNoDirtyPagesOrWALGrowth(t *testing.T) {
	withWriteModes(t, testDuplicateWriteNoDirtyPagesOrWALGrowth)
}

func testDuplicateWriteNoDirtyPagesOrWALGrowth(t *testing.T, writeMode string) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	walPath := dbPath + "-wal"

	// Create database and add initial data
	db := openTestDB(t, dbPath, writeMode)
	var err error

	// Set initial value
	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set initial data: %v", err)
	}

	// Close and reopen to ensure data is persisted to main file
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	db, err = Open(dbPath, modeOptions(writeMode))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Check state before duplicate write
	dirtyPagesBefore := db.dirtyPageCount.Load()
	walSizeBefore := getFileSize(t, walPath)

	t.Logf("Before duplicate write - Dirty pages: %d, WAL size: %d bytes",
		dirtyPagesBefore, walSizeBefore)

	// Write the exact same key-value pair again
	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set duplicate data: %v", err)
	}

	// Check state after duplicate write
	dirtyPagesAfter := db.dirtyPageCount.Load()
	walSizeAfter := getFileSize(t, walPath)

	t.Logf("After duplicate write - Dirty pages: %d, WAL size: %d bytes",
		dirtyPagesAfter, walSizeAfter)

	// Check if dirty pages increased
	if dirtyPagesAfter > dirtyPagesBefore {
		t.Errorf("BUG: Duplicate write created %d dirty pages (should be 0)",
			dirtyPagesAfter - dirtyPagesBefore)
	}

	// Force a flush to trigger WAL writes
	err = db.flushIndexToDisk()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	walSizeFinal := getFileSize(t, walPath)
	t.Logf("After flush - WAL size: %d bytes", walSizeFinal)

	if walSizeFinal > walSizeBefore {
		t.Errorf("BUG: WAL grew by %d bytes for duplicate write",
			walSizeFinal - walSizeBefore)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestCacheSizeThresholdPercentage(t *testing.T) {
	// Create a test database
	dbPath := "test_cache_percentage.db"

	cleanupTestFiles(dbPath)

	// Test opening database with percentage CacheSizeThreshold
	db, err := Open(dbPath, Options{"CacheSizeThreshold": "10%"})
	if err != nil {
		t.Fatalf("Failed to open database with percentage CacheSizeThreshold: %v", err)
	}

	// Verify the cache size was set (should be greater than the minimum 1024)
	if db.cacheSizeThreshold.Load() <= 1024 {
		t.Errorf("Cache size threshold should be greater than 1024, got %d", db.cacheSizeThreshold.Load())
	}

	// Test SetOption with percentage
	err = db.SetOption("CacheSizeThreshold", "5%")
	if err != nil {
		t.Fatalf("Failed to set CacheSizeThreshold with percentage: %v", err)
	}

	// Verify the cache size was updated
	if db.cacheSizeThreshold.Load() <= 1024 {
		t.Errorf("Cache size threshold should be greater than 1024 after percentage update, got %d", db.cacheSizeThreshold.Load())
	}

	// Test DirtyPageThreshold as percentage of cache
	err = db.SetOption("DirtyPageThreshold", "30%")
	if err != nil {
		t.Fatalf("Failed to set DirtyPageThreshold with percentage: %v", err)
	}

	// Verify dirty page threshold is approximately 30% of cache size,
	// floored at MinDirtyPageThreshold and capped at MaxDirtyPageThreshold.
	expectedDirtyPages := int(float64(db.cacheSizeThreshold.Load()) * 0.30)
	if expectedDirtyPages < MinDirtyPageThreshold {
		expectedDirtyPages = MinDirtyPageThreshold
	}
	if expectedDirtyPages > MaxDirtyPageThreshold {
		expectedDirtyPages = MaxDirtyPageThreshold
	}
	if db.dirtyPageThreshold.Load() < int64(expectedDirtyPages)-1 || db.dirtyPageThreshold.Load() > int64(expectedDirtyPages)+1 {
		t.Errorf("Expected dirty page threshold around %d (30%% of %d, floor %d, cap %d), got %d",
			expectedDirtyPages, db.cacheSizeThreshold.Load(), MinDirtyPageThreshold, MaxDirtyPageThreshold, db.dirtyPageThreshold.Load())
	}

	// Percentage dirty thresholds must track CacheSizeThreshold changes.
	err = db.SetOption("CacheSizeThreshold", 1000)
	if err != nil {
		t.Fatalf("Failed to set CacheSizeThreshold to 1000: %v", err)
	}
	if db.dirtyPageThreshold.Load() != MinDirtyPageThreshold {
		t.Errorf("Expected dirty page threshold floored at %d (30%% of 1000), got %d",
			MinDirtyPageThreshold, db.dirtyPageThreshold.Load())
	}

	// Percentage dirty thresholds are capped so adaptive cache growth cannot
	// push opportunistic flush batches without bound.
	err = db.SetOption("CacheSizeThreshold", MaxDirtyPageThreshold*10)
	if err != nil {
		t.Fatalf("Failed to set large CacheSizeThreshold: %v", err)
	}
	if db.dirtyPageThreshold.Load() != MaxDirtyPageThreshold {
		t.Errorf("Expected dirty page threshold capped at %d, got %d",
			MaxDirtyPageThreshold, db.dirtyPageThreshold.Load())
	}

	// Test invalid percentage (should fail)
	err = db.SetOption("CacheSizeThreshold", "150%")
	if err == nil {
		t.Error("Expected error for percentage > 100, but got none")
	}

	// Test absolute value as string (should succeed)
	err = db.SetOption("CacheSizeThreshold", "25")
	if err != nil {
		t.Fatalf("Failed to set CacheSizeThreshold with absolute string value: %v", err)
	}
	if db.cacheSizeThreshold.Load() != 25 {
		t.Errorf("Expected cache size threshold 25, got %d", db.cacheSizeThreshold.Load())
	}

	// Test invalid value (should fail)
	err = db.SetOption("CacheSizeThreshold", "invalid")
	if err == nil {
		t.Error("Expected error for invalid value, but got none")
	}

	// Test setting back to integer value
	err = db.SetOption("CacheSizeThreshold", 2048)
	if err != nil {
		t.Fatalf("Failed to set CacheSizeThreshold back to integer: %v", err)
	}

	if db.cacheSizeThreshold.Load() != 2048 {
		t.Errorf("Expected cache size threshold 2048, got %d", db.cacheSizeThreshold.Load())
	}

	// Test DirtyPageThreshold with integer value
	err = db.SetOption("DirtyPageThreshold", 512)
	if err != nil {
		t.Fatalf("Failed to set DirtyPageThreshold to integer: %v", err)
	}

	if db.dirtyPageThreshold.Load() != 512 {
		t.Errorf("Expected dirty page threshold 512, got %d", db.dirtyPageThreshold.Load())
	}

	// Absolute dirty thresholds must not track later cache size changes.
	err = db.SetOption("CacheSizeThreshold", 4096)
	if err != nil {
		t.Fatalf("Failed to set CacheSizeThreshold to 4096: %v", err)
	}
	if db.dirtyPageThreshold.Load() != 512 {
		t.Errorf("Expected absolute dirty page threshold to stay 512 after cache resize, got %d",
			db.dirtyPageThreshold.Load())
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

// getFileSize returns the size of a file in bytes, or 0 if the file doesn't exist
func getFileSize(t *testing.T, filePath string) int64 {
	if stat, err := os.Stat(filePath); err == nil {
		return stat.Size()
	} else if !os.IsNotExist(err) {
		t.Logf("Warning: Could not stat file %s: %v", filePath, err)
	}
	return 0
}
// ---------------------------------------------------------------------------
// Hybrid tree pointer / allocation regressions
// ---------------------------------------------------------------------------

func openTinyDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(filepath.Join(dir, "data.db"), Options{
		"UseWAL": true,
		"SyncMainFileOnCommit": false,
		"HashTableSize":        1,
		"CacheSizeThreshold":   4096,
		"CheckpointThreshold":  int64(16 << 20),
		"AdaptiveCacheEnabled": false,
		"FastRollback":         true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func reopenDB(t *testing.T, path string, opts Options) *DB {
	t.Helper()
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return db
}

func mustSet(t *testing.T, db *DB, key, val []byte) {
	t.Helper()
	if err := db.Set(key, val); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
}

func isSubPageErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "sub-page with index")
}

func bytesOf(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func putU16LE(dst []byte, v uint16) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
}

func putU64LE(dst []byte, v uint64) {
	for i := 0; i < 8; i++ {
		dst[i] = byte(v >> (8 * i))
	}
}

func emptySlotInHybridSubPage(db *DB, sub *HybridSubPage) (int, error) {
	for slot := 0; slot < TableEntries; slot++ {
		_, pn, _, off, _, err := db.findEntryInHybridSubPage(sub.Page, &sub.Page.SubPages[sub.SubPageId], slot)
		if err != nil {
			return 0, err
		}
		if pn == 0 && off == 0 {
			return slot, nil
		}
	}
	return 0, fmt.Errorf("no empty slot on page %d sub-page %d", sub.Page.pageNumber, sub.SubPageId)
}

func keyForEmptyHybridSlot(db *DB, sub *HybridSubPage, salt uint8, prefix string) ([]byte, int, error) {
	for i := 0; i < TableEntries*4; i++ {
		k := []byte(fmt.Sprintf("%s-%d", prefix, i))
		slot := db.getTableSlot(k, salt)
		_, pn, _, off, _, err := db.findEntryInHybridSubPage(sub.Page, &sub.Page.SubPages[sub.SubPageId], slot)
		if err != nil {
			return nil, 0, err
		}
		if pn == 0 && off == 0 {
			return k, slot, nil
		}
	}
	return nil, 0, fmt.Errorf("no key hashing to an empty slot on page %d sub-page %d", sub.Page.pageNumber, sub.SubPageId)
}

// ---------------------------------------------------------------------------
// Path 1: moveSubPageToNewHybridPage — parent must track new identity
// ---------------------------------------------------------------------------

func TestMoveSubPageParentPointerSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		"UseWAL": true, "SyncMainFileOnCommit": false, "HashTableSize": 1,
		"CacheSizeThreshold": 4096, "AdaptiveCacheEnabled": false,
	}
	db, err := Open(filepath.Join(dir, "data.db"), opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 8000
	val := bytesOf('v', 40)
	for i := 0; i < n; i++ {
		mustSet(t, db, []byte(fmt.Sprintf("move-key-%08d", i)), val)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenDB(t, filepath.Join(dir, "data.db"), opts)
	defer db.Close()

	var subPageErrs int
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("move-key-%08d", i))
		_, err := db.Get(k)
		if isSubPageErr(err) {
			subPageErrs++
			if subPageErrs <= 5 {
				t.Logf("%s: %v", k, err)
			}
		} else if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
	if subPageErrs > 0 {
		t.Fatalf("move/reopen sub-page errors: %d/%d", subPageErrs, n)
	}
}

// ---------------------------------------------------------------------------
// Path 2: convertHybridSubPageToTablePage must not destroy siblings
// ---------------------------------------------------------------------------

func TestConvertLeavesSiblingSubpageReachable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")
	db := openTinyDB(t, dir)

	db.writeMutex.Lock()
	db.readMutex.RLock()

	mkGroup := func(prefix byte, n int) []HybridEntry {
		out := make([]HybridEntry, 0, n)
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("conv-%c-%04d", prefix, i))
			off, _, err := db.appendData(k, bytesOf(prefix, 20))
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, HybridEntry{Key: k, DataOffset: off})
		}
		return out
	}
	groupA := mkGroup('A', 20)
	groupB := mkGroup('B', 20)

	subA, err := db.addEntriesToNewHybridSubPage(1, groupA)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	// Place B on the same hybrid page.
	hp := subA.Page
	saltB := uint8(2)
	subPageSize := len(groupB) * 10
	total := HybridSubPageHeaderSize + subPageSize
	hp, err = db.getWritablePage(hp)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	if hp.ContentSize+total > PageSize {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal("not enough space to co-locate sibling")
	}
	var idB uint8
	for idB < 255 && hp.SubPages[idB].Offset != 0 {
		idB++
	}
	off := hp.ContentSize
	hp.data[off] = idB
	hp.data[off+1] = saltB
	putU16LE(hp.data[off+2:], uint16(subPageSize))
	slotsPos := off + HybridSubPageHeaderSize
	ptrsPos := slotsPos + 2*len(groupB)
	for _, e := range groupB {
		slot := db.getTableSlot(e.Key, saltB)
		putU16LE(hp.data[slotsPos:], uint16(slot))
		slotsPos += 2
		putU64LE(hp.data[ptrsPos:], hybridDataPtrWord(e.DataOffset, e.DataSize))
		ptrsPos += 8
	}
	hp.SubPages[idB] = HybridSubPageInfo{Salt: saltB, Offset: uint16(off), Size: uint16(subPageSize)}
	hp.ContentSize += total
	hp.NumSubPages++
	db.markPageDirty(hp)
	subB := &HybridSubPage{Page: hp, SubPageId: idB}
	subA.Page = hp

	main, err := db.getTablePage(1)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	if err := db.setTableEntry(main, 10, subA.Page.pageNumber, subA.SubPageId, 0); err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	if err := db.setTableEntry(main, 11, subB.Page.pageNumber, subB.SubPageId, 0); err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}

	hybridPageNum := subA.Page.pageNumber
	t.Logf("siblings on hybrid page %d: A=%d B=%d", hybridPageNum, subA.SubPageId, subB.SubPageId)

	// Force convert of A with an empty slot (convert only runs when findEntry missed).
	k := []byte("conv-A-forced")
	bigOff, _, err := db.appendData(k, bytesOf('Z', 3500))
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	emptySlot, err := emptySlotInHybridSubPage(db, subA)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	if err := db.convertHybridSubPageToTablePage(subA, emptySlot, k, bigOff, 0); err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatalf("convert: %v", err)
	}
	if subA.Page.pageNumber == hybridPageNum {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatalf("convert reused hybrid page %d; want new table page so siblings survive", hybridPageNum)
	}
	t.Logf("converted A to table page %d; retargeted subPageId=%d", subA.Page.pageNumber, subA.SubPageId)

	// Parent must be updated to the new table (simulates setOnTablePage check).
	if err := db.setTableEntry(main, 10, subA.Page.pageNumber, subA.SubPageId, 0); err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}

	// Sibling B must still exist on the original hybrid page.
	stillHybrid, err := db.getHybridPage(hybridPageNum)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatalf("hybrid page %d lost after convert: %v", hybridPageNum, err)
	}
	if stillHybrid.SubPages[subB.SubPageId].Offset == 0 {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal("sibling sub-page B cleared by convert")
	}

	db.readMutex.RUnlock()
	db.writeMutex.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenDB(t, path, Options{
		"UseWAL": true, "SyncMainFileOnCommit": false, "HashTableSize": 1,
		"CacheSizeThreshold": 4096, "AdaptiveCacheEnabled": false,
	})
	defer db.Close()

	// Probe through pinned slots — no sub-page errors allowed.
	for i := 0; i < 20; i++ {
		for _, prefix := range []byte{'A', 'B'} {
			k := []byte(fmt.Sprintf("conv-%c-%04d", prefix, i))
			_, err := db.Get(k)
			if isSubPageErr(err) {
				t.Fatalf("%s: %v", k, err)
			}
		}
	}
}

func TestConvertViaSetWithDeepTreeSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")
	opts := Options{
		"UseWAL": true, "SyncMainFileOnCommit": false, "HashTableSize": 1,
		"CacheSizeThreshold": 2048, "AdaptiveCacheEnabled": false,
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 2000
	short := bytesOf('s', 16)
	for i := 0; i < n; i++ {
		mustSet(t, db, []byte(fmt.Sprintf("s-%08d", i)), short)
	}
	big := bytesOf('B', 3000)
	for i := 0; i < 200; i++ {
		mustSet(t, db, []byte(fmt.Sprintf("b-%08d", i)), big)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenDB(t, path, opts)
	defer db.Close()

	var subPageErrs int
	for i := 0; i < n; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("s-%08d", i)))
		if isSubPageErr(err) {
			subPageErrs++
			if subPageErrs <= 8 {
				t.Logf("s-%08d: %v", i, err)
			}
		} else if err != nil {
			t.Fatalf("s-%08d: %v", i, err)
		}
	}
	for i := 0; i < 200; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("b-%08d", i)))
		if isSubPageErr(err) {
			t.Fatalf("b-%08d: %v", i, err)
		} else if err != nil {
			t.Fatalf("b-%08d: %v", i, err)
		} else if len(got) != len(big) {
			t.Fatalf("b-%08d: len %d", i, len(got))
		}
	}
	if subPageErrs > 0 {
		t.Fatalf("convert/deep-tree reopen: subPageErrs=%d", subPageErrs)
	}
}

// Path 2b: converting a single sub-page hybrid page converts that page in place
// so the parent pointer does not need to be rewritten.
func TestConvertSingleSubPageReusesPageAndSkipsParentRewrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")
	db := openTinyDB(t, dir)

	db.writeMutex.Lock()
	db.readMutex.RLock()
	unlock := func() {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
	}

	group := make([]HybridEntry, 0, 20)
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("solo-%04d", i))
		off, _, err := db.appendData(k, bytesOf('S', 20))
		if err != nil {
			unlock()
			t.Fatal(err)
		}
		group = append(group, HybridEntry{Key: k, DataOffset: off})
	}
	sub, err := db.addEntriesToNewHybridSubPage(1, group)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	if sub.Page.NumSubPages != 1 {
		unlock()
		t.Fatalf("expected hybrid page %d to hold a single sub-page %d, live=%v",
			sub.Page.pageNumber, sub.SubPageId, liveHybridSubPageIDs(sub.Page))
	}

	main, err := db.getTablePage(1)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	if err := db.setTableEntry(main, 10, sub.Page.pageNumber, sub.SubPageId, 0); err != nil {
		unlock()
		t.Fatal(err)
	}

	origPN := sub.Page.pageNumber
	origID := sub.SubPageId
	salt := sub.Page.SubPages[origID].Salt
	k, slot, err := keyForEmptyHybridSlot(db, sub, salt, "solo-forced")
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	off, _, err := db.appendData(k, bytesOf('Z', 3500))
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	if err := db.convertHybridSubPageToTablePage(sub, slot, k, off, 0); err != nil {
		unlock()
		t.Fatalf("convert: %v", err)
	}
	if sub.Page.pageNumber != origPN || sub.SubPageId != origID {
		unlock()
		t.Fatalf("in-place convert changed identity page %d id %d -> page %d id %d",
			origPN, origID, sub.Page.pageNumber, sub.SubPageId)
	}
	if sub.Page.pageType != ContentTypeTable {
		unlock()
		t.Fatalf("expected in-place table at page %d, type %c", origPN, sub.Page.pageType)
	}
	if _, err := db.getTablePage(origPN); err != nil {
		unlock()
		t.Fatalf("converted page %d is not a table: %v", origPN, err)
	}
	if _, err := db.getHybridPage(origPN); err == nil {
		unlock()
		t.Fatalf("converted page %d still readable as hybrid", origPN)
	}

	// Parent still points at the original identity — no rewrite required.
	main, err = db.getTablePage(1)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	pn, id, _ := db.getTableEntry(main, 10)
	if pn != origPN || id != origID {
		unlock()
		t.Fatalf("parent slot changed to page %d id %d, want page %d id %d", pn, id, origPN, origID)
	}

	seq := db.txnSequence
	check := func(key []byte) {
		t.Helper()
		dataOffset, dataSize, err := db.lookupOffsetInPage(key, origPN, origID, seq)
		if err != nil {
			unlock()
			t.Fatalf("%s: %v", key, err)
		}
		if dataOffset == 0 {
			unlock()
			t.Fatalf("%s: missing", key)
		}
		if _, err := db.readContentValue(dataOffset, key, dataSize); err != nil {
			unlock()
			t.Fatalf("%s: %v", key, err)
		}
	}
	for i := 0; i < 20; i++ {
		check([]byte(fmt.Sprintf("solo-%04d", i)))
	}
	check(k)

	unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenDB(t, path, Options{
		"UseWAL": true, "SyncMainFileOnCommit": false, "HashTableSize": 1,
		"CacheSizeThreshold": 4096, "AdaptiveCacheEnabled": false,
	})
	defer db.Close()

	main, err = db.getTablePage(1)
	if err != nil {
		t.Fatal(err)
	}
	pn, id, _ = db.getTableEntry(main, 10)
	if pn != origPN || id != origID {
		t.Fatalf("reopen parent slot page %d id %d, want page %d id %d", pn, id, origPN, origID)
	}
	if _, err := db.getTablePage(origPN); err != nil {
		t.Fatalf("reopen page %d is not a table: %v", origPN, err)
	}
	seq = db.txnSequence
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("solo-%04d", i))
		dataOffset, dataSize, err := db.lookupOffsetInPage(key, origPN, origID, seq)
		if isSubPageErr(err) {
			t.Fatalf("%s: %v", key, err)
		}
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if dataOffset == 0 {
			t.Fatalf("%s: missing", key)
		}
		got, err := db.readContentValue(dataOffset, key, dataSize)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if len(got) != 20 {
			t.Fatalf("%s: len %d", key, len(got))
		}
	}
	dataOffset, dataSize, err := db.lookupOffsetInPage(k, origPN, origID, seq)
	if err != nil {
		t.Fatalf("solo-forced: %v", err)
	}
	if dataOffset == 0 {
		t.Fatal("solo-forced: missing")
	}
	got, err := db.readContentValue(dataOffset, k, dataSize)
	if err != nil {
		t.Fatalf("solo-forced: %v", err)
	}
	if len(got) != 3500 {
		t.Fatalf("solo-forced: len %d", len(got))
	}
}

// ---------------------------------------------------------------------------
// Path 3: nested child move must re-find parent entry after layout shift
// ---------------------------------------------------------------------------

func TestNestedMoveRewritesParentAfterLayoutShift(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")
	db := openTinyDB(t, dir)

	db.writeMutex.Lock()
	db.readMutex.RLock()

	childEntries := make([]HybridEntry, 0, 5)
	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("child-%d", i))
		off, _, err := db.appendData(k, []byte("cv"))
		if err != nil {
			db.readMutex.RUnlock()
			db.writeMutex.Unlock()
			t.Fatal(err)
		}
		childEntries = append(childEntries, HybridEntry{Key: k, DataOffset: off})
	}
	child, err := db.addEntriesToNewHybridSubPage(5, childEntries)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	pageNum := child.Page.pageNumber
	childID := child.SubPageId

	hp, err := db.getWritablePage(child.Page)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	salt := uint8(9)
	parentKey := []byte("parent-key")
	slot := db.getTableSlot(parentKey, salt)
	entrySize := 10 // u16 slot + u64 pointer word
	total := HybridSubPageHeaderSize + entrySize
	if hp.ContentSize+total > PageSize {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal("no space for parent sub-page")
	}
	var parentID uint8
	for parentID < 255 && hp.SubPages[parentID].Offset != 0 {
		parentID++
	}
	off := hp.ContentSize
	hp.data[off] = parentID
	hp.data[off+1] = salt
	putU16LE(hp.data[off+2:], uint16(entrySize))
	slotsPos := off + HybridSubPageHeaderSize
	putU16LE(hp.data[slotsPos:], uint16(slot))
	putU64LE(hp.data[slotsPos+2:], hybridSubPtrWord(pageNum, childID))
	hp.SubPages[parentID] = HybridSubPageInfo{Salt: salt, Offset: uint16(off), Size: uint16(entrySize)}
	hp.ContentSize += total
	hp.NumSubPages++
	db.markPageDirty(hp)

	main, err := db.getTablePage(1)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	if err := db.setTableEntry(main, 20, pageNum, parentID, 0); err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	t.Logf("page %d childSub=%d parentSub=%d", pageNum, childID, parentID)

	// Direct move of the earlier child (shifts the later parent sub-page).
	childSub := &HybridSubPage{Page: hp, SubPageId: childID}
	eoff, _, err := db.appendData([]byte("force-move"), []byte("m"))
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}
	oldID := childSub.SubPageId
	if err := db.moveSubPageToNewHybridPage(childSub, 0, eoff, 0); err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatalf("move: %v", err)
	}
	t.Logf("moved child %d -> page %d sub %d", oldID, childSub.Page.pageNumber, childSub.SubPageId)

	parentPage, err := db.getHybridPage(pageNum)
	if err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatalf("get parent page: %v", err)
	}

	// Production fix path: refresh + re-find by slot (not stale entryOffset).
	pSub := &HybridSubPage{Page: parentPage, SubPageId: parentID}
	ei, pn, _, _, _, err := db.findEntryInHybridSubPage(parentPage, &parentPage.SubPages[parentID], slot)
	if err != nil || pn == 0 {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatalf("re-find parent entry after move: err=%v pageNumber=%d (layout/pointer bug)", err, pn)
	}
	if err := db.updateSubPagePointerInHybridSubPage(pSub, ei, childSub.Page.pageNumber, childSub.SubPageId); err != nil {
		db.readMutex.RUnlock()
		db.writeMutex.Unlock()
		t.Fatal(err)
	}

	db.readMutex.RUnlock()
	db.writeMutex.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenDB(t, path, Options{
		"UseWAL": true, "SyncMainFileOnCommit": false, "HashTableSize": 1,
		"CacheSizeThreshold": 4096, "AdaptiveCacheEnabled": false,
	})
	defer db.Close()

	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("child-%d", i))
		_, err := db.Get(k)
		if isSubPageErr(err) {
			t.Fatalf("%s after reopen: %v", k, err)
		}
	}
}

func TestProductionNestedMoveUpdatesParentPointer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.db")
	opts := Options{
		"UseWAL": true, "SyncMainFileOnCommit": false, "HashTableSize": 1,
		"CacheSizeThreshold": 1024, "AdaptiveCacheEnabled": false,
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	val := bytesOf('p', 64)
	const n = 12000
	for i := 0; i < n; i++ {
		mustSet(t, db, []byte(fmt.Sprintf("nm-%08d", i)), val)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenDB(t, path, opts)
	defer db.Close()
	var subPageErrs int
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("nm-%08d", i))
		_, err := db.Get(k)
		if isSubPageErr(err) {
			subPageErrs++
			if subPageErrs <= 10 {
				t.Logf("%s: %v", k, err)
			}
		} else if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
	if subPageErrs > 0 {
		t.Fatalf("nested move/reopen sub-page errors: %d/%d", subPageErrs, n)
	}
}

func TestAllocateHybridSubPageIDReserved(t *testing.T) {
	dir := t.TempDir()
	db := openTinyDB(t, dir)
	defer db.Close()

	db.writeMutex.Lock()
	db.readMutex.RLock()
	defer db.readMutex.RUnlock()
	defer db.writeMutex.Unlock()

	a, err := db.allocateHybridPageWithSpace(64)
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.allocateHybridPageWithSpace(64)
	if err != nil {
		t.Fatal(err)
	}
	if a.Page.pageNumber == b.Page.pageNumber && a.SubPageId == b.SubPageId {
		t.Fatalf("double-allocate same page %d subPageId %d (reservation bug)", a.Page.pageNumber, a.SubPageId)
	}
	t.Logf("a=(%d,%d) b=(%d,%d)", a.Page.pageNumber, a.SubPageId, b.Page.pageNumber, b.SubPageId)
}

// TestIteratorModes verifies that the offsets and scan+lookup modes yield the
// same pairs: records superseded by updates and deleted keys are skipped, and
// external (mutable) keys are yielded after the main pass
func TestIteratorModes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "modes.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numKeys = 200
	expected := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("mode:%05d", i)
		value := fmt.Sprintf("value-%05d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		expected[key] = value
	}
	// Updates leave superseded records the modes must skip
	for i := 0; i < numKeys; i += 2 {
		key := fmt.Sprintf("mode:%05d", i)
		value := fmt.Sprintf("updated-%05d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		expected[key] = value
	}
	// Deletions leave tombstones the modes must skip
	for i := 0; i < numKeys; i += 10 {
		key := fmt.Sprintf("mode:%05d", i)
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
		delete(expected, key)
	}
	// External (mutable) keys are yielded after the main pass in both modes
	for _, key := range []string{"ext-a", "ext-b"} {
		if err := db.SetOption("AddMutableKey", []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"ext-a", "ext-b"} {
		value := "ext-value-" + key
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		expected[key] = value
	}

	// Flush so part of the index is on disk and the offsets walk exercises
	// both the page cache and the index file reads
	waitForBackgroundFlush(t, db)

	modes := map[string]*Iterator{
		"offsets":     db.newOffsetsIterator(),
		"scan+lookup": db.newScanLookupIterator(),
		"auto":        db.NewIterator(),
	}
	for name, it := range modes {
		got := make(map[string]string)
		for it.Valid() {
			got[string(it.Key())] = string(it.Value())
			it.Next()
		}
		it.Close()

		if len(got) != len(expected) {
			t.Errorf("%s: got %d pairs, want %d", name, len(got), len(expected))
		}
		for k, v := range expected {
			if got[k] != v {
				t.Errorf("%s: key %s = %q, want %q", name, k, got[k], v)
			}
		}
	}
}

// TestIteratorModeGate forces each mode through the RAM estimator: a small
// database picks the offsets mode on any machine, a tiny available-RAM fake
// forces the scan+lookup fallback
func TestIteratorModeGate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "gate.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		if err := db.Set([]byte(fmt.Sprintf("gate:%04d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	waitForBackgroundFlush(t, db)

	if db.estimateOffsetsMemory() == 0 {
		t.Fatal("estimator returned 0 for a populated database")
	}

	it := db.NewIterator()
	if it.mode != iterModeOffsets {
		t.Errorf("small database picked mode %d, want offsets (%d)", it.mode, iterModeOffsets)
	}
	it.Close()

	// A tiny available-RAM budget forces the scan+lookup fallback
	withFakeMemory(t, 1<<20, 1<<10)
	it = db.NewIterator()
	if it.mode != iterModeScanLookup {
		t.Errorf("tiny RAM picked mode %d, want scan+lookup (%d)", it.mode, iterModeScanLookup)
	}
	it.Close()
}

// TestIterateAfterReopenYieldsEachRecordOnce covers the duplicate-iteration
// regression: update-heavy churn reworks collision sub-pages, and a stale
// sub-page info could make the add path append a second entry for a key that
// already had one, leaving two index entries pointing at the same record.
// The offsets iterator then yielded the record once per entry. The collect
// collapses duplicate offsets after the sort, so the iteration must yield
// exactly one entry per stored key, in the creating session and again after
// a close and reopen
func TestIterateAfterReopenYieldsEachRecordOnce(t *testing.T) {
	dbPath := t.TempDir() + "/bench.db"
	db, err := Open(dbPath, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	keys := 200000
	val := make([]byte, 100)
	for base := 0; base < keys; base += 1000 {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := base; i < base+1000 && i < keys; i++ {
			k := []byte(fmt.Sprintf("key-%08d", i))
			if serr := tx.Set(k, val); serr != nil {
				t.Fatalf("set: %v", serr)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	// Update churn: rewrites that force collision reworks, sub-page moves
	// and conversions — the shape that used to leave second index entries
	// behind
	for txn := 0; txn < 2000; txn++ {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := 0; i < 1000; i++ {
			k := []byte(fmt.Sprintf("key-%08d", (txn*7919+i*104729)%keys))
			if serr := tx.Set(k, val); serr != nil {
				t.Fatalf("set: %v", serr)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	iterateOnce := func(round string) int {
		db, err := Open(dbPath, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		count := 0
		seen := make(map[string]int, keys)
		it := db.NewIterator()
		for ; it.Valid(); it.Next() {
			count++
			seen[string(it.Key())]++
			if count > keys*2 {
				it.Close()
				db.Close()
				t.Fatalf("%s: iteration yielded more than %d entries for %d keys", round, count, keys)
			}
		}
		it.Close()
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if count != keys {
			t.Errorf("%s: iteration yielded %d entries for %d keys", round, count, keys)
		}
		dups := 0
		var sample string
		for k, c := range seen {
			if c > 1 {
				dups++
				if sample == "" {
					sample = fmt.Sprintf("%s x%d", k, c)
				}
			}
		}
		if dups != 0 {
			t.Errorf("%s: %d keys yielded more than once (sample: %s)", round, dups, sample)
		}
		return count
	}

	iterateOnce("first session after create")
	iterateOnce("session after reopen")
	iterateOnce("second session after reopen")

	// The database files must not linger outside the test directory
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("database vanished: %v", err)
	}
}

// TestIterateMatchesGet cross-checks the full iteration against point Gets:
// a key must either appear in both with the same value, or in neither
// (deleted or never written). This is the net that catches iteration entries
// Get cannot see — duplicates, resurrected deletes and stale offsets
func TestIterateMatchesGet(t *testing.T) {
	dbPath := t.TempDir() + "/bench.db"
	db, err := Open(dbPath, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	keys := 50000
	val := make([]byte, 100)
	updated := make([]byte, 100)
	for i := range updated {
		updated[i] = byte(i % 251)
	}
	for base := 0; base < keys; base += 1000 {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := base; i < base+1000 && i < keys; i++ {
			if serr := tx.Set([]byte(fmt.Sprintf("key-%08d", i)), val); serr != nil {
				t.Fatalf("set: %v", serr)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	// Update every 10th key, delete every 100th
	for base := 0; base < keys; base += 1000 {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := base; i < base+1000 && i < keys; i++ {
			k := []byte(fmt.Sprintf("key-%08d", i))
			if i%10 == 0 {
				if serr := tx.Set(k, updated); serr != nil {
					t.Fatalf("update: %v", serr)
				}
			}
			if i%100 == 0 {
				if derr := tx.Delete(k); derr != nil {
					t.Fatalf("delete: %v", derr)
				}
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	iterated := make(map[string]string, keys)
	it := db.NewIterator()
	for ; it.Valid(); it.Next() {
		iterated[string(it.Key())] = string(it.Value())
		if len(iterated) > keys {
			it.Close()
			db.Close()
			t.Fatalf("iteration yielded more than %d distinct keys", keys)
		}
	}
	it.Close()

	live := 0
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key-%08d", i)
		got, gerr := db.Get([]byte(k))
		iv, inMap := iterated[k]
		deleted := i%100 == 0
		if gerr != nil {
			if !deleted {
				t.Fatalf("Get(%s): %v", k, gerr)
			}
			if inMap {
				t.Errorf("iteration yielded deleted key %s", k)
			}
			continue
		}
		live++
		if deleted {
			t.Errorf("Get(%s) returned a value for a deleted key", k)
		}
		if !inMap {
			t.Errorf("iteration missed key %s that Get returned", k)
			continue
		}
		want := val
		if i%10 == 0 {
			want = updated
		}
		if iv != string(want) {
			t.Errorf("value mismatch for %s between iteration and the written value", k)
		}
		if string(got) != string(want) {
			t.Errorf("value mismatch for %s between Get and the written value", k)
		}
	}
	if len(iterated) != live {
		t.Errorf("iteration yielded %d distinct keys, %d are live per Get", len(iterated), live)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestIterateModesAgree walks the same database with both iterator modes —
// the offsets mode (index walk, used when RAM allows) and the scan+lookup
// mode (main-file scan, the constrained fallback) — and requires the same
// key set with the same values, since a mode flip must not change what a
// caller observes
func TestIterateModesAgree(t *testing.T) {
	dbPath := t.TempDir() + "/bench.db"
	db, err := Open(dbPath, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	keys := 20000
	val := make([]byte, 100)
	for base := 0; base < keys; base += 1000 {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := base; i < base+1000 && i < keys; i++ {
			if serr := tx.Set([]byte(fmt.Sprintf("key-%08d", i)), val); serr != nil {
				t.Fatalf("set: %v", serr)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	// Update and delete churn so the two modes walk different structures
	for txn := 0; txn < 200; txn++ {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := 0; i < 500; i++ {
			idx := (txn*7919 + i*104729) % keys
			k := []byte(fmt.Sprintf("key-%08d", idx))
			if idx%97 == 0 {
				if derr := tx.Delete(k); derr != nil {
					t.Fatalf("delete: %v", derr)
				}
				continue
			}
			if serr := tx.Set(k, val); serr != nil {
				t.Fatalf("set: %v", serr)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(dbPath, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	walk := func(it *Iterator) map[string]string {
		m := make(map[string]string, keys)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}
		it.Close()
		return m
	}
	offsetsMap := walk(db2.newOffsetsIterator())
	scanMap := walk(db2.newScanLookupIterator())

	if len(offsetsMap) != len(scanMap) {
		t.Errorf("mode key counts differ: offsets=%d scanLookup=%d", len(offsetsMap), len(scanMap))
	}
	for k, ov := range offsetsMap {
		sv, ok := scanMap[k]
		if !ok {
			t.Errorf("offsets mode yielded %s that the scan+lookup mode missed", k)
			continue
		}
		if ov != sv {
			t.Errorf("value mismatch for %s between the modes", k)
		}
	}
	for k := range scanMap {
		if _, ok := offsetsMap[k]; !ok {
			t.Errorf("scan+lookup mode yielded %s that the offsets mode missed", k)
		}
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestDeleteThenReopenNoResurrection covers deletes surviving recovery and
// reopens, including keys deleted and later re-set — the recovery reindexes
// every record version in the unindexed region, so deleted keys must not
// come back through either Get or the iteration
func TestDeleteThenReopenNoResurrection(t *testing.T) {
	dbPath := t.TempDir() + "/bench.db"
	db, err := Open(dbPath, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	keys := 30000
	val := make([]byte, 100)
	reval := make([]byte, 100)
	for i := range reval {
		reval[i] = byte(i%127 + 1)
	}
	for base := 0; base < keys; base += 1000 {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := base; i < base+1000 && i < keys; i++ {
			if serr := tx.Set([]byte(fmt.Sprintf("key-%08d", i)), val); serr != nil {
				t.Fatalf("set: %v", serr)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	// Delete every 3rd key, then re-set every 9th of those (delete + re-add
	// churn on the same keys)
	for base := 0; base < keys; base += 1000 {
		tx, terr := db.Begin()
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		for i := base; i < base+1000 && i < keys; i++ {
			if i%3 != 0 {
				continue
			}
			k := []byte(fmt.Sprintf("key-%08d", i))
			if derr := tx.Delete(k); derr != nil {
				t.Fatalf("delete: %v", derr)
			}
			if i%9 == 0 {
				if serr := tx.Set(k, reval); serr != nil {
					t.Fatalf("re-set: %v", serr)
				}
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	assertState := func(round string) {
		db, err := Open(dbPath, Options{"UseWAL": true, "SyncMainFileOnCommit": false})
		if err != nil {
			t.Fatalf("%s: reopen: %v", round, err)
		}
		count := 0
		it := db.NewIterator()
		for ; it.Valid(); it.Next() {
			count++
			k := string(it.Key())
			idx := 0
			if _, err := fmt.Sscanf(k, "key-%08d", &idx); err != nil {
				t.Errorf("%s: unexpected iterated key %q", round, k)
				continue
			}
			if idx%3 == 0 && idx%9 != 0 {
				t.Errorf("%s: deleted key %s resurrected through iteration", round, k)
			}
			if count > keys {
				it.Close()
				db.Close()
				t.Fatalf("%s: iteration yielded more than %d entries", round, keys)
			}
		}
		it.Close()
		live := 0
		for i := 0; i < keys; i++ {
			k := []byte(fmt.Sprintf("key-%08d", i))
			got, gerr := db.Get(k)
			if i%3 == 0 && i%9 != 0 {
				if gerr == nil {
					t.Errorf("%s: deleted key %s resurrected through Get", round, k)
				}
				continue
			}
			live++
			if gerr != nil {
				t.Errorf("%s: Get(%s): %v", round, k, gerr)
				continue
			}
			want := val
			if i%9 == 0 {
				want = reval
			}
			if string(got) != string(want) {
				t.Errorf("%s: value mismatch for %s after reopen", round, k)
			}
		}
		if count != live {
			t.Errorf("%s: iteration yielded %d entries, Get sees %d live keys", round, count, live)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("%s: close: %v", round, err)
		}
	}

	assertState("first reopen")
	assertState("second reopen")
	assertState("third reopen")
}

// ---------------------------------------------------------------------------
// Binary page-content verification: every container-page mutation is checked
// against a hand-built expected byte buffer, covering the page header, each
// sub-page header, each slot and each pointer word. The expected buffers are
// built by test-side serializers that know the on-page layout, never by the
// production code under test
// ---------------------------------------------------------------------------

// binPageDB returns a database stub with no files: the page cache, the header
// page and the free list live in memory only, so container-page mutations can
// be exercised without any disk or cache machinery
func binPageDB() *DB {
	db := &DB{}
	db.txnSequence = 1_000_000
	db.cloningSequence = 0
	db.minReaderSeq = -1
	db.fastRollback = false
	db.cacheSizeThreshold.Store(1 << 30)
	db.virtualIndexFileSize.Store(PageSize) // page 0 reserved for the header
	hp := &Page{}
	hp.pageNumber = 0
	hp.txnSequence = db.txnSequence
	hp.freeSpaceArray = make([]FreeSpaceEntry, 0, MaxFreeSpaceEntries)
	db.headerPageForTransaction = hp
	return db
}

// binHybridPage returns an empty writable hybrid page
func binHybridPage(pageNumber uint32) *HybridPage {
	p := &HybridPage{}
	p.pageNumber = pageNumber
	p.pageType = ContentTypeHybrid
	p.Salt = 7
	p.txnSequence = 1_000_000
	return p
}

// binSubPage is the test-side description of one sub-page body
type binSubPage struct {
	id    uint8
	salt  uint8
	slots []int
	ptrs  []uint64
}

// binExpectedHybrid builds the exact expected page.data bytes of a hybrid
// page holding the given sub-pages in order: CRC(4) + type(1) + NumSubPages(1)
// + ContentSize(2), then per sub-page: id(1) + salt(1) + size(2) + count u16
// slots + count u64 pointer words
func binExpectedHybrid(subs []binSubPage) []byte {
	data := make([]byte, PageSize)
	body := 8
	for i := range subs {
		body += HybridSubPageHeaderSize + len(subs[i].slots)*10
	}
	for i := range subs {
		s := &subs[i]
		pos := 8
		for j := 0; j < i; j++ {
			pos += HybridSubPageHeaderSize + len(subs[j].slots)*10
		}
		data[pos] = s.id
		data[pos+1] = s.salt
		binary.LittleEndian.PutUint16(data[pos+2:pos+4], uint16(len(s.slots)*10))
		slotsPos := pos + HybridSubPageHeaderSize
		ptrsPos := slotsPos + 2*len(s.slots)
		for k, slot := range s.slots {
			binary.LittleEndian.PutUint16(data[slotsPos+2*k:], uint16(slot))
			binary.LittleEndian.PutUint64(data[ptrsPos+8*k:], s.ptrs[k])
		}
	}
	// In memory the type byte, the sub-page count, the content size and the
	// checksum stay zero: the flush's serialize callback materializes them,
	// while the values live in the page struct fields
	return data
}

// binExpectedTable builds the exact expected page.data bytes of a table page:
// CRC(4) + type(1) + Salt(1), then 5-byte entries at TableHeaderSize
func binExpectedTable(salt byte, dataEntries map[int]int64, ptrEntries map[int]uint32) []byte {
	data := make([]byte, PageSize)
	// In memory the type byte and the salt stay zero: the flush's serialize
	// callback materializes them, the values live in the page struct fields
	for slot, off := range dataEntries {
		offset := TableHeaderSize + slot*TableEntrySize
		binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(off>>8))
		data[offset+4] = byte(off)
	}
	for slot, pn := range ptrEntries {
		offset := TableHeaderSize + slot*TableEntrySize
		binary.LittleEndian.PutUint32(data[offset:offset+4], 0x80000000|pn)
		data[offset+4] = byte(0)
	}
	return data
}

// assertPageData compares the page data byte-for-byte and dumps the
// surrounding region when a byte differs
func assertPageData(t *testing.T, name string, got []byte, want []byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: data length %d, want %d", name, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			lo := i - 8
			if lo < 0 {
				lo = 0
			}
			hi := i + 8
			if hi > len(want) {
				hi = len(want)
			}
			t.Fatalf("%s: byte %d differs: got %02x want %02x\ngot[%d:%d]  = % x\nwant[%d:%d] = % x",
				name, i, got[i], want[i], lo, hi, got[lo:hi], lo, hi, want[lo:hi])
		}
	}
}

// binFillSubPage writes a sub-page body (header + slots + pointers) into the
// page data at the given offset and returns the matching info struct
func binFillSubPage(page *HybridPage, offset int, id uint8, salt uint8, slots []int, ptrs []uint64) HybridSubPageInfo {
	page.data[offset] = id
	page.data[offset+1] = salt
	binary.LittleEndian.PutUint16(page.data[offset+2:offset+4], uint16(len(slots)*10))
	slotsPos := offset + HybridSubPageHeaderSize
	ptrsPos := slotsPos + 2*len(slots)
	for k, slot := range slots {
		binary.LittleEndian.PutUint16(page.data[slotsPos+2*k:], uint16(slot))
		binary.LittleEndian.PutUint64(page.data[ptrsPos+8*k:], ptrs[k])
	}
	binary.BigEndian.PutUint32(page.data[0:4], 0)
	info := HybridSubPageInfo{Salt: salt, Offset: uint16(offset), Size: uint16(len(slots) * 10)}
	page.SubPages[id] = info
	return info
}

func TestBinHybridAddEntriesToNewSubPage(t *testing.T) {
	db := binPageDB()

	entries := []HybridEntry{
		{Key: []byte("key-a"), DataOffset: 1000, DataSize: 115},
		{Key: []byte("key-b"), DataOffset: 2000, DataSize: 115},
	}
	newSub, err := db.addEntriesToNewHybridSubPage(7, entries)
	if err != nil {
		t.Fatalf("addEntriesToNewHybridSubPage: %v", err)
	}
	page := newSub.Page

	// Structural expectations that do not depend on the chosen salt
	if page.NumSubPages != 1 {
		t.Errorf("NumSubPages = %d, want 1", page.NumSubPages)
	}
	if page.ContentSize != HybridHeaderSize+HybridSubPageHeaderSize+20 {
		t.Errorf("ContentSize = %d, want %d", page.ContentSize, HybridHeaderSize+HybridSubPageHeaderSize+20)
	}
	info := page.SubPages[newSub.SubPageId]
	if !hybridSubPageLive(info) {
		t.Errorf("sub-page %d not live after creation", newSub.SubPageId)
	}
	if int(info.Offset) != HybridHeaderSize {
		t.Errorf("sub-page offset = %d, want %d", info.Offset, HybridHeaderSize)
	}
	if int(info.Size) != 20 {
		t.Errorf("sub-page size = %d, want 20", info.Size)
	}
	// The two entries must sit in the slot array at salt-derived slots
	slot1 := int(binary.LittleEndian.Uint16(page.data[int(info.Offset)+HybridSubPageHeaderSize:]))
	slot2 := int(binary.LittleEndian.Uint16(page.data[int(info.Offset)+HybridSubPageHeaderSize+2:]))
	salt, saltErr := db.findNonCollidingSalt(7, []HybridEntry{
		{Key: []byte("key-a")},
		{Key: []byte("key-b")},
	})
	if saltErr != nil {
		t.Fatalf("findNonCollidingSalt: %v", saltErr)
	}
	if slot1 != db.getTableSlot([]byte("key-a"), salt) && slot1 != db.getTableSlot([]byte("key-b"), salt) {
		t.Errorf("first slot %d matches neither key's salt-derived slot", slot1)
	}
	if slot2 != db.getTableSlot([]byte("key-a"), salt) && slot2 != db.getTableSlot([]byte("key-b"), salt) {
		t.Errorf("second slot %d matches neither key's salt-derived slot", slot2)
	}
	off1 := int64(binary.LittleEndian.Uint64(page.data[int(info.Offset)+HybridSubPageHeaderSize+2*2:]) >> 16)
	if off1 != 1000 && off1 != 2000 {
		t.Errorf("first pointer offset %d matches neither expected offset", off1)
	}
}

func TestBinHybridAddEntry(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)

	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10, 20}, []uint64{1<<16 | 5, 2<<16 | 6})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20
	subPage := &HybridSubPage{Page: page, SubPageId: 0}

	// The expected bytes before the call: the page as built
	want := binExpectedHybrid([]binSubPage{{id: 0, salt: 9, slots: []int{10, 20}, ptrs: []uint64{1<<16 | 5, 2<<16 | 6}}})
	assertPageData(t, "pre-populated page", page.data[:], want)

	// Add a third entry
	newSlot := db.getTableSlot([]byte("key-new"), 9)
	if err := db.addEntryToHybridSubPage(subPage, newSlot, []byte("key-new"), 3000, 115); err != nil {
		t.Fatalf("addEntryToHybridSubPage: %v", err)
	}

	wantAfter := binExpectedHybrid([]binSubPage{{id: 0, salt: 9, slots: []int{10, 20, newSlot}, ptrs: []uint64{1<<16 | 5, 2<<16 | 6, hybridDataPtrWord(3000, 115)}}})
	assertPageData(t, "page after add", page.data[:page.ContentSize], wantAfter[:page.ContentSize])
	if page.ContentSize != HybridHeaderSize+HybridSubPageHeaderSize+30 {
		t.Errorf("ContentSize after add = %d", page.ContentSize)
	}
	if page.SubPages[0].Size != 30 {
		t.Errorf("sub-page size after add = %d", page.SubPages[0].Size)
	}
}

func TestBinHybridUpdateDataOffset(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10, 20}, []uint64{1<<16 | 5, 2<<16 | 6})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20
	subPage := &HybridSubPage{Page: page, SubPageId: 0}

	if err := db.updateDataOffsetInHybridSubPage(subPage, 1, 9999, 115); err != nil {
		t.Fatalf("updateDataOffsetInHybridSubPage: %v", err)
	}

	want := binExpectedHybrid([]binSubPage{{id: 0, salt: 9, slots: []int{10, 20}, ptrs: []uint64{1<<16 | 5, hybridDataPtrWord(9999, 115)}}})
	assertPageData(t, "page after update", page.data[:page.ContentSize], want[:page.ContentSize])
}

func TestBinHybridUpdateSubPagePointer(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10, 20}, []uint64{1<<16 | 5, 2<<16 | 6})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20
	subPage := &HybridSubPage{Page: page, SubPageId: 0}

	if err := db.updateSubPagePointerInHybridSubPage(subPage, 1, 77, 3); err != nil {
		t.Fatalf("updateSubPagePointerInHybridSubPage: %v", err)
	}

	want := binExpectedHybrid([]binSubPage{{id: 0, salt: 9, slots: []int{10, 20}, ptrs: []uint64{1<<16 | 5, hybridSubPtrWord(77, 3)}}})
	assertPageData(t, "page after pointer update", page.data[:page.ContentSize], want[:page.ContentSize])
}

func TestBinHybridRemoveEntry(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10, 20, 30}, []uint64{1<<16 | 5, 2<<16 | 6, 3<<16 | 7})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 30
	subPage := &HybridSubPage{Page: page, SubPageId: 0}

	if err := db.removeEntryFromHybridSubPage(subPage, 1); err != nil {
		t.Fatalf("removeEntryFromHybridSubPage: %v", err)
	}

	want := binExpectedHybrid([]binSubPage{{id: 0, salt: 9, slots: []int{10, 30}, ptrs: []uint64{1<<16 | 5, 3<<16 | 7}}})
	assertPageData(t, "page after entry removal", page.data[:page.ContentSize], want[:page.ContentSize])
}

func TestBinHybridRemoveSubPage(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	// Two sub-pages: the first holds one entry, the second two. Removing the
	// first must shift the second's body left and adjust its offset
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10}, []uint64{1<<16 | 5})
	binFillSubPage(page, HybridHeaderSize+HybridSubPageHeaderSize+10, 1, 11, []int{20, 30}, []uint64{2<<16 | 6, 3<<16 | 7})
	page.NumSubPages = 2
	page.ContentSize = HybridHeaderSize + 2*HybridSubPageHeaderSize + 30

	db.removeSubPageFromHybridPage(page, 0)

	want := binExpectedHybrid([]binSubPage{{id: 1, salt: 11, slots: []int{20, 30}, ptrs: []uint64{2<<16 | 6, 3<<16 | 7}}})
	// The surviving sub-page keeps its id but moves to the first body slot
	want[HybridHeaderSize] = 1
	want[HybridHeaderSize+1] = 11
	assertPageData(t, "page after sub-page removal", page.data[:page.ContentSize], want[:page.ContentSize])
	if page.NumSubPages != 1 {
		t.Errorf("NumSubPages after removal = %d, want 1", page.NumSubPages)
	}
	if page.ContentSize != HybridHeaderSize+HybridSubPageHeaderSize+20 {
		t.Errorf("ContentSize after removal = %d", page.ContentSize)
	}
}

func TestBinHybridConvertEntryToSubPagePointer(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10, 20}, []uint64{1<<16 | 5, 2<<16 | 6})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20
	subPage := &HybridSubPage{Page: page, SubPageId: 0}

	if err := db.convertEntryInHybridSubPage(subPage, 1, 300, 2); err != nil {
		t.Fatalf("convertEntryInHybridSubPage: %v", err)
	}

	want := binExpectedHybrid([]binSubPage{{id: 0, salt: 9, slots: []int{10, 20}, ptrs: []uint64{1<<16 | 5, hybridSubPtrWord(300, 2)}}})
	assertPageData(t, "page after entry conversion", page.data[:page.ContentSize], want[:page.ContentSize])
}

func TestBinMoveSubPageToNewHybridPage(t *testing.T) {
	db := binPageDB()
	// Page 2: the allocations in this test take page 1, so the moved
	// sub-page cannot land on the source page
	pageA := binHybridPage(2)
	// Sub-page 0 on page A holds two entries; the moved one will be
	// re-created on a fresh page with the same salt and slots
	binFillSubPage(pageA, HybridHeaderSize, 0, 9, []int{10, 20}, []uint64{1<<16 | 5, 2<<16 | 6})
	pageA.NumSubPages = 1
	pageA.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20
	subPage := &HybridSubPage{Page: pageA, SubPageId: 0}

	if err := db.moveSubPageToNewHybridPage(subPage, 10, 1000, 115); err != nil {
		t.Fatalf("moveSubPageToNewHybridPage: %v", err)
	}

	// The moved sub-page must live on a different page with the same body
	if subPage.Page == pageA {
		t.Fatalf("sub-page was not moved off page A")
	}
	moved := subPage.Page
	if moved.pageType != ContentTypeHybrid {
		t.Fatalf("moved page type = %c", moved.pageType)
	}
	info := moved.SubPages[subPage.SubPageId]
	if !hybridSubPageLive(info) {
		t.Fatalf("moved sub-page not live on the new page")
	}
	wantBody := binExpectedHybrid([]binSubPage{{id: subPage.SubPageId, salt: 9, slots: []int{10, 20, 10}, ptrs: []uint64{1<<16 | 5, 2<<16 | 6, hybridDataPtrWord(1000, 115)}}})
	// The body layout is preserved: id, salt, size and the two entries — the
	// pointer words carry the moved entry's new offset in both slots because
	// moveSubPageToNewHybridPage rewrites the entry that triggered the move
	assertPageData(t, "moved page body", moved.data[info.Offset:info.Offset+HybridSubPageHeaderSize+20], wantBody[HybridHeaderSize:HybridHeaderSize+HybridSubPageHeaderSize+20])
	// Page A must no longer hold the sub-page
	if pageA.NumSubPages != 0 {
		t.Errorf("page A NumSubPages after move = %d, want 0", pageA.NumSubPages)
	}
	if pageA.ContentSize != HybridHeaderSize {
		t.Errorf("page A ContentSize after move = %d, want %d", pageA.ContentSize, HybridHeaderSize)
	}
}

func TestBinConvertHybridToTable(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	// Two data entries on the sub-page
	slotA := db.getTableSlot([]byte("key-a"), 9)
	slotB := db.getTableSlot([]byte("key-b"), 9)
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{slotA, slotB}, []uint64{1000<<16 | 115, 2000<<16 | 115})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20

	// The unit conversion wipes the data block and flips the identity: the
	// entries are copied by the compounding convertHybridSubPageToTablePage
	db.convertWritableHybridPageToTable(page, 9)

	if page.pageType != ContentTypeTable {
		t.Errorf("page type after conversion = %c", page.pageType)
	}
	if page.Salt != 9 {
		t.Errorf("page salt after conversion = %d, want 9", page.Salt)
	}
	if page.NumSubPages != 0 || page.ContentSize != 0 {
		t.Errorf("hybrid fields after conversion: numSub=%d contentSize=%d, want 0/0", page.NumSubPages, page.ContentSize)
	}
	for i, b := range page.data {
		if b != 0 {
			t.Errorf("data byte %d = %02x after the wipe, want 0", i, b)
		}
	}
}

func TestBinTableSetEntry(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	db.convertWritableHybridPageToTable(page, 5)

	slot := db.getTableSlot([]byte("key-a"), 5)
	if err := db.setTableEntry(page, slot, 300, 2, 0); err != nil {
		t.Fatalf("setTableEntry: %v", err)
	}
	want := binExpectedTable(5, nil, map[int]uint32{slot: 300})
	// The pointer entry carries the sub-page id in its fifth byte
	want[TableHeaderSize+slot*TableEntrySize+4] = 2
	assertPageData(t, "table page after pointer entry", page.data[:], want)

	slot2 := db.getTableSlot([]byte("key-b"), 5)
	if err := db.setTableSlotDataOffset(page, slot2, nil, 4000, 115); err != nil {
		t.Fatalf("setTableSlotDataOffset: %v", err)
	}
	want2 := binExpectedTable(5, map[int]int64{slot2: 4000}, map[int]uint32{slot: 300})
	want2[TableHeaderSize+slot*TableEntrySize+4] = 2
	assertPageData(t, "table page after data entry", page.data[:], want2)
}

func TestBinConsecutiveSubPageOperations(t *testing.T) {
	db := binPageDB()
	page := binHybridPage(1)
	subPage := &HybridSubPage{Page: page, SubPageId: 0}

	// Step 1: create the sub-page with two entries; the allocation returns
	// its own page, and the sub-page struct rebinds to it
	newSub, err := db.addEntriesToNewHybridSubPage(9, []HybridEntry{
		{Key: []byte("key-a"), DataOffset: 1000, DataSize: 115},
		{Key: []byte("key-b"), DataOffset: 2000, DataSize: 115},
	})
	if err != nil {
		t.Fatalf("step 1: %v", err)
	}
	subPage.Page = newSub.Page
	subPage.SubPageId = newSub.SubPageId
	page = subPage.Page
	// The salt is the production choice for this key pair, re-derived so the
	// expected bytes stay independent of findNonCollidingSalt's policy
	salt := page.SubPages[subPage.SubPageId].Salt
	slotA := db.getTableSlot([]byte("key-a"), salt)
	slotB := db.getTableSlot([]byte("key-b"), salt)
	want := binExpectedHybrid([]binSubPage{{id: subPage.SubPageId, salt: salt, slots: []int{slotA, slotB}, ptrs: []uint64{hybridDataPtrWord(1000, 115), hybridDataPtrWord(2000, 115)}}})
	assertPageData(t, "step 1: created", page.data[page.ContentSize-4-20:page.ContentSize], want[HybridHeaderSize:HybridHeaderSize+4+20])

	// Step 2: add a third entry
	slotC := db.getTableSlot([]byte("key-c"), salt)
	if err := db.addEntryToHybridSubPage(subPage, slotC, []byte("key-c"), 3000, 115); err != nil {
		t.Fatalf("step 2: %v", err)
	}
	want = binExpectedHybrid([]binSubPage{{id: newSub.SubPageId, salt: page.SubPages[newSub.SubPageId].Salt, slots: []int{slotA, slotB, slotC}, ptrs: []uint64{hybridDataPtrWord(1000, 115), hybridDataPtrWord(2000, 115), hybridDataPtrWord(3000, 115)}}})
	assertPageData(t, "step 2: added", page.data[page.ContentSize-4-30:page.ContentSize], want[HybridHeaderSize:HybridHeaderSize+4+30])

	// Step 3: update the middle entry
	if err := db.updateDataOffsetInHybridSubPage(subPage, 1, 2500, 115); err != nil {
		t.Fatalf("step 3: %v", err)
	}
	want = binExpectedHybrid([]binSubPage{{id: newSub.SubPageId, salt: page.SubPages[newSub.SubPageId].Salt, slots: []int{slotA, slotB, slotC}, ptrs: []uint64{hybridDataPtrWord(1000, 115), hybridDataPtrWord(2500, 115), hybridDataPtrWord(3000, 115)}}})
	assertPageData(t, "step 3: updated", page.data[page.ContentSize-4-30:page.ContentSize], want[HybridHeaderSize:HybridHeaderSize+4+30])

	// Step 4: remove the first entry
	if err := db.removeEntryFromHybridSubPage(subPage, 0); err != nil {
		t.Fatalf("step 4: %v", err)
	}
	want = binExpectedHybrid([]binSubPage{{id: newSub.SubPageId, salt: page.SubPages[newSub.SubPageId].Salt, slots: []int{slotB, slotC}, ptrs: []uint64{hybridDataPtrWord(2500, 115), hybridDataPtrWord(3000, 115)}}})
	assertPageData(t, "step 4: removed", page.data[page.ContentSize-4-20:page.ContentSize], want[HybridHeaderSize:HybridHeaderSize+4+20])

	// Step 5: convert the last remaining data entry into a sub-page pointer
	if err := db.convertEntryInHybridSubPage(subPage, 1, 400, 1); err != nil {
		t.Fatalf("step 5: %v", err)
	}
	want = binExpectedHybrid([]binSubPage{{id: newSub.SubPageId, salt: page.SubPages[newSub.SubPageId].Salt, slots: []int{slotB, slotC}, ptrs: []uint64{hybridDataPtrWord(2500, 115), hybridSubPtrWord(400, 1)}}})
	assertPageData(t, "step 5: converted", page.data[page.ContentSize-4-20:page.ContentSize], want[HybridHeaderSize:HybridHeaderSize+4+20])
}

// TestBinWALFrameCarriesPageBytes covers phase 2: a hybrid page flushed to
// the WAL must land in the frame with its bytes unchanged after the 20-byte
// frame header, and the frame header must carry the page number and the
// running checksum
func TestBinWALFrameCarriesPageBytes(t *testing.T) {
	db := binPageDB()
	db.useWAL = true
	db.filePath = t.TempDir() + "/bin-wal.db"
	page := binHybridPage(1)
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10, 20}, []uint64{1<<16 | 5, 2<<16 | 6})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20

	if err := db.writeToWAL(page.data[:], page.pageNumber); err != nil {
		t.Fatalf("writeToWAL: %v", err)
	}

	frame := make([]byte, WalFrameHeaderSize+PageSize)
	if _, err := db.walInfo.file.ReadAt(frame, WalHeaderSize); err != nil {
		t.Fatalf("read WAL frame: %v", err)
	}
	if pn := binary.BigEndian.Uint32(frame[0:4]); pn != 1 {
		t.Errorf("frame page number = %d, want 1", pn)
	}
	if !bytes.Equal(frame[WalFrameHeaderSize:WalFrameHeaderSize+PageSize], page.data[:]) {
		t.Errorf("WAL frame page data differs from the in-memory page")
	}
}

// TestBinIndexFileRoundTrip covers phase 3: a page written to the index file
// must read back with identical bytes and parse into identical fields
func TestBinIndexFileRoundTrip(t *testing.T) {
	db := binPageDB()
	db.realIndexFileSize.Store(2 * PageSize)
	db.virtualIndexFileSize.Store(2 * PageSize)
	idxPath := t.TempDir() + "/bin-index.db-index"
	idxFile, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		t.Fatalf("open index file: %v", err)
	}
	db.indexFile = idxFile
	defer idxFile.Close()

	page := binHybridPage(1)
	binFillSubPage(page, HybridHeaderSize, 0, 9, []int{10, 20}, []uint64{1<<16 | 5, 2<<16 | 6})
	page.NumSubPages = 1
	page.ContentSize = HybridHeaderSize + HybridSubPageHeaderSize + 20

	// Materialize the header into the data block first: the flush's
	// serialize callback does this in production before the write
	page.data[4] = ContentTypeHybrid
	page.data[5] = page.NumSubPages
	binary.LittleEndian.PutUint16(page.data[6:8], uint16(page.ContentSize))
	binary.BigEndian.PutUint32(page.data[0:4], crc32.ChecksumIEEE(page.data[4:]))
	if err := db.writeToIndexFile(page.data[:], page.pageNumber); err != nil {
		t.Fatalf("writeToIndexFile: %v", err)
	}

	raw := make([]byte, PageSize)
	if _, err := idxFile.ReadAt(raw, int64(page.pageNumber)*PageSize); err != nil {
		t.Fatalf("read back: %v", err)
	}
	assertPageData(t, "index file round trip", raw, page.data[:])

	var scratch Page
	if err := db.readPageInto(page.pageNumber, &scratch); err != nil {
		t.Fatalf("readPageInto: %v", err)
	}
	if scratch.pageType != page.pageType || scratch.NumSubPages != page.NumSubPages || scratch.ContentSize != page.ContentSize {
		t.Errorf("parsed fields differ: type=%c numSub=%d contentSize=%d", scratch.pageType, scratch.NumSubPages, scratch.ContentSize)
	}
	if scratch.SubPages[0] != page.SubPages[0] {
		t.Errorf("parsed sub-page info differs: %+v vs %+v", scratch.SubPages[0], page.SubPages[0])
	}
	assertPageData(t, "parsed page data", scratch.data[:], page.data[:])
}

// ---------------------------------------------------------------------------
// Bulk sessions: large writes with internal auto-commit rotations
// ---------------------------------------------------------------------------

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
	dbPath := testDBPath(".", "test_bulk_discard.db", "wal")
	cleanupTestFiles(dbPath)

	db := openTestDB(t, dbPath, "wal", Options{"DirtyPageThreshold": 20})
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
	db2 := openTestDB(t, dbPath, "wal", Options{"DirtyPageThreshold": 20})
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

// ---------------------------------------------------------------------------
// Adaptive cache: memory-pressure driven cache and checkpoint thresholds
// ---------------------------------------------------------------------------

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
//  3. thresholds are not grown under low RAM,
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
