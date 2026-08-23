package hashtabledb

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
)

func TestMainFileMmapBasic(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("main file mmap is only implemented on linux")
	}

	dbPath := "test_main_mmap.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := Open(dbPath, Options{
		"UseMmap":              true,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	key := []byte("mmap-key")
	value := []byte("mmap-value-with-some-content")
	if err := db.Set(key, value); err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value: got %q, want %q", got, value)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = Open(dbPath, Options{
		"ReadOnly":             true,
		"UseMmap":              true,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db.Close()

	if m := db.mainMmap.Load(); m == nil {
		t.Fatal("expected main file mmap to be active")
	}

	got, err = db.Get(key)
	if err != nil {
		t.Fatalf("failed to get key after reopen: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value after reopen: got %q, want %q", got, value)
	}
}

func TestMainFileMmapGrowthRemap(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("main file mmap is only implemented on linux")
	}

	dbPath := "test_main_mmap_growth.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	// Fixed reservation small enough that writes cross it several times
	db, err := Open(dbPath, Options{
		"UseMmap":              true,
		"MmapSize":             int64(64 * 1024),
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	const numKeys = 2000
	value := make([]byte, 512)
	for i := range value {
		value[i] = byte(i)
	}
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("growth-key-%06d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("failed to set key %d: %v", i, err)
		}
	}

	if m := db.mainMmap.Load(); m == nil {
		t.Fatal("expected main file mmap to be active")
	}
	fileSize := db.mainFileSize.Load()
	if int64(len(*db.mainMmap.Load())) < fileSize {
		t.Fatalf("mapping length %d smaller than file size %d", len(*db.mainMmap.Load()), fileSize)
	}
	if len(db.staleMainMmaps) == 0 {
		t.Fatal("expected replaced mappings after growth")
	}

	// Verify every key through the mapping after all the remaps
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("growth-key-%06d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("failed to get key %d: %v", i, err)
		}
		if string(got) != string(value) {
			t.Fatalf("unexpected value at key %d", i)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	// Reopen with the default auto-sized mapping and verify again
	db, err = Open(dbPath, Options{
		"UseMmap":              true,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db.Close()

	for i := 0; i < numKeys; i += 97 {
		key := []byte(fmt.Sprintf("growth-key-%06d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("failed to get key %d after reopen: %v", i, err)
		}
		if string(got) != string(value) {
			t.Fatalf("unexpected value at key %d after reopen", i)
		}
	}
}

func TestMainFileMmapConcurrentReadersDuringGrowth(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("main file mmap is only implemented on linux")
	}

	dbPath := "test_main_mmap_concurrent.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := Open(dbPath, Options{
		"UseMmap":              true,
		"MmapSize":             int64(64 * 1024),
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	value := []byte("concurrent-mmap-value")
	if err := db.Set([]byte("seed-key"), value); err != nil {
		t.Fatalf("failed to set seed key: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := db.Get([]byte("seed-key"))
				if err != nil {
					t.Errorf("concurrent get failed: %v", err)
					return
				}
				if string(got) != string(value) {
					t.Errorf("concurrent get mismatch: %q", got)
					return
				}
			}
		}()
	}

	bigValue := make([]byte, 4096)
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("grow-key-%06d", i))
		if err := db.Set(key, bigValue); err != nil {
			t.Fatalf("failed to set grow key %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()

	if len(db.staleMainMmaps) == 0 {
		t.Fatal("expected replaced mappings after concurrent growth")
	}
}

func TestMainFileMmapFallbackBeyondMappedRegion(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("main file mmap is only implemented on linux")
	}

	dbPath := "test_main_mmap_fallback.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := Open(dbPath, Options{
		"UseMmap":              true,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	key := []byte("fallback-key")
	value := []byte("fallback-value")
	if err := db.Set(key, value); err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	// Simulate a mapping that does not cover the record: the read must fall
	// back to ReadAt instead of failing
	full := *db.mainMmap.Load()
	short := full[:1]
	db.mainMmap.Store(&short)

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("failed to get key after fallback: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value after fallback: got %q, want %q", got, value)
	}

	db.mainMmap.Store(&full)
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestMainFileMmapFitGateSkipsLargeFile(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("main file mmap is only implemented on linux")
	}

	dbPath := "test_main_mmap_fitgate.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	// Simulate a host with little available memory
	orig := getSystemMemoryInfo
	getSystemMemoryInfo = func() MemoryInfo {
		return MemoryInfo{Total: 4 << 30, Available: 4 << 30}
	}
	defer func() { getSystemMemoryInfo = orig }()

	db, err := Open(dbPath, Options{
		"UseMmap":              true,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if got := db.mainMmapMaxSize; got != mainMmapFitLimit(4<<30) {
		t.Fatalf("unexpected fit limit: got %d", got)
	}

	key := []byte("fit-key")
	value := []byte("fit-value")
	if err := db.Set(key, value); err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	// Tiny file still fits: mapping must be active
	if m := db.mainMmap.Load(); m == nil {
		t.Fatal("expected main file mmap to be active for a small file")
	}
	got, err := db.Get(key)
	if err != nil || string(got) != string(value) {
		t.Fatalf("unexpected get result: %q %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	// Shrink the fit limit below the file size and reopen: mapping must be
	// skipped and reads must go through ReadAt
	getSystemMemoryInfo = func() MemoryInfo {
		return MemoryInfo{Total: 4 << 30, Available: PageSize}
	}
	db, err = Open(dbPath, Options{
		"UseMmap":              true,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db.Close()

	if m := db.mainMmap.Load(); m != nil {
		t.Fatal("expected main file mmap to be skipped when the file exceeds the fit limit")
	}
	got, err = db.Get(key)
	if err != nil {
		t.Fatalf("failed to get key without mmap: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value without mmap: got %q", got)
	}
}

func TestMainFileMmapRetiredWhenOutgrowingFitLimit(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("main file mmap is only implemented on linux")
	}

	dbPath := "test_main_mmap_retire.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	// 512 MiB available -> fit limit ~410 MiB. Auto mode caps the mapping
	// length at the fit limit, so appends past it retire the mapping and
	// reads fall back to ReadAt.
	orig := getSystemMemoryInfo
	getSystemMemoryInfo = func() MemoryInfo {
		return MemoryInfo{Total: 1 << 30, Available: 512 << 20}
	}
	defer func() { getSystemMemoryInfo = orig }()

	db, err := Open(dbPath, Options{
		"UseMmap":              true,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if m := db.mainMmap.Load(); m == nil {
		t.Fatal("expected main file mmap to be active at open")
	}

	value := make([]byte, 64<<10)
	for i := range value {
		value[i] = byte(i)
	}
	expected := mainMmapFitLimit(512 << 20)
	var i int
	for db.mainMmap.Load() != nil {
		key := []byte(fmt.Sprintf("retire-key-%08d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("failed to set key %d: %v", i, err)
		}
		i++
		if i > 20000 {
			t.Fatal("mapping was never retired after outgrowing the fit limit")
		}
	}
	if db.mainFileSize.Load() <= expected {
		t.Fatalf("file size %d did not exceed fit limit %d", db.mainFileSize.Load(), expected)
	}

	// Reads of data written before and after retirement must both work
	for j := 0; j <= i; j += 127 {
		key := []byte(fmt.Sprintf("retire-key-%08d", j))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("failed to get key %d: %v", j, err)
		}
		if len(got) != len(value) {
			t.Fatalf("unexpected value length at key %d: %d", j, len(got))
		}
	}
}

func TestComputeMainMmapSize(t *testing.T) {
	tests := []struct {
		fileSize int64
		want     int64
	}{
		{100 << 20, 4 << 30},
		{512 << 20, 4 << 30},
		{1 << 30, 4 << 30},
		{1500 << 20, 4 * 1500 << 20},
		{2 << 30, 8 << 30},
	}

	for _, tc := range tests {
		db := &DB{}
		db.mainFileSize.Store(tc.fileSize)
		got := db.computeMainMmapSize(tc.fileSize)
		if got != tc.want {
			t.Fatalf("computeMainMmapSize(%d) = %d, want %d", tc.fileSize, got, tc.want)
		}
	}
}

func TestComputeMainMmapSizeReadOnlyAndConfigured(t *testing.T) {
	pageSize := int64(os.Getpagesize())

	// Read-only auto mode caps the mapping at the aligned file size
	fileSize := int64(2 << 30)
	db := &DB{readOnly: true}
	db.mainFileSize.Store(fileSize)
	got := db.computeMainMmapSize(fileSize)
	want := (fileSize + pageSize - 1) / pageSize * pageSize
	if got != want {
		t.Fatalf("computeMainMmapSize(read-only 2GB) = %d, want %d", got, want)
	}

	// An explicit reservation wins over the auto size
	configured := int64(16 << 30)
	db = &DB{mainMmapReservation: configured}
	db.mainFileSize.Store(500 << 20)
	if got = db.computeMainMmapSize(500 << 20); got != configured {
		t.Fatalf("computeMainMmapSize(configured) = %d, want %d", got, configured)
	}

	// A configured size below the current file is raised to cover the file
	db = &DB{mainMmapReservation: 1 << 20}
	db.mainFileSize.Store(500 << 20)
	got = db.computeMainMmapSize(500 << 20)
	want = (500 << 20 + pageSize - 1) / pageSize * pageSize
	if got != want {
		t.Fatalf("computeMainMmapSize(too small config) = %d, want %d", got, want)
	}
}
