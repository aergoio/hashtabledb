package hashtabledb

import (
	"os"
	"runtime"
	"testing"
)

func TestMainFileMmapReadContent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("main file mmap is only implemented on linux")
	}

	dbPath := "test_main_mmap.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := Open(dbPath, Options{
		"UseMmap":              true,
		"ValueCacheThreshold":  0,
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
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db, err = Open(dbPath, Options{
		"ReadOnly":             true,
		"UseMmap":              true,
		"ValueCacheThreshold":  0,
		"AdaptiveCacheEnabled": false,
	})
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db.Close()

	if db.mainMmap == nil {
		t.Fatal("expected main file mmap to be active")
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value: got %q, want %q", got, value)
	}

	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("failed to stat main file: %v", err)
	}
	expectedMmapSize := expectedMainMmapLength(info.Size(), true, 0)
	if int64(len(db.mainMmap)) != expectedMmapSize {
		t.Fatalf("unexpected mmap size: got %d, want %d", len(db.mainMmap), expectedMmapSize)
	}
}

func expectedMainMmapLength(fileSize int64, readOnly bool, configuredSize int64) int64 {
	db := &DB{
		mainFileSize:     fileSize,
		readOnly:         readOnly,
		mainFileMmapSize: configuredSize,
	}
	mmapSize := db.effectiveMainMmapSize()
	pageSize := int64(os.Getpagesize())
	if mmapSize%pageSize != 0 {
		mmapSize = ((mmapSize / pageSize) + 1) * pageSize
	}
	return mmapSize
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
		"ValueCacheThreshold":  0,
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

	// Simulate a read beyond the fixed mmap region while still within the file.
	fullMmap := db.mainMmap
	db.mainMmap = fullMmap[:1]

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("failed to get key after fallback: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value after fallback: got %q, want %q", got, value)
	}

	db.mainMmap = fullMmap
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
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
		got := computeMainMmapSize(tc.fileSize)
		if got != tc.want {
			t.Fatalf("computeMainMmapSize(%d) = %d, want %d", tc.fileSize, got, tc.want)
		}
	}
}

func TestEffectiveMainMmapSizeReadOnly(t *testing.T) {
	pageSize := int64(os.Getpagesize())
	fileSize := int64(2 << 30)

	db := &DB{mainFileSize: fileSize, readOnly: true}
	got := db.effectiveMainMmapSize()
	want := (fileSize + pageSize - 1) / pageSize * pageSize
	if got != want {
		t.Fatalf("effectiveMainMmapSize(read-only 2GB) = %d, want %d", got, want)
	}
}

func TestEffectiveMainMmapSizeConfigured(t *testing.T) {
	fileSize := int64(500 << 20) // 500 MB
	configured := int64(16 << 30)  // 16 GB reservation

	db := &DB{mainFileSize: fileSize, mainFileMmapSize: configured}
	got := db.effectiveMainMmapSize()
	if got != configured {
		t.Fatalf("effectiveMainMmapSize(configured) = %d, want %d", got, configured)
	}

	// Configured size below current file is raised to cover the file
	db = &DB{mainFileSize: fileSize, mainFileMmapSize: 1 << 20}
	got = db.effectiveMainMmapSize()
	pageSize := int64(os.Getpagesize())
	want := (fileSize + pageSize - 1) / pageSize * pageSize
	if got != want {
		t.Fatalf("effectiveMainMmapSize(too small config) = %d, want %d", got, want)
	}
}
