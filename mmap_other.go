//go:build !linux

package hashtabledb

import (
	"fmt"
	"os"
)

// Keep the advice constants aligned with linux (MADV_NORMAL=0, MADV_RANDOM=1,
// MADV_SEQUENTIAL=3) so option parsing is platform independent
const (
	madviseNormal     = 0
	madviseRandom     = 1
	madviseSequential = 3
)

// mmapMainFile is a stub where mmap is not wired up; enabling UseMmap there
// fails at Open instead of silently falling back
func mmapMainFile(file *os.File, length int64, advise int) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("mmap of the main file is not supported on this platform")
}

// munmapMainFile is a stub matching mmapMainFile
func munmapMainFile(data []byte) error {
	return nil
}
