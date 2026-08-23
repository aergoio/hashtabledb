//go:build linux

package hashtabledb

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	madviseNormal     = syscall.MADV_NORMAL
	madviseRandom     = syscall.MADV_RANDOM
	madviseSequential = syscall.MADV_SEQUENTIAL
)

// mmapMainFile maps length bytes of file starting at offset 0 read-only
// shared. Mapping past the current end of file is allowed; callers must
// bounds-check every access against the file size to avoid SIGBUS.
func mmapMainFile(file *os.File, length int64, advise int) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}

	pageSize := int64(syscall.Getpagesize())
	if length%pageSize != 0 {
		length = ((length / pageSize) + 1) * pageSize
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(length), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	if len(data) > 0 {
		if _, _, errno := syscall.Syscall(
			syscall.SYS_MADVISE,
			uintptr(unsafe.Pointer(&data[0])),
			uintptr(len(data)),
			uintptr(advise),
		); errno != 0 {
			debugPrint("madvise(%d) on main file mmap: %v\n", advise, errno)
		}
	}

	return data, nil
}

// munmapMainFile releases a mapping created by mmapMainFile
func munmapMainFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
