//go:build linux

package hashtabledb

import (
	"fmt"
	"syscall"
	"unsafe"
)

const madviseRandom = 2

func (db *DB) mapMainFile() error {
	if !db.mainFileMmapEnabled || db.mainFile == nil {
		return nil
	}

	mmapSize := db.effectiveMainMmapSize()
	if mmapSize <= 0 {
		return nil
	}

	pageSize := int64(syscall.Getpagesize())
	if mmapSize%pageSize != 0 {
		mmapSize = ((mmapSize / pageSize) + 1) * pageSize
	}

	data, err := syscall.Mmap(int(db.mainFile.Fd()), 0, int(mmapSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap main file: %w", err)
	}

	if len(data) > 0 {
		_, _, errno := syscall.Syscall(
			syscall.SYS_MADVISE,
			uintptr(unsafe.Pointer(&data[0])),
			uintptr(len(data)),
			uintptr(madviseRandom),
		)
		if errno != 0 {
			debugPrint("madvise random on main file mmap: %v\n", errno)
		}
	}

	db.mainMmap = data
	return nil
}

func (db *DB) unmapMainFile() error {
	if db.mainMmap == nil {
		return nil
	}

	err := syscall.Munmap(db.mainMmap)
	db.mainMmap = nil
	return err
}
