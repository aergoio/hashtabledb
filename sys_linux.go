//go:build linux && (amd64 || arm64)

// Linux-specific support
//
// The architecture clause in the constraint above is required by adviseFile:
// posix_fadvise is reached through a raw syscall, and the 32-bit ABIs differ
// from the one used here. arm has no SYS_FADVISE64 at all, and 386 passes the
// 64-bit offset as a register pair, which would shift the advice argument
// Linux code that applies to every architecture belongs in a file constrained
// to linux alone

package hashtabledb

import (
	"os"
	"syscall"
)

// adviseFile passes an access pattern hint for the whole file to the kernel
// Failures are ignored: the hint only tunes readahead, it is never required
func adviseFile(file *os.File, advice int) {
	if file == nil {
		return
	}
	syscall.Syscall6(syscall.SYS_FADVISE64, file.Fd(), 0, 0, uintptr(advice), 0, 0)
}
