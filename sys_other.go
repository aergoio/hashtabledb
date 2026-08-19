//go:build !linux || (!amd64 && !arm64)

// Fallbacks for the platforms not covered by sys_linux.go

package hashtabledb

import "os"

// adviseFile is a no-op where posix_fadvise is not available. The hint only
// tunes readahead, so the database behaves the same without it
func adviseFile(file *os.File, advice int) {}
