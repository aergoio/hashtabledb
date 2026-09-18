package hashtabledb

import (
	"bytes"
	"io"
	"os"

	"github.com/aergoio/hashtabledb/varint"
)

// Iterator iterates over the database key-value pairs by scanning the main
// file sequentially and resolving each record against the index
//
// For every record the key is looked up on the index and only the data offset
// is retrieved (no content reads). The record is active when the index still
// points at it; records superseded by newer versions of the same key or
// deleted are skipped without reading their values
//
// Keys are hashed into the index, so the iteration order is unspecified
// (as in any hash table)
type Iterator struct {
	db     *DB
	valid  bool // Whether the iterator is valid
	closed bool // Whether the iterator is closed

	maxReadSeq   int64         // Maximum transaction sequence to read (for MVCC consistency)
	registration readerSlotRef // Handle for this iterator's reader registration
	scanOffset   int64         // Offset of the next record to scan in the main file
	endOffset    int64         // Snapshot of lastIndexedOffset: records below it are indexed and committed

	// Sequential scan buffer covering the file range
	// [bufOffset, bufOffset + bufLen)
	buf       []byte
	bufLen    int
	bufOffset int64

	currentKey   []byte // Current key
	currentValue []byte // Current value

	externalKeyIndex int // Next external (mutable) key index after the scan
}

// Worst case bytes needed to parse a data record header and its key:
// type(1) + keyLen small varint(2) + valueLen varint(9) + key(2048)
const iteratorHeaderSlack = 1 + 2 + varintMaxSize + MaxKeyLength

// varintMaxSize is the largest varint encoding (SQLite4 format)
const varintMaxSize = 9

// iteratorScanBufferSize is the size of the sequential scan buffer
const iteratorScanBufferSize = 256 * 1024

// NewIterator returns a new iterator for the database
// It provides simple unordered iteration over all key-value pairs
func (db *DB) NewIterator() *Iterator {
	// Check if database is closed
	if db.isClosed.Load() {
		return &Iterator{
			db:     db,
			valid:  false,
			closed: true,
		}
	}

	// Capture the current transaction sequence for MVCC consistency and register
	// so flush/cleaner keep index page versions the lookups may walk until Close.
	// The snapshot comes from the published atomic word, so no seqMutex is taken
	maxReadSeq, registration := db.captureIteratorReadSeq()

	// Snapshot the scan range: the whole main file at creation time. Records
	// not committed or not indexed yet (still-dirty index pages ahead of the
	// flusher) are scanned too, but the offset match skips them: the index
	// lookup with the snapshot's maxReadSeq either misses the key or points at
	// an older record version
	endOffset := db.mainFileSize.Load()
	if endOffset < int64(PageSize) {
		endOffset = int64(PageSize)
	}

	it := &Iterator{
		db:           db,
		valid:        false,
		maxReadSeq:   maxReadSeq,
		registration: registration,
		scanOffset:   int64(PageSize),
		endOffset:    endOffset,
	}

	// Warm the OS page cache with the index file sequentially before the
	// scan-driven lookups start, so they never touch the disk. Skipped when
	// the index cannot stay in RAM: a read-through of a file larger than the
	// available memory only evicts itself, so lookups read pages on demand
	if indexFile := db.indexFile; indexFile != nil {
		if info, err := indexFile.Stat(); err == nil && info.Size() > 0 {
			if info.Size() <= getSystemMemoryInfo().Available {
				// Sequential pass with a completion barrier: when it
				// returns, the whole index is resident in the page cache
				db.preloadIndexFileCache(indexFile, info.Size())
			}
		}
	}

	// Move to the first entry
	it.Next()
	return it
}

// captureIteratorReadSeq pins the current MVCC read watermark and registers it
// so flush/cleaner keep index page versions the iterator may walk until Close.
// Returns the watermark and the registration handle
func (db *DB) captureIteratorReadSeq() (int64, readerSlotRef) {
	var maxReadSeq int64
	var registration readerSlotRef
	state := db.publishedTxnState.Load()
	for {
		if state&1 == 1 {
			maxReadSeq = int64(state>>1) - 1
		} else {
			maxReadSeq = int64(state >> 1)
		}
		registration = db.registerReaderSequence(maxReadSeq)
		// Retry when the published state moved before the registration, so the
		// registered floor always covers the snapshot. The verification load
		// doubles as the next attempt's snapshot
		next := db.publishedTxnState.Load()
		if next == state {
			break
		}
		db.unregisterReaderSequence(registration)
		state = next
	}
	return maxReadSeq, registration
}

// Next moves the iterator to the next key-value pair
func (it *Iterator) Next() {
	if it.closed {
		it.valid = false
		return
	}
	if it.db.isClosed.Load() {
		it.Close()
		return
	}

	// Use readMutex for database state consistency
	it.db.readMutex.RLock()
	defer it.db.readMutex.RUnlock()

	// Sequentially scan the main file looking for active records
	for it.scanOffset < it.endOffset {
		data, ok := it.scanBuffer(iteratorHeaderSlack)
		if !ok {
			break
		}

		contentType := data[0]

		if contentType == ContentTypeCommit {
			// Commit marker: 1 byte type + 4 bytes checksum
			if len(data) < 5 {
				break
			}
			it.scanOffset += 5
			continue
		}

		if contentType != ContentTypeData {
			// Unknown content type: stop the scan (corrupted tail)
			debugPrint("iterator: unknown content type '%c' at offset %d\n", contentType, it.scanOffset)
			break
		}

		// Parse key length with the small varint: keys are capped at
		// MaxKeyLength, well below the two-byte ceiling
		if !smallVarintFits(data, 1, len(data)) {
			break
		}
		keyLen, keyLenSize := readSmallVarint(data[1:])
		if keyLen > MaxKeyLength {
			break
		}

		// Parse value length
		if 1+keyLenSize >= len(data) {
			break
		}
		valueLen64, valueLenSize := varint.Read(data[1+keyLenSize:])
		if valueLenSize == 0 || valueLen64 > MaxValueLength {
			break
		}

		keyOffset := 1 + keyLenSize + valueLenSize
		keyEnd := keyOffset + keyLen
		recordSize := keyEnd + int(valueLen64)
		if keyEnd > len(data) || it.scanOffset+int64(recordSize) > it.endOffset {
			// Truncated record at the end of the indexed range
			break
		}

		key := data[keyOffset:keyEnd]

		// Resolve the key on the index (offset only, no content reads) and
		// yield the record only when the index still points at it
		indexedOffset, _, err := it.db.lookupRecordOffset(key, it.maxReadSeq)
		if err != nil {
			// On lookup errors skip the record, like the index walk does
			debugPrint("iterator: lookup failed for offset %d: %v\n", it.scanOffset, err)
		} else if indexedOffset == it.scanOffset {
			value, ok := it.recordValue(data, keyEnd, int(valueLen64))
			if !ok {
				break
			}
			it.currentKey = bytes.Clone(key)
			it.currentValue = value
			it.valid = true
			it.scanOffset += int64(recordSize)
			return
		}

		it.scanOffset += int64(recordSize)
	}

	// Scan complete; yield external (mutable) keys not stored in the index tree
	if it.nextExternalKey() {
		return
	}

	it.valid = false
}

// scanBuffer returns a slice of the scan buffer starting at scanOffset. The
// buffer is refilled from the current record when the request crosses the
// buffered range, keeping record headers aligned with the buffer start
func (it *Iterator) scanBuffer(minNeeded int) ([]byte, bool) {
	rel := int(it.scanOffset - it.bufOffset)
	if rel >= 0 && rel+minNeeded <= it.bufLen {
		// Fast path: request covered by the current buffer
		return it.buf[rel:it.bufLen], true
	}

	// Lazily allocate the scan buffer
	if it.buf == nil {
		it.buf = make([]byte, iteratorScanBufferSize)
	}

	// Refill from the current record; a short read (io.EOF) near the end of
	// the file returns whatever is available, the caller bounds-checks
	n, err := it.db.mainFile.ReadAt(it.buf, it.scanOffset)
	if err != nil && err != io.EOF {
		debugPrint("iterator: scan read failed at offset %d: %v\n", it.scanOffset, err)
		return nil, false
	}
	it.bufOffset = it.scanOffset
	it.bufLen = n

	if n > 0 {
		return it.buf[:n], true
	}
	return nil, false
}

// recordValue returns the value bytes of the current record, reading directly
// from the file when the value does not fit in the scan buffer (large records)
func (it *Iterator) recordValue(data []byte, valueStart, valueLen int) ([]byte, bool) {
	if valueStart+valueLen <= len(data) {
		// Copy out so the slice survives the next buffer refill
		value := make([]byte, valueLen)
		copy(value, data[valueStart:valueStart+valueLen])
		return value, true
	}

	value := make([]byte, valueLen)
	n, err := it.db.mainFile.ReadAt(value, it.scanOffset+int64(valueStart))
	if (err != nil && err != io.EOF) || n != valueLen {
		debugPrint("iterator: failed to read value at offset %d: %v\n", it.scanOffset+int64(valueStart), err)
		return nil, false
	}
	return value, true
}

// iteratorPreloadChunkSize is the read chunk used by the sequential preload
// of the index file
const iteratorPreloadChunkSize = 4 * 1024 * 1024

// preloadIndexFileCache reads the index file sequentially so the OS page
// cache holds the whole index before the scan-driven lookups start. ReadAt is
// served from the page cache, so pages already resident only cost a memcpy
// and no residency check is needed. Best effort: a read error just ends the
// warm up and the load is aborted when the database closes mid-way
func (db *DB) preloadIndexFileCache(indexFile *os.File, size int64) {
	buf := make([]byte, iteratorPreloadChunkSize)
	for offset := int64(0); offset < size; {
		// Abort when the database closes mid-load
		if db.isClosed.Load() {
			return
		}
		n, err := indexFile.ReadAt(buf, offset)
		offset += int64(n)
		if err != nil {
			return
		}
	}
}

// nextExternalKey advances to the next external key with a visible value.
// Returns true if the iterator is positioned on a valid entry.
func (it *Iterator) nextExternalKey() bool {
	for it.externalKeyIndex < len(it.db.externalKeys) {
		extKey := it.db.externalKeys[it.externalKeyIndex]
		it.externalKeyIndex++

		value, ok := it.externalValueForKey(extKey)
		if !ok {
			continue
		}

		it.currentKey = bytes.Clone(extKey.key)
		it.currentValue = bytes.Clone(value)
		it.valid = true
		return true
	}
	return false
}

func (it *Iterator) externalValueForKey(extKey *externalKey) ([]byte, bool) {
	entry := extKey.value
	for entry != nil {
		if entry.txnSequence <= it.maxReadSeq {
			return entry.value, true
		}
		entry = entry.next
	}
	return nil, false
}

// Valid returns whether the iterator is valid
func (it *Iterator) Valid() bool {
	// Check if database is closed
	if it.db.isClosed.Load() {
		it.Close()
	}
	return !it.closed && it.valid
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currentKey
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currentValue
}

// Close closes the iterator and drops its reader registration
func (it *Iterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.valid = false
	it.db.unregisterReaderSequence(it.registration)
}
