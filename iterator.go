package hashtabledb

import (
	"bytes"
	"io"
	"os"
	"slices"

	"github.com/aergoio/hashtabledb/varint"
)

// Iterator iterates over the database key-value pairs in main file order
// (insertion order), yielding only the records the index still points at
//
// Two modes, chosen at creation by estimating the RAM the offsets array needs
// from a sample of container pages:
//   - offsets mode: the index is walked once to collect the data offsets it
//     points at (offsets only, no content reads) and each record is then read
//     straight from its offset in append order. The array takes 8 bytes per
//     live key
//   - scan+lookup mode (when the offsets would take more than 80% of the
//     available RAM): the main file is scanned record by record and each key
//     is looked up on the index; the record is active when the index still
//     points at its offset. No per-key memory
//
// In both modes commit markers and records superseded by newer versions of
// the same key or deleted are never read as values. Keys are hashed into the
// index, so no mode yields an order beyond the main file layout
type Iterator struct {
	db     *DB
	valid  bool // Whether the iterator is valid
	closed bool // Whether the iterator is closed

	mode         int            // Iteration mode: iterModeScanLookup or iterModeOffsets
	maxReadSeq   int64          // Maximum transaction sequence to read (for MVCC consistency)
	registration readerSlotRef // Handle for this iterator's reader registration

	// Scan+lookup mode: the main file is scanned record by record between
	// PageSize and endOffset (the main file size snapshot taken at creation)
	scanOffset int64
	endOffset  int64

	// Sequential scan buffer covering the file range
	// [bufOffset, bufOffset + bufLen)
	buf       []byte
	bufLen    int
	bufOffset int64

	// Offsets mode: data offsets the index points at, sorted ascending
	activeOffsets []int64
	offsetCursor  int  // Next activeOffsets entry to read
	offsetsLoaded bool // Whether the offsets have been collected

	currentKey   []byte // Current key
	currentValue []byte // Current value

	externalKeyIndex int // Next external (mutable) key index after the main pass
}

// Iteration modes of the main file iterator
const (
	iterModeScanLookup = iota // Main file scanned record by record, each key looked up on the index
	iterModeOffsets           // Offsets collected from the index once, each record read directly
)

// Worst case bytes needed to parse a data record header and its key:
// type(1) + keyLen small varint(2) + valueLen varint(9) + key(2048)
const iteratorHeaderSlack = 1 + 2 + varintMaxSize + MaxKeyLength

// varintMaxSize is the largest varint encoding (SQLite4 format)
const varintMaxSize = 9

// iteratorScanBufferSize is the size of the sequential scan buffer
const iteratorScanBufferSize = 256 * 1024

// iteratorIndexBytesPerRecord is the approximate number of index file bytes
// each live record costs, measured end to end: direct table slots take 5
// bytes and hybrid entries 9-10, plus main index pages, subtree pages and
// page overhead
const iteratorIndexBytesPerRecord = 12

// estimateOffsetsMemory estimates the RAM the offsets array needs from the
// index file size: roughly iteratorIndexBytesPerRecord bytes per live record,
// 8 array bytes each. The estimate errs on the high side for sparse indexes,
// which only pushes toward the safer scan+lookup mode. Page sampling is not
// usable here: the hash layout spreads records thinly over the main index
// slots, so no small sample is representative of the record count
func (db *DB) estimateOffsetsMemory() int64 {
	return db.virtualIndexFileSize.Load() / iteratorIndexBytesPerRecord * 8
}

// NewIterator returns a new iterator for the database, picking the iteration
// mode from the RAM estimator
func (db *DB) NewIterator() *Iterator {
	// Fall back to the scan+lookup mode when the offsets array would take
	// more than 80% of the available RAM
	if db.estimateOffsetsMemory()*10 > getSystemMemoryInfo().Available*8 {
		return db.newScanLookupIterator()
	}
	return db.newOffsetsIterator()
}

// newOffsetsIterator returns the iterator in offsets mode: the index is walked
// once to collect the offsets it points at and each record is then read
// straight from its offset
func (db *DB) newOffsetsIterator() *Iterator {
	if db.isClosed.Load() {
		return closedIterator(db)
	}

	maxReadSeq, registration := db.captureIteratorReadSeq()

	it := &Iterator{
		db:           db,
		mode:         iterModeOffsets,
		maxReadSeq:   maxReadSeq,
		registration: registration,
	}

	// Move to the first entry
	it.Next()
	return it
}

// newScanLookupIterator returns the iterator in scan+lookup mode: the main
// file is scanned record by record and each key is looked up on the index,
// using no per-key memory
func (db *DB) newScanLookupIterator() *Iterator {
	if db.isClosed.Load() {
		return closedIterator(db)
	}

	// Snapshot the scan range: the whole main file at creation time. Records
	// not committed or not indexed yet (still-dirty index pages ahead of the
	// flusher) are scanned too, but the index lookup with the snapshot's
	// maxReadSeq either misses the key or points at an older record version
	endOffset := db.mainFileSize.Load()
	if endOffset < int64(PageSize) {
		endOffset = int64(PageSize)
	}

	maxReadSeq, registration := db.captureIteratorReadSeq()

	it := &Iterator{
		db:           db,
		mode:         iterModeScanLookup,
		maxReadSeq:   maxReadSeq,
		registration: registration,
		scanOffset:   int64(PageSize),
		endOffset:    endOffset,
	}

	// Warm the OS page cache with the index file sequentially before the
	// scan-driven lookups start, so they never touch the disk. Skipped when
	// the index cannot stay in RAM: a read-through of a file larger than the
	// available memory only evicts itself, so lookups read pages on demand.
	// The offsets mode does not need this: its single index walk reads the
	// pages once, streaming
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

// closedIterator returns an invalid, closed iterator for a closed database
func closedIterator(db *DB) *Iterator {
	return &Iterator{
		db:     db,
		valid:  false,
		closed: true,
	}
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

	it.nextScannedRecord()
}

// nextScannedRecord advances the iterator to the next record, dispatching on
// the iteration mode
func (it *Iterator) nextScannedRecord() {
	if it.mode == iterModeOffsets {
		it.nextOffsetsRecord()
		return
	}
	it.nextScanLookupRecord()
}

// nextScanLookupRecord advances the scan+lookup mode iterator: the main file
// is scanned record by record and each key is looked up on the index, yielding
// the first record the index still points at. External (mutable) keys are
// yielded after the scan
func (it *Iterator) nextScanLookupRecord() {
	// Sequentially scan the main file looking for active records
	for it.scanOffset < it.endOffset {
		data, ok := it.recordBuffer(it.scanOffset, iteratorHeaderSlack)
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
		keyLen, keyLenSize := readSmallVarint(data[1:])
		if keyLenSize == 0 {
			break
		}
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
			value, ok := it.recordValue(it.scanOffset, data, keyEnd, int(valueLen64))
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

// nextOffsetsRecord advances the offsets mode iterator to the record at the
// next active offset. The offsets are sorted ascending, so reading them in
// order walks the main file sequentially while skipping commit markers and
// records the index no longer points at. Each offset yields exactly one pair,
// so the cursor advances by one per record. External (mutable) keys are
// yielded after the pass
func (it *Iterator) nextOffsetsRecord() {
	// Collect the indexed offsets once, on the first Next
	if !it.offsetsLoaded {
		it.collectIndexedOffsets()
		it.offsetsLoaded = true
	}

	for ; it.offsetCursor < len(it.activeOffsets); it.offsetCursor++ {
		offset := it.activeOffsets[it.offsetCursor]

		data, ok := it.recordBuffer(offset, iteratorHeaderSlack)
		if !ok {
			continue
		}

		if contentType := data[0]; contentType != ContentTypeData {
			// The offset does not point at a data record (corrupted or stale
			// entry): skip it, the next offset is independent of this one
			debugPrint("iterator: unexpected content type '%c' at offset %d\n", contentType, offset)
			continue
		}

		// Parse key length with the small varint: keys are capped at
		// MaxKeyLength, well below the two-byte ceiling
		keyLen, keyLenSize := readSmallVarint(data[1:])
		if keyLenSize == 0 {
			continue
		}
		if keyLen > MaxKeyLength {
			continue
		}

		// Parse value length
		if 1+keyLenSize >= len(data) {
			continue
		}
		valueLen64, valueLenSize := varint.Read(data[1+keyLenSize:])
		if valueLenSize == 0 || valueLen64 > MaxValueLength {
			continue
		}

		keyOffset := 1 + keyLenSize + valueLenSize
		keyEnd := keyOffset + keyLen
		if keyEnd > len(data) {
			// Truncated record
			continue
		}

		value, ok := it.recordValue(offset, data, keyEnd, int(valueLen64))
		if !ok {
			continue
		}

		it.currentKey = bytes.Clone(data[keyOffset:keyEnd])
		it.currentValue = value
		it.valid = true
		it.offsetCursor++
		return
	}

	// Pass complete; yield external (mutable) keys not stored in the index
	if it.nextExternalKey() {
		return
	}

	it.valid = false
}

// recordBuffer returns a slice of the scan buffer starting at offset. The
// buffer is refilled from that offset when the request is not covered, keeping
// record headers aligned with the buffer start
func (it *Iterator) recordBuffer(offset int64, minNeeded int) ([]byte, bool) {
	rel := int(offset - it.bufOffset)
	if rel >= 0 && rel+minNeeded <= it.bufLen {
		// Fast path: request covered by the current buffer
		return it.buf[rel:it.bufLen], true
	}

	// Lazily allocate the scan buffer
	if it.buf == nil {
		it.buf = make([]byte, iteratorScanBufferSize)
	}

	// Refill from the record; a short read (io.EOF) near the end of the file
	// returns whatever is available, the caller bounds-checks
	n, err := it.db.mainFile.ReadAt(it.buf, offset)
	if err != nil && err != io.EOF {
		debugPrint("iterator: scan read failed at offset %d: %v\n", offset, err)
		return nil, false
	}
	it.bufOffset = offset
	it.bufLen = n

	if n > 0 {
		return it.buf[:n], true
	}
	return nil, false
}

// recordValue returns the value bytes of the record at offset, reading
// directly from the file when the value does not fit in the scan buffer
// (large records)
func (it *Iterator) recordValue(offset int64, data []byte, valueStart, valueLen int) ([]byte, bool) {
	if valueStart+valueLen <= len(data) {
		// Copy out so the slice survives the next buffer refill
		value := make([]byte, valueLen)
		copy(value, data[valueStart:valueStart+valueLen])
		return value, true
	}

	value := make([]byte, valueLen)
	n, err := it.db.mainFile.ReadAt(value, offset+int64(valueStart))
	if (err != nil && err != io.EOF) || n != valueLen {
		debugPrint("iterator: failed to read value at offset %d: %v\n", offset+int64(valueStart), err)
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

// collectIndexedOffsets scans the index file once, sequentially, and records
// every data offset the index points at, sorted so the records can be read
// directly in append order
//
// Container pages are read in page-number order. Pages already in the page
// cache are preferred (they hold the MVCC versions this snapshot must see)
// and pages read from disk are never inserted into the page cache: the scan
// visits each page once and caching them would only grow memory on large
// databases. Main index and sub table pages contribute their direct data
// offsets and hybrid sub-pages their data entries; child page pointers are
// skipped because the walk reaches those pages itself, so no tree walk is
// needed
func (it *Iterator) collectIndexedOffsets() {
	db := it.db

	it.activeOffsets = it.activeOffsets[:0]

	// Page 0 is the header page; container pages start at page 1. The virtual
	// file size covers every allocated page, including pages still dirty in
	// the page cache and not yet flushed to the index file
	virtualPages := db.virtualIndexFileSize.Load() / int64(PageSize)
	if virtualPages < 2 {
		return
	}

	// Pages are read one at a time, only when they miss the page cache; the
	// read is the same readFromIndexFile path any cache miss takes
	realPages := db.realIndexFileSize.Load() / int64(PageSize)

	// Single forward pass over the whole index file
	for pageNumber := uint32(1); int64(pageNumber) < virtualPages; pageNumber++ {
		// Cached page first: it carries the version of the snapshot
		page := db.lookupCachedPage(pageNumber, it.maxReadSeq)
		if page == nil {
			if int64(pageNumber) >= realPages {
				// Not flushed and not cached: nothing to read
				continue
			}
			raw, err := db.readFromIndexFile(pageNumber)
			if err != nil || len(raw) < 5 {
				debugPrint("iterator: page %d read failed: %v\n", pageNumber, err)
				continue
			}
			// Parse the flushed page without inserting it into the page cache
			if raw[4] == ContentTypeTable {
				tablePage, perr := db.parseTablePage(raw, pageNumber)
				if perr != nil {
					continue
				}
				page = (*Page)(tablePage)
			} else if raw[4] == ContentTypeHybrid {
				hybridPage, perr := db.parseHybridPage(raw, pageNumber)
				if perr != nil {
					continue
				}
				page = (*Page)(hybridPage)
			} else {
				// Not a container page: skip it
				continue
			}
		}

		it.collectPageOffsets(page)
	}

	slices.Sort(it.activeOffsets)
}

// collectPageOffsets records the data offsets held by one container page:
// every occupied slot with a direct data offset on table pages, and every
// data entry of live hybrid sub-pages. Child page pointers are skipped: the
// sequential walk reaches those pages itself
func (it *Iterator) collectPageOffsets(page *Page) {
	if page.pageType == ContentTypeTable {
		tablePage := (*TablePage)(page)
		for slot := 0; slot < TableEntries; slot++ {
			_, _, dataOffset := it.db.getTableEntry(tablePage, slot)
			if dataOffset != 0 {
				it.activeOffsets = append(it.activeOffsets, dataOffset)
			}
		}
	} else if page.pageType == ContentTypeHybrid {
		hybridPage := (*HybridPage)(page)
		for subPageId := 0; subPageId < len(hybridPage.SubPages); subPageId++ {
			if !hybridSubPageLive(hybridPage.SubPages[subPageId]) {
				continue
			}
			it.db.iterateHybridSubPageEntries(hybridPage, &hybridPage.SubPages[subPageId], func(_ int, _ int, _ uint32, _ uint8, dataOffset uint64, _ uint16) bool {
				if dataOffset > 0 {
					it.activeOffsets = append(it.activeOffsets, int64(dataOffset))
				}
				return true
			})
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
