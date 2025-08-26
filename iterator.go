package hashtabledb

// Iterator implements iteration over database key-value pairs
type Iterator struct {
	db           *DB
	currentKey   []byte    // Current key
	currentValue []byte    // Current value
	valid        bool      // Whether the iterator is valid
	closed       bool      // Whether the iterator is closed
	stack        []iterPos // Stack for depth-first traversal
	maxReadSeq   int64     // Maximum transaction sequence to read (for MVCC consistency)
}

// iterPos represents a position in the iteration
type iterPos struct {
	pageNumber   uint32 // Page number
	pageType     byte   // Type of the page (table or hybrid)
	slot         int    // Current slot in table page (-1 if not applicable)
	SubPageId   uint8  // Current sub-page index in hybrid page
	entryIdx     int    // Current entry index in hybrid sub-page (-1 if not started)
	totalEntries int    // Total entries in hybrid sub-page
	entries      []hybridEntry // Cached entries from hybrid sub-page
}

// hybridEntry represents an entry from a hybrid sub-page
type hybridEntry struct {
	isSubPage bool
	value     uint64 // Either data offset or (subPageId | pageNumber << 8)
}

// NewIterator returns a new iterator for the database
// It provides simple unordered iteration over all key-value pairs
func (db *DB) NewIterator() *Iterator {
	// Check if database is closed
	if db.isClosed {
		return &Iterator{
			db:     db,
			valid:  false,
			closed: true,
			stack:  make([]iterPos, 0),
		}
	}

	// Capture the current transaction sequence for MVCC consistency
	db.seqMutex.Lock()
	var maxReadSeq int64
	if db.inTransaction {
		maxReadSeq = db.txnSequence - 1
	} else {
		maxReadSeq = db.txnSequence
	}
	db.seqMutex.Unlock()

	// Create a new iterator
	it := &Iterator{
		db:         db,
		valid:      false,
		stack:      make([]iterPos, 0),
		maxReadSeq: maxReadSeq,
	}

	// Start with the first main index page (page 1)
	// We'll iterate through all main index pages sequentially
	it.stack = append(it.stack, iterPos{
		pageNumber: 1,
		pageType:   ContentTypeTable,
		slot:       -1, // Start at -1 so Next() will move to slot 0
	})

	// Move to the first entry
	it.Next()
	return it
}

// Next moves the iterator to the next key-value pair
func (it *Iterator) Next() {
	if it.closed || it.db.isClosed {
		it.valid = false
		return
	}

	// Use readMutex for database state consistency
	it.db.readMutex.RLock()
	defer it.db.readMutex.RUnlock()

	for len(it.stack) > 0 {
		// Get the current position from the top of the stack
		pos := &it.stack[len(it.stack)-1]

		if pos.pageType == ContentTypeTable {
			// Process table page
			if !it.processTablePage(pos) {
				// If we've exhausted this table page, pop it from the stack and continue
				it.stack = it.stack[:len(it.stack)-1]
				continue
			}
			return
		} else if pos.pageType == ContentTypeHybrid {
			// Process hybrid page
			if !it.processHybridPage(pos) {
				// If we've exhausted this hybrid page, pop it from the stack and continue
				it.stack = it.stack[:len(it.stack)-1]
				continue
			}
			return
		}

		// If we get here with an unknown page type, pop it and continue
		it.stack = it.stack[:len(it.stack)-1]
	}

	// If we get here, we've exhausted all pages
	it.valid = false
}

// processTablePage processes the current table page position
// Returns true if a valid entry was found, false if the page is exhausted
func (it *Iterator) processTablePage(pos *iterPos) bool {
	// Get the table page
	tablePage, err := it.db.getTablePage(pos.pageNumber, it.maxReadSeq)
	if err != nil {
		return false
	}

	// Move to the next slot
	pos.slot++

	// Find the next non-empty slot
	for pos.slot < TableEntries {
		pageNumber, SubPageId := it.db.getTableEntry(tablePage, pos.slot)
		if pageNumber != 0 {
			// Found an entry, load the page
			page, err := it.db.getPage(pageNumber, it.maxReadSeq)
			if err != nil {
				// If we can't load the page, continue to next slot
				pos.slot++
				continue
			}

			// Push the new page to the stack
			if page.pageType == ContentTypeTable {
				it.stack = append(it.stack, iterPos{
					pageNumber: pageNumber,
					pageType:   ContentTypeTable,
					slot:       -1, // Start at -1 so Next() will move to slot 0
				})
				return it.processTablePage(&it.stack[len(it.stack)-1])
			} else if page.pageType == ContentTypeHybrid {
				it.stack = append(it.stack, iterPos{
					pageNumber: pageNumber,
					pageType:   ContentTypeHybrid,
					SubPageId: SubPageId,
					entryIdx:   -1, // Start at -1 so we'll load entries first
				})
				return it.processHybridPage(&it.stack[len(it.stack)-1])
			}
		}

		// Move to the next slot
		pos.slot++
	}

	// We've exhausted all slots in this page
	// If this is a main index page, try to move to the next main index page
	if pos.pageNumber >= 1 && pos.pageNumber <= uint32(it.db.mainIndexPages) {
		nextMainIndexPage := pos.pageNumber + 1
		if nextMainIndexPage <= uint32(it.db.mainIndexPages) {
			// Update current position to next main index page
			pos.pageNumber = nextMainIndexPage
			pos.slot = -1 // Reset slot to start from beginning
			// Recursively process the next main index page
			return it.processTablePage(pos)
		}
	}

	// If we get here, we've exhausted this table page
	return false
}

// processHybridPage processes the current hybrid page position
// Returns true if a valid entry was found, false if the page is exhausted
func (it *Iterator) processHybridPage(pos *iterPos) bool {
	// Load entries if not already done
	if pos.entryIdx == -1 {
		if !it.loadHybridEntries(pos) {
			return false
		}
		pos.entryIdx = 0
	}

	// Move to the next entry
	for pos.entryIdx < pos.totalEntries {
		entry := pos.entries[pos.entryIdx]
		pos.entryIdx++

		if entry.isSubPage {
			// It's a sub-page pointer, extract page number and sub-page index
			SubPageId := uint8(entry.value & 0xFF)
			pageNumber := uint32(entry.value >> 8)

			// Load the page
			page, err := it.db.getPage(pageNumber, it.maxReadSeq)
			if err != nil {
				// If we can't load the page, continue to next entry
				continue
			}

			// Push the new page to the stack
			if page.pageType == ContentTypeTable {
				it.stack = append(it.stack, iterPos{
					pageNumber: pageNumber,
					pageType:   ContentTypeTable,
					slot:       -1,
				})
				return it.processTablePage(&it.stack[len(it.stack)-1])
			} else if page.pageType == ContentTypeHybrid {
				it.stack = append(it.stack, iterPos{
					pageNumber: pageNumber,
					pageType:   ContentTypeHybrid,
					SubPageId: SubPageId,
					entryIdx:   -1,
				})
				return it.processHybridPage(&it.stack[len(it.stack)-1])
			}
		} else {
			// It's a data offset, read the content
			dataOffset := int64(entry.value)
			content, err := it.db.readContent(dataOffset)
			if err != nil {
				// If we can't read the content, continue to next entry
				continue
			}

			// Set current key and value
			it.currentKey = content.key
			it.currentValue = content.value
			it.valid = true
			return true
		}
	}

	// If we get here, we've exhausted this hybrid page
	return false
}

// loadHybridEntries loads all entries from a hybrid sub-page
func (it *Iterator) loadHybridEntries(pos *iterPos) bool {
	// Get the hybrid page
	hybridPage, err := it.db.getPage(pos.pageNumber, it.maxReadSeq)
	if err != nil {
		return false
	}

	// Clear previous entries
	pos.entries = pos.entries[:0]
	pos.totalEntries = 0

	// Load entries from the specific sub-page
	err = it.db.iterateHybridSubPageEntries(hybridPage, pos.SubPageId, func(entryOffset int, entrySize int, slot int, isSubPage bool, value uint64) bool {
		// Add to our list
		pos.entries = append(pos.entries, hybridEntry{
			isSubPage: isSubPage,
			value:     value,
		})
		pos.totalEntries++
		return true // Continue iteration
	})

	if err != nil {
		return false
	}

	return pos.totalEntries > 0
}

// Valid returns whether the iterator is valid
func (it *Iterator) Valid() bool {
	// Check if database is closed
	if it.db.isClosed {
		it.valid = false
		it.closed = true
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

// Close closes the iterator
func (it *Iterator) Close() {
	it.closed = true
	it.valid = false
}
