package hashtabledb

import (
	"encoding/binary"
	"testing"
)

func TestTableEntryStoresDataSize(t *testing.T) {
	db := &DB{
		cloningSequence: -1,
	}
	tablePage := &TablePage{
		pageNumber: 1,
		data:       make([]byte, PageSize),
		txnSequence: 1,
	}

	const (
		slot      = 10
		dataOffset = int64(0x123456789)
		dataSize   = uint32(1234)
	)
	if err := db.setTableEntry(tablePage, slot, 0, 0, dataOffset, dataSize); err != nil {
		t.Fatalf("setTableEntry: %v", err)
	}

	pageNumber, subPageID, gotOffset, gotSize := db.getTableEntry(tablePage, slot)
	if pageNumber != 0 || subPageID != 0 || gotOffset != dataOffset || gotSize != uint16(dataSize) {
		t.Fatalf("unexpected table entry: page=%d subPage=%d offset=%d size=%d",
			pageNumber, subPageID, gotOffset, gotSize)
	}
}

func TestTableEntryClearsDataSizeForPagePointer(t *testing.T) {
	db := &DB{
		cloningSequence: -1,
	}
	tablePage := &TablePage{
		pageNumber: 1,
		data:       make([]byte, PageSize),
		txnSequence: 1,
	}
	offset := TableHeaderSize + 10*TableEntrySize
	binary.LittleEndian.PutUint16(tablePage.data[offset+5:offset+7], 1234)

	if err := db.setTableEntry(tablePage, 10, 42, 7, 0, 0); err != nil {
		t.Fatalf("setTableEntry: %v", err)
	}

	pageNumber, subPageID, dataOffset, dataSize := db.getTableEntry(tablePage, 10)
	if pageNumber != 42 || subPageID != 7 || dataOffset != 0 || dataSize != 0 {
		t.Fatalf("unexpected page pointer: page=%d subPage=%d offset=%d size=%d",
			pageNumber, subPageID, dataOffset, dataSize)
	}
	if got := binary.LittleEndian.Uint16(tablePage.data[offset+5 : offset+7]); got != 0 {
		t.Fatalf("page pointer data-size bytes = %d, want 0", got)
	}
}
