package hashtabledb

// Small varint used for hybrid sub-page entry slots and record key lengths.
// It trades range for decode speed: one byte up to 240, two bytes up to
// maxSmallVarint, and a single branch either way, so both functions inline
// into the entry scan loops. Slots are capped by TableEntries and keys at
// MaxKeyLength, both well below the two-byte ceiling
//
// This encoding is not compatible with the SQLite4 varint in the varint
// package, which is still used for record value sizes
//
// Decode:
//
//	If A0 is between 0 and 240 inclusive, the result is A0.
//	Otherwise the result is 241 + 256*(A0-241) + A1.
const maxSmallVarint = 241 + 256*(255-241) + 255 // 4080

// readSmallVarint decodes a small varint, returning the value and the number of
// bytes it occupies. A two-byte form running past the buffer returns (0, 0),
// which callers report as a parse error. The one-byte fast path carries no
// explicit check, matching the previous varint.Read behavior on the entry
// scan loops' hot path
func readSmallVarint(buf []byte) (int, int) {
	a0 := int(buf[0])
	if a0 <= 240 {
		return a0, 1
	}
	if len(buf) < 2 {
		return 0, 0
	}
	return 241 + (a0-241)<<8 + int(buf[1]), 2
}

// writeSmallVarint encodes v, returning the number of bytes written. v must not
// exceed maxSmallVarint
func writeSmallVarint(buf []byte, v int) int {
	if v <= 240 {
		buf[0] = byte(v)
		return 1
	}
	v -= 241
	buf[0] = byte(241 + v>>8)
	buf[1] = byte(v)
	return 2
}

// smallVarintSize returns the number of bytes writeSmallVarint uses for v
func smallVarintSize(v int) int {
	if v <= 240 {
		return 1
	}
	return 2
}
