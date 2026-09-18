package hashtabledb

import "testing"

func TestSmallVarintRoundTrip(t *testing.T) {
	buf := make([]byte, 2)
	for v := 0; v <= maxSmallVarint; v++ {
		size := writeSmallVarint(buf, v)
		if size != smallVarintSize(v) {
			t.Fatalf("write(%d) used %d bytes, smallVarintSize says %d", v, size, smallVarintSize(v))
		}
		got, read := readSmallVarint(buf)
		if got != v || read != size {
			t.Fatalf("read back (%d,%d), want (%d,%d)", got, read, v, size)
		}
	}
}

func TestSmallVarintBoundaries(t *testing.T) {
	cases := []struct {
		value int
		size  int
		bytes []byte
	}{
		{0, 1, []byte{0}},
		{240, 1, []byte{240}},
		{241, 2, []byte{241, 0}},
		{maxSmallVarint, 2, []byte{255, 255}},
	}
	buf := make([]byte, 2)
	for _, tc := range cases {
		size := writeSmallVarint(buf, tc.value)
		if size != tc.size {
			t.Fatalf("write(%d) size = %d, want %d", tc.value, size, tc.size)
		}
		for i := range tc.bytes {
			if buf[i] != tc.bytes[i] {
				t.Fatalf("write(%d) = %v, want %v", tc.value, buf[:size], tc.bytes)
			}
		}
	}

	// A record key length can never exceed the key limit, and a hybrid entry
	// slot is bounded by TableEntries, so both always fit
	if MaxKeyLength > maxSmallVarint {
		t.Fatalf("MaxKeyLength %d exceeds small varint range %d", MaxKeyLength, maxSmallVarint)
	}
	if TableEntries-1 > maxSmallVarint {
		t.Fatalf("TableEntries %d exceeds small varint range %d", TableEntries, maxSmallVarint)
	}
}
