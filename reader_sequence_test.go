package hashtabledb

import (
	"math/rand"
	"sync"
	"testing"
)

// registryState returns the total number of live registrations across the
// fixed slots and the overflow list, for asserting the registry drains fully
func (db *DB) registryState(t *testing.T) (total int, overflow int) {
	t.Helper()
	for i := range db.readerSlots {
		if n := db.readerSlots[i].Load() & readerSlotCountMax; n != 0 {
			total += int(n)
		}
	}
	db.readerOverflow.mutex.Lock()
	for _, ref := range db.readerOverflow.refs {
		total += ref.count
		overflow += ref.count
	}
	db.readerOverflow.mutex.Unlock()
	return total, overflow
}

func TestReaderSequenceSlotsBasics(t *testing.T) {
	db := &DB{}

	// Empty registry reports no readers
	if oldest, ok := db.oldestReaderSequence(); ok {
		t.Fatalf("expected no readers on a zero DB, got oldest=%d", oldest)
	}

	// A single registration is visible as the floor
	ref := db.registerReaderSequence(42)
	if oldest, ok := db.oldestReaderSequence(); !ok || oldest != 42 {
		t.Fatalf("expected oldest=42, got %d ok=%v", oldest, ok)
	}

	// Duplicate epochs stack on the same slot and still report the floor
	dup := db.registerReaderSequence(42)
	if oldest, _ := db.oldestReaderSequence(); oldest != 42 {
		t.Fatalf("expected oldest=42 after dup, got %d", oldest)
	}
	db.unregisterReaderSequence(dup)
	if oldest, _ := db.oldestReaderSequence(); oldest != 42 {
		t.Fatalf("expected oldest=42 after dup unregister, got %d", oldest)
	}

	// A lower epoch becomes the new floor and releases it when drained
	low := db.registerReaderSequence(7)
	if oldest, _ := db.oldestReaderSequence(); oldest != 7 {
		t.Fatalf("expected oldest=7, got %d", oldest)
	}
	db.unregisterReaderSequence(low)
	if oldest, _ := db.oldestReaderSequence(); oldest != 42 {
		t.Fatalf("expected oldest=42 after low unregister, got %d", oldest)
	}

	// Draining everything clears the registry
	db.unregisterReaderSequence(ref)
	if total, _ := db.registryState(t); total != 0 {
		t.Fatalf("expected empty registry, got %d live", total)
	}
	if _, ok := db.oldestReaderSequence(); ok {
		t.Fatal("expected no readers after draining")
	}
}

func TestReaderSequenceSlotsFullTable(t *testing.T) {
	db := &DB{}

	// Fill every fixed slot with a distinct epoch
	refs := make([]readerSlotRef, 0, readerSlotCount)
	for i := int64(0); i < readerSlotCount; i++ {
		refs = append(refs, db.registerReaderSequence(100+i))
	}
	if oldest, ok := db.oldestReaderSequence(); !ok || oldest != 100 {
		t.Fatalf("expected oldest=100, got %d ok=%v", oldest, ok)
	}
	if _, overflow := db.registryState(t); overflow != 0 {
		t.Fatalf("expected no overflow registrations, got %d", overflow)
	}

	// One more distinct epoch must land in the overflow list without
	// blocking, and the floor must still be exact
	extra := db.registerReaderSequence(50)
	if _, overflow := db.registryState(t); overflow != 1 {
		t.Fatalf("expected 1 overflow registration, got %d", overflow)
	}
	if oldest, _ := db.oldestReaderSequence(); oldest != 50 {
		t.Fatalf("expected oldest=50 from overflow, got %d", oldest)
	}

	// Draining the overflow entry restores the slot-only floor
	db.unregisterReaderSequence(extra)
	if _, overflow := db.registryState(t); overflow != 0 {
		t.Fatalf("expected overflow drained, got %d", overflow)
	}
	if oldest, _ := db.oldestReaderSequence(); oldest != 100 {
		t.Fatalf("expected oldest=100 after overflow drain, got %d", oldest)
	}

	// Mixed overflow epochs report the lowest across both stores
	extraLow := db.registerReaderSequence(40)
	extraHigh := db.registerReaderSequence(60)
	extraHighDup := db.registerReaderSequence(60)
	if oldest, _ := db.oldestReaderSequence(); oldest != 40 {
		t.Fatalf("expected oldest=40, got %d", oldest)
	}
	db.unregisterReaderSequence(extraLow)
	if oldest, _ := db.oldestReaderSequence(); oldest != 60 {
		t.Fatalf("expected oldest=60 while extraHigh live, got %d", oldest)
	}
	db.unregisterReaderSequence(extraHigh)
	if oldest, _ := db.oldestReaderSequence(); oldest != 60 {
		t.Fatalf("expected oldest=60 while extraHighDup live, got %d", oldest)
	}
	db.unregisterReaderSequence(extraHighDup)
	if oldest, _ := db.oldestReaderSequence(); oldest != 100 {
		t.Fatalf("expected oldest=100 after overflow drain, got %d", oldest)
	}

	for _, ref := range refs {
		db.unregisterReaderSequence(ref)
	}
	if total, _ := db.registryState(t); total != 0 {
		t.Fatalf("expected empty registry, got %d live", total)
	}
	if _, ok := db.oldestReaderSequence(); ok {
		t.Fatal("expected no readers after draining")
	}
}

func TestReaderSequenceSlotSaturation(t *testing.T) {
	db := &DB{}

	// Fill every slot with readerSlotCountMax registrations of one epoch:
	// dup labels make saturation spill across the whole table first
	refs := make([]readerSlotRef, 0, readerSlotCount*readerSlotCountMax)
	for i := 0; i < readerSlotCount*readerSlotCountMax; i++ {
		refs = append(refs, db.registerReaderSequence(500))
	}
	if oldest, ok := db.oldestReaderSequence(); !ok || oldest != 500 {
		t.Fatalf("expected oldest=500, got %d ok=%v", oldest, ok)
	}
	if _, overflow := db.registryState(t); overflow != 0 {
		t.Fatalf("expected no overflow registrations, got %d", overflow)
	}

	// The next registration of the same epoch lands in the overflow list
	// without disturbing any slot word
	extra := db.registerReaderSequence(500)
	if _, overflow := db.registryState(t); overflow != 1 {
		t.Fatalf("expected 1 overflow registration, got %d", overflow)
	}
	if oldest, _ := db.oldestReaderSequence(); oldest != 500 {
		t.Fatalf("expected oldest=500 with overflow, got %d", oldest)
	}

	db.unregisterReaderSequence(extra)
	for _, ref := range refs {
		db.unregisterReaderSequence(ref)
	}
	if total, _ := db.registryState(t); total != 0 {
		t.Fatalf("expected empty registry, got %d live", total)
	}
	if _, ok := db.oldestReaderSequence(); ok {
		t.Fatal("expected no readers after draining")
	}
}

func TestReaderSequenceSlotsConcurrent(t *testing.T) {
	db := &DB{}

	// Concurrent registrations across moving epochs must never lose counts
	// or report a floor above a live reader. One goroutine per slot holds an
	// old epoch while the others churn epochs above it
	pinned := db.registerReaderSequence(1000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 20000; i++ {
				seq := 1001 + int64(rng.Intn(64))
				ref := db.registerReaderSequence(seq)
				if oldest, ok := db.oldestReaderSequence(); !ok || oldest > seq {
					// The pinned floor keeps oldest at most 1000, so a miss
					// or an over-report here means the protocol broke
					t.Errorf("oldest=%d ok=%v for live seq=%d", oldest, ok, seq)
					return
				}
				db.unregisterReaderSequence(ref)
			}
		}(g)
	}
	wg.Wait()
	db.unregisterReaderSequence(pinned)

	if total, _ := db.registryState(t); total != 0 {
		t.Fatalf("expected empty registry, got %d live", total)
	}
	if _, ok := db.oldestReaderSequence(); ok {
		t.Fatal("expected no readers after draining")
	}
}
