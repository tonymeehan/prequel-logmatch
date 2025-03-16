package match

import "testing"

func TestSetSingle(t *testing.T) {

	var (
		clock  int64 = 0
		window int64 = 10
	)
	sm, err := NewMatchSet(window, "alpha")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	hits := sm.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+1 {
		t.Errorf("Expected 3,4,5,8 got: %v", hits)
	}
}

// -*****************
// A--------E-------
// -----C-------G-H--
// --B-----D--F------

// Should see {A,C,B, {E,G,D}
func TestSetSimple(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 50
	)
	sm, err := NewMatchSet(window, "alpha", "beta", "gamma")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	hits := sm.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 2, Line: "gamma"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 3, Line: "beta"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+1 || hits.Logs[1].Timestamp != clock+3 || hits.Logs[2].Timestamp != clock+2 {
		t.Errorf("Expected 1,2,3 got: %v", hits)
	}

	hits = sm.Scan(LogEntry{Timestamp: clock + 4, Line: "gamma"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 5, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 6, Line: "gamma"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 7, Line: "beta"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+5 || hits.Logs[1].Timestamp != clock+7 || hits.Logs[2].Timestamp != clock+4 {
		t.Errorf("Expected 4,6,5 got: %v", hits)
	}

	if sm.hotMask != 0b100 {
		t.Errorf("Expected hotMask == 0b100, got %b", sm.hotMask)
	}

	hits = sm.Scan(LogEntry{Timestamp: clock + 8, Line: "beta"})
	testNoFire(t, hits)

	if sm.hotMask != 0b110 {
		t.Errorf("Expected hotMask == 0b110, got %b", sm.hotMask)
	}

}

// -*****************
// A----------D------
// --------C---------
// -----B-------E----

// With window of 5. should see {D,C,B}
func TestSetWindow(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 5
	)
	sm, err := NewMatchSet(window, "alpha", "beta", "gamma")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	hits := sm.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 4, Line: "gamma"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 7, Line: "beta"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 8, Line: "alpha"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+8 || hits.Logs[1].Timestamp != clock+7 || hits.Logs[2].Timestamp != clock+4 {
		t.Errorf("Expected 1,2,3 got: %v", hits)
	}

	hits = sm.Scan(LogEntry{Timestamp: clock + 9, Line: "gamma"})
	testNoFire(t, hits)

	if sm.hotMask != 0b100 {
		t.Errorf("Expected hotMask == 0b100, got %b", sm.hotMask)
	}
}

// Dupe timestamps are tolerated.
func TestSetDupeTimestamps(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 5
	)
	sm, err := NewMatchSet(window, "alpha", "beta", "gamma")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	hits := sm.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 1, Line: "gamma"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 1, Line: "beta"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}
}
