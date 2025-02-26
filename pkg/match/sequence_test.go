package match

import (
	"testing"
	"time"
)

// Test a simple sequence fire as expected.
func TestSequenceSimple(t *testing.T) {
	sm, err := NewMatchSeq(time.Second, "shrubbery", "america")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Start with a non matching line; should not fire.
	hits := sm.Scan(LogEntry{Timestamp: time.Now().UnixNano(), Line: "nope"})
	testNoFire(t, hits)

	// Next a line that matches the second but not the first; should be ignored.
	hits = sm.Scan(LogEntry{Timestamp: time.Now().UnixNano(), Line: "I live in america"})
	testNoFire(t, hits)

	// Ok, let's match the first item; should not fire.
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Bring me a shrubbery"}
	hits = sm.Scan(ev1)
	testNoFire(t, hits)

	// Ok, match the second term; we should fire.
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Living in america! I feel good!"}
	hits = sm.Scan(ev2)

	if hits.Cnt != 1 {
		t.Errorf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail equal logs")
	}
}

// Test sequence overlap processing.
func TestSequenceOverlap(t *testing.T) {
	sm, err := NewMatchSeq(time.Second, "shrubbery", "america", "eleven")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Ok, let's match the first item; should not fire.
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Bring me a shrubbery"}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	// Ok, let's match the first item again; should not fire.
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Bring me a shrubbery now"}
	hits = sm.Scan(ev2)
	testNoFire(t, hits)

	// Fire second event, should not cause a match
	ev3 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Got to, this is america!"}
	hits = sm.Scan(ev3)
	testNoFire(t, hits)

	// Add third matching instance of first term; still should not fire
	ev4 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "A shrubbery is what I would like please."}
	hits = sm.Scan(ev4)
	testNoFire(t, hits)

	// Ok, fire the third matching term; the first and second items should fire, but not the third.
	ev5 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "The numbers all go to eleven"}
	hits = sm.Scan(ev5)

	if hits.Cnt != 2 {
		t.Errorf("Expected hits.Cnt == 2, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs[:3], []LogEntry{ev1, ev3, ev5}) {
		t.Errorf("Fail equal logs")
	}

	if !testEqualLogs(t, hits.Logs[3:], []LogEntry{ev2, ev3, ev5}) {
		t.Errorf("Fail equal logs")
	}

	// Cool, now fire second matcher; should not fire
	ev6 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Made in america!"}
	hits = sm.Scan(ev6)
	testNoFire(t, hits)

	// Add another starter to create a new frame, should not fire.
	ev7 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Mares eat oats, not shrubbery."}
	hits = sm.Scan(ev7)
	testNoFire(t, hits)

	// This should fire the hotFrame, but not pending.
	ev8 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "One more than ten is eleven."}
	hits = sm.Scan(ev8)

	if hits.Cnt != 1 {
		t.Errorf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev4, ev6, ev8}) {
		t.Errorf("Fail log match")
	}

	//  Run second match; should not fire
	ev9 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "I took a swim in the Gulf of america."}
	hits = sm.Scan(ev9)
	testNoFire(t, hits)

	// Just for fun, fire some noops
	fireNoops(t, sm, 11)

	// Run third match, last pending should fire.
	ev10 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "One more than ten is eleven."}
	hits = sm.Scan(ev10)

	if hits.Cnt != 1 {
		t.Errorf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev7, ev9, ev10}) {
		t.Errorf("Fail equal logs")
	}

	expectCleanState(t, sm)
}

// Test that sequence times out after window.
func TestSequenceWindow(t *testing.T) {
	sm, err := NewMatchSeq(time.Second, "frank", "burns")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// First event matching first term, should  not fire.
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Let's be frank."}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	// Wait the window
	time.Sleep(time.Duration(sm.window))

	// Fire second matching term, should not fire.
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Ouch, that burns."}
	hits = sm.Scan(ev2)
	testNoFire(t, hits)

	expectCleanState(t, sm)
}

// Test that sequence times out after window even if pending match exists.
func TestSequenceWindow2(t *testing.T) {
	sm, err := NewMatchSeq(time.Second, "frank", "burns")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Scan first event, should not fire.
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Let's be frank."}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	// Fire matching first event again, should pend and not fire.
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Let's be frank for reals."}
	hits = sm.Scan(ev2)
	testNoFire(t, hits)

	// Wait the window; this should timeout both intial events.
	time.Sleep(time.Duration(sm.window))

	// Now fire ev2, should get no match because intial events timed out on window.
	ev3 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Ouch, that burns."}
	hits = sm.Scan(ev3)
	testNoFire(t, hits)

	expectCleanState(t, sm)

}

// Time window timeout on overlapping sequences.
func TestSequenceWindowOverlap(t *testing.T) {
	sm, err := NewMatchSeq(time.Second, "frank", "burns")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Set up partial match, should not fire
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Let's be frank."}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	// Just for fun, fire some noops
	fireNoops(t, sm, 100)

	// Wait for timer to fire;
	time.Sleep(time.Duration(sm.window))

	// Now scan matching ev2, should not fire.
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Ouch, that burns."}
	hits = sm.Scan(ev2)
	testNoFire(t, hits)

	// Validate that the events are good by firing two in a row immediately.
	// (Adjust timestamps to adhere to increasing invariant)
	ev1.Timestamp = time.Now().UnixNano()
	hits = sm.Scan(ev1)
	testNoFire(t, hits)

	// Scan ev2 again with new timestamp. should fire.
	ev2.Timestamp = time.Now().UnixNano()
	hits = sm.Scan(ev2)

	if hits.Cnt != 1 {
		t.Errorf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail log match")
	}

	// Fire two initial frame matchers, somewhat delayed from each other.
	ev3 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "The name is frank."}
	hits = sm.Scan(ev3)
	testNoFire(t, hits)

	time.Sleep(time.Duration(sm.window) / 2)

	ev4 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "The name is frank again."}
	hits = sm.Scan(ev4)
	testNoFire(t, hits)

	// Wait for first item to roll off
	time.Sleep(time.Duration(sm.window) / 2)

	// Now fire second matcher; should complete pending frame.
	ev5 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "OOOOH burns"}
	hits = sm.Scan(ev5)

	if hits.Cnt != 1 {
		t.Errorf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev4, ev5}) {
		t.Errorf("Fail log match")
	}

	expectCleanState(t, sm)
}

// Demonstrate that we can match N copies of the same line
func TestMatchDupes(t *testing.T) {
	sm, err := NewMatchSeq(time.Minute, "frank", "frank", "frank")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Set up partial match, should not fire
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Let's be frank."}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	// Set up partial match, should not fire
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Let's be frank duex."}
	hits = sm.Scan(ev2)
	testNoFire(t, hits)

	// Third time is a charm; fire.

	ev3 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Let's be frank trois."}
	hits = sm.Scan(ev3)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

}

func BenchmarkSequenceMisses(b *testing.B) {
	sm, err := NewMatchSeq(time.Second, "frank", "burns")
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	noop := LogEntry{Line: "NOOP", Timestamp: time.Now().UnixNano()}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		noop.Timestamp += 1
		sm.Scan(noop)
	}
}

func fireNoops(t *testing.T, sm Matcher, n int) {
	// Just for fun, fire some noops
	for i := 0; i < n; i++ {
		hits := sm.Scan(LogEntry{Timestamp: time.Now().UnixNano(), Line: "NOOP"})

		if hits.Cnt != 0 {
			t.Errorf("Expected hits.Cnt == 0, got %v", hits.Cnt)
		}

		if hits.Logs != nil {
			t.Fatalf("Expected nil hits.Logs")
		}
	}
}

func testNoFire(t *testing.T, hits Hits) {
	if hits.Cnt != 0 {
		t.Errorf("Expected hits.Cnt == 0, got %v", hits.Cnt)
	}

	if hits.Logs != nil {
		t.Errorf("Expected nil hits.Logs")
	}
}

// Expect clean internal state
func expectCleanState(t *testing.T, sm *MatchSeq) {

	// Check internal state
	if sm.hotFrame.nActive != 0 {
		t.Errorf("Expected inactive, got %v", sm.hotFrame.nActive)
	}

	if sm.pendFrames != nil {
		t.Errorf("Expected no pend famres")
	}
}

func BenchmarkSequenceHitSequence(b *testing.B) {
	sm, err := NewMatchSeq(time.Second, "frank", "burns")
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	ts := time.Now().UnixNano()
	ev1 := LogEntry{Line: "Let's be frank"}
	ev2 := LogEntry{Line: "Mr burns I am"}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ev1.Timestamp = ts
		ev2.Timestamp = ts + 1
		ts += 2
		sm.Scan(ev1)
		m := sm.Scan(ev2)
		if m.Cnt != 1 {
			b.FailNow()
		}
	}
}

func BenchmarkSequenceHitOverlap(b *testing.B) {
	sm, err := NewMatchSeq(time.Second, "frank", "burns")
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	var (
		ts  = time.Now().UnixNano()
		ev1 = LogEntry{Line: "Let's be frank"}
		ev2 = LogEntry{Line: "Mr burns I am"}
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ev1.Timestamp = ts
		sm.Scan(ev1)
		ts += 1
		ev1.Timestamp = ts
		sm.Scan(ev1)
		ts += 1
		ev1.Timestamp = ts
		sm.Scan(ev1)
		ts += 1
		ev2.Timestamp = ts
		ts += 1
		m := sm.Scan(ev2)
		if m.Cnt != 3 {
			b.FailNow()
		}
	}
}
