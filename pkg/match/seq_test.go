package match

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

//********
// --A--B--C-----
// -----------D--

// Should fire *ONLY* {A,D},
// not {A,D}, {B,D}, {C,D}

func TestSeqOverFire(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 10
	)
	sm, err := NewMatchSeq(window, "alpha", "beta")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	hits := sm.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 2, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 3, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + 4, Line: "beta"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+1 ||
		hits.Logs[1].Timestamp != clock+4 {
		t.Errorf("Expected 3,4,5,8 got: %v", hits)
	}
}

// Test a simple sequence fire as expected.
func TestSequenceSimple(t *testing.T) {
	sm, err := NewMatchSeq(int64(time.Second), "shrubbery", "america")
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
	window := int64(time.Minute)
	sm, err := NewMatchSeq(window, "shrubbery", "america", "eleven")
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

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs[:3], []LogEntry{ev1, ev3, ev5}) {
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

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev2, ev6, ev8}) {
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

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev4, ev9, ev10}) {
		t.Errorf("Fail equal logs")
	}

	// Fire out side the window; should cleanup ev7
	sm.GarbageCollect(ev7.Timestamp + window + 1)

	expectCleanState(t, sm)
}

// Test that sequence times out after window.
func TestSequenceWindow(t *testing.T) {
	sm, err := NewMatchSeq(int64(time.Second), "frank", "burns")
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
	sm, err := NewMatchSeq(int64(time.Second), "frank", "burns")
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
	sm, err := NewMatchSeq(int64(time.Second), "frank", "burns")
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
	sm, err := NewMatchSeq(int64(time.Minute), "frank", "frank", "frank")
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

//*******
// -1------4--------------10----------
// ---2--3----------8---9-----11----
// ----------5--6-7---------------12-
// Should fire {1,2,5}, {4,8,12}

func TestSeqGCOldSecondaryTerms(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 50
	)

	sm, err := NewMatchSeq(window, "alpha", "beta", "gamma")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	clock += 1
	ev1 := LogEntry{Timestamp: clock, Line: "alpha"}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	clock += 1
	ev2 := LogEntry{Timestamp: clock, Line: "beta"}
	hits = sm.Scan(ev2)
	testNoFire(t, hits)

	clock += 1
	ev3 := LogEntry{Timestamp: clock, Line: "beta"}
	hits = sm.Scan(ev3)
	testNoFire(t, hits)

	clock += 1
	ev4 := LogEntry{Timestamp: clock, Line: "alpha"}
	hits = sm.Scan(ev4)
	testNoFire(t, hits)

	clock += 1
	ev5 := LogEntry{Timestamp: clock, Line: "gamma"}
	hits = sm.Scan(ev5)

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2, ev5}) {
		t.Errorf("Fail log match")
	}

	clock += 1
	ev6 := LogEntry{Timestamp: clock, Line: "gamma"}
	hits = sm.Scan(ev6)
	testNoFire(t, hits)

	clock += 1
	ev7 := LogEntry{Timestamp: clock, Line: "gamma"}
	hits = sm.Scan(ev7)
	testNoFire(t, hits)

	clock += 1
	ev8 := LogEntry{Timestamp: clock, Line: "beta"}
	hits = sm.Scan(ev8)
	testNoFire(t, hits)

	clock += 1
	ev9 := LogEntry{Timestamp: clock, Line: "beta"}
	hits = sm.Scan(ev9)

	clock += 1
	ev10 := LogEntry{Timestamp: clock, Line: "alpha"}
	hits = sm.Scan(ev10)

	clock += 1
	ev11 := LogEntry{Timestamp: clock, Line: "beta"}
	hits = sm.Scan(ev11)

	clock += 1
	ev12 := LogEntry{Timestamp: clock, Line: "gamma"}
	hits = sm.Scan(ev12)

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev4, ev8, ev12}) {
		t.Errorf("Fail log match")
	}

	sm.GarbageCollect(clock + window)

	expectCleanState(t, sm)
}

// -**********
// --1----3--4-5-6-----
// --1----3--4-5-6-----
// --1----3--4 5-6-----
// ----2-----------7--8

// Because we are using a duplicate term, there is a possibility
// of overlapping fire events.  This test should ensure that
// the sequence matcher is able to handle this case.
// Above should fire {1,3,4,7} and {3,4,5,8}
func TestSeqDupes(t *testing.T) {
	var (
		clock   int64 = 0
		sWindow int64 = 10
	)

	iq, err := NewMatchSeq(
		sWindow,
		"Discarding message",
		"Discarding message",
		"Discarding message",
		"Mnesia overloaded",
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Emit first row.
	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "Discarding message"})
	testNoFire(t, hits)

	// Emit last item, should not fire.
	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "Mnesia overloaded"})
	testNoFire(t, hits)

	// Emit first item 4 times; should not fire until "Mnesia overloaded again"
	hits = iq.Scan(LogEntry{Timestamp: clock + 3, Line: "Discarding message"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 4, Line: "Discarding message"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 5, Line: "Discarding message"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 6, Line: "Discarding message"})
	testNoFire(t, hits)

	// Emit last item, should fire once
	hits = iq.Scan(LogEntry{Timestamp: clock + 7, Line: "Mnesia overloaded"})

	if hits.Cnt != 1 {
		t.Errorf("Expected 1 hits, got: %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+1 ||
		hits.Logs[1].Timestamp != clock+3 ||
		hits.Logs[2].Timestamp != clock+4 ||
		hits.Logs[3].Timestamp != clock+7 {
		t.Fatalf("Expected 1,3,4,7 got: %v", hits)
	}

	// Should emit another
	hits = iq.Scan(LogEntry{Timestamp: clock + 8, Line: "Mnesia overloaded"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected 1 hits, got: %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+3 ||
		hits.Logs[1].Timestamp != clock+4 ||
		hits.Logs[2].Timestamp != clock+5 ||
		hits.Logs[3].Timestamp != clock+8 {
		t.Errorf("Expected 3,4,5,8 got: %v", hits)
	}

	hits = iq.Eval(clock + sWindow*2)

	if hits.Cnt != 0 {
		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
	}

	// Should fail out of window;
	// clock+5 is the last hot zero event in the window,
	// (if we were doing strict sequential, clock+6 would be the last hot event)
	// adding sWindow + 1 should be out of window.
	hits = iq.Scan(LogEntry{Timestamp: clock + 6 + sWindow + 1, Line: "Mnesia overloaded"})

	if hits.Cnt != 0 {
		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
	}
}

func BenchmarkSequenceMisses(b *testing.B) {
	sm, err := NewMatchSeq(int64(time.Second), "frank", "burns")
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

// Ignore events fired out of order
func TestSeqTimestampOutofOrder(t *testing.T) {
	var (
		clock  int64 = 1
		window int64 = 10
	)

	sm, err := NewMatchSeq(window, "alpha", "gamma")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Set up partial match, should not fire
	ev1 := LogEntry{Timestamp: clock, Line: "alpha"}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	// Fire second matcher at same time; should fire
	// since we are not enforcing strict ordering.
	ev2 := LogEntry{Timestamp: clock - 1, Line: "gamma"}
	hits = sm.Scan(ev2)
	testNoFire(t, hits)
}

// Fire events on same timestamp, should match
// as we are currently not enforcing strict ordering.

func TestSeqDupeTimestamps(t *testing.T) {
	var (
		clock  int64 = 1
		window int64 = 10
	)

	sm, err := NewMatchSeq(window, "alpha", "gamma")
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Set up partial match, should not fire
	ev1 := LogEntry{Timestamp: clock, Line: "alpha"}
	hits := sm.Scan(ev1)
	testNoFire(t, hits)

	// Fire second matcher at same time; should fire
	// since we are not enforcing strict ordering.
	ev2 := LogEntry{Timestamp: clock, Line: "gamma"}
	hits = sm.Scan(ev2)

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail log match")
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
	if sm.nActive != 0 {
		t.Errorf("Expected clean state, got %v", sm.nActive)
	}
}

func BenchmarkSequenceHitSequence(b *testing.B) {
	sm, err := NewMatchSeq(int64(time.Second), "frank", "burns")
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
	sm, err := NewMatchSeq(int64(time.Second), "frank", "burns")
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
		if m.Cnt != 1 {
			b.FailNow()
		}
	}
}

func BenchmarkSeqRunawayMatch(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewMatchSeq(1000000, "frank", "burns")
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	var (
		ev1 = LogEntry{Line: "Let's be frank"}
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ev1.Timestamp += 1
		sm.Scan(ev1)
	}
}
