package match

import (
	"testing"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
	"github.com/rs/zerolog"
)

//********
// --A--B--C-----
// -----------D--

// Should fire *ONLY* {A,D},
// not {A,D}, {B,D}, {C,D}

func TestSeqInverseBadReset(t *testing.T) {
	var (
		window int64 = 10

		resets = []ResetT{
			{
				Term:   "Shutdown initiated",
				Anchor: 11, // Bad anchor
			},
		}
	)

	_, err := NewInverseSeq(window, []string{"alpha", "beta"}, resets)
	if err != ErrAnchorRange {
		t.Fatalf("Expected err == ErrAnchorRange, got %v", err)
	}
}

//********
// --A----------
// ----------B--

// Fire B outside of window, should fail.

func TestSeqInverseSimpleOutOfWindowMatchWithAbsoluteReset(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 10
	)
	sm, err := NewInverseSeq(window, []string{"alpha", "beta"}, []ResetT{
		{
			Term:     "badterm",
			Window:   50,
			Absolute: true,
		},
	})
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	hits := sm.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})
	testNoFire(t, hits)

	hits = sm.Scan(LogEntry{Timestamp: clock + window + 2, Line: "beta"})
	testNoFire(t, hits)

}

//********
// --A--B--C-----
// -----------D--

// Should fire *ONLY* {A,D},
// not {A,D}, {B,D}, {C,D}

func TestSeqInverseOverFire(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 10
	)
	sm, err := NewInverseSeq(window, []string{"alpha", "beta"}, nil)
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

// Create a match with an inverse that has a long reset window.
// Should still fire even if the last emitted time is way out of window.
func TestSeqInverseSequenceManualEval(t *testing.T) {
	var (
		clock   int64 = 1
		sWindow int64 = 10
		rWindow int64 = 20
	)

	iq, err := NewInverseSeq(
		sWindow,
		[]string{"alpha", "beta"},
		[]ResetT{
			{
				Term:     "Shutdown initiated",
				Window:   rWindow,
				Absolute: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Emit valid sequence in order, should not fire until inverse timer
	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "beta"})
	testNoFire(t, hits)

	hits = iq.Eval(clock + 10000)

	if hits.Cnt != 1 {
		t.Fatalf("Expected 1 hits, got: %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+1 ||
		hits.Logs[1].Timestamp != clock+2 {
		t.Errorf("Expected 1,2 got: %v", hits)
	}
}

// Event order: R, A, B
// -**********
// --A-----------
// ----B---------
// -R------------

// Setup a reset window, and assert reset at end of window.  Should not fire.

func TestSeqInverseSequenceSlideLeft(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		slide     = int64(-1 * time.Second)
		absWindow = int64(time.Second)
	)
	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]ResetT{
			{
				Term:     "badterm1",
				Slide:    slide,
				Window:   absWindow,
				Absolute: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Fire a negative term slightout side of the left slide.
	nv1 := LogEntry{Timestamp: clock, Line: "badterm1"}
	hits := iq.Scan(nv1)
	testNoFire(t, hits)

	// Scan first matcher right on the slide boundary
	ev1 := LogEntry{Timestamp: clock + absWindow, Line: "Match alpha."}
	hits = iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan first matcher right on the slide boundary; should fail due to slide.
	ev2 := LogEntry{Timestamp: clock + absWindow + 1, Line: "Match beta."}
	hits = iq.Scan(ev2)
	testNoFire(t, hits)

	// Ok let's fire again now passed the slide window. Should come through.

	// Scan first matcher right on the slide boundary
	ev3 := LogEntry{Timestamp: clock + absWindow + 2, Line: "Match alpha."}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Scan first matcher right on the slide boundary; should fail due to slide.
	ev4 := LogEntry{Timestamp: clock + absWindow + 3, Line: "Match beta."}
	hits = iq.Scan(ev4)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev3, ev4}) {
		t.Errorf("Fail logs equal")
	}
}

// Event order: A,B,C
// -**********
// -A-------------
// --B------------
// ---C-----------
// -------------R-

// Setup a reset window, and assert reset at end of window.  Should not fire.

func TestSeqInverseRelativeResetWindowMiss(t *testing.T) {
	var (
		clock   int64 = 1
		sWindow int64 = 3
		rWindow int64 = 10
	)

	iq, err := NewInverseSeq(
		sWindow,
		[]string{"alpha", "beta", "gamma"},
		[]ResetT{
			{
				Term:   "reset",
				Window: rWindow,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan first matcher
	ev2 := LogEntry{Timestamp: clock + 1, Line: "Match beta."}
	hits = iq.Scan(ev2)
	testNoFire(t, hits)

	// Scan third matcher; should fire the seqeuence but delay on reset
	ev3 := LogEntry{Timestamp: clock + 2, Line: "Match gamma"}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Eval current time, should not fire.
	hits = iq.Eval(clock + 2)
	testNoFire(t, hits)

	// Assert reset event immediately before window
	clock += 11
	rv1 := LogEntry{Timestamp: clock, Line: "Match reset"}
	hits = iq.Scan(rv1)
	testNoFire(t, hits)

	// Eval much later; should have reset
	hits = iq.Eval(clock + 50)
	testNoFire(t, hits)
}

func TestSeqInverseSlideRight(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Second * 500)
		slide     = int64(time.Second)
		absWindow = int64(time.Second)
	)
	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]ResetT{
			{
				Term:     "badterm1",
				Slide:    slide,
				Window:   absWindow,
				Absolute: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan first matcher
	ev2 := LogEntry{Timestamp: clock + 1, Line: "Match beta."}
	hits = iq.Scan(ev2)
	testNoFire(t, hits)

	// Fire a negative term within the window slide window.
	// This should negate the first match
	nv1 := LogEntry{Timestamp: clock + absWindow + slide - 1, Line: "badterm1"}
	hits = iq.Scan(nv1)
	testNoFire(t, hits)

	// Ok let's fire again now passed the slide window. Should come through.

	// Scan first matcher right on the slide boundary
	clock += absWindow + slide

	ev3 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Scan first matcher right on the slide boundary; should fail due to slide.
	ev4 := LogEntry{Timestamp: clock + 1, Line: "Match beta."}
	hits = iq.Scan(ev4)
	testNoFire(t, hits) // Can't fire until past the slide window

	ev5 := LogEntry{Timestamp: clock + absWindow + slide - 1, Line: "NOOP"}
	hits = iq.Scan(ev5)
	testNoFire(t, hits) // Can't fire until past the slide window

	// Should be good now
	ev6 := LogEntry{Timestamp: clock + absWindow + slide, Line: "NOOP"}
	hits = iq.Scan(ev6)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev3, ev4}) {
		t.Errorf("Fail logs equal")
	}
}

func TestSeqInverseSlideAnchor(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		absWindow = int64(time.Minute)
	)
	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]ResetT{
			{
				Term:     "badterm1",
				Window:   absWindow,
				Absolute: true,
				Anchor:   1,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan first matcher
	ev2 := LogEntry{Timestamp: clock + int64(window), Line: "Match beta."}
	hits = iq.Scan(ev2)
	testNoFire(t, hits)

	// Should not fire as we are still beneath the shifted window
	ev3 := LogEntry{Timestamp: clock + int64(window) + absWindow - 1, Line: "Nope"}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Should should fire as we are outside window
	ev4 := LogEntry{Timestamp: clock + int64(window) + absWindow, Line: "Nope"}
	hits = iq.Scan(ev4)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail logs equal")
	}
}

// Event order: A,R,B,C
// -**********
// -A-----------
// ---B---------
// -----C---D---
// --R----------

// Anchor absolute reset window with neg slide  on line 2
// Should disallow A,B,C, but A,B,D should fire

func TestSeqInverseAbsSlideResetContinue(t *testing.T) {

	var (
		clock   int64 = 1
		sWindow int64 = 50
		rWindow int64 = 5
		slide   int64 = -5
	)

	iq, err := NewInverseSeq(
		sWindow,
		[]string{"alpha", "beta", "gamma"},
		[]ResetT{
			{
				Term:     "reset",
				Window:   rWindow,
				Absolute: true,
				Anchor:   2,
				Slide:    slide,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	rv1 := LogEntry{Timestamp: clock + 1, Line: "Match reset."}
	hits = iq.Scan(rv1)
	testNoFire(t, hits)

	// Scan second matcher
	ev2 := LogEntry{Timestamp: clock + 2, Line: "Match beta."}
	hits = iq.Scan(ev2)
	testNoFire(t, hits)

	// Should not fire as we are still beneath the shifted window
	ev3 := LogEntry{Timestamp: clock + 3, Line: "Match gamma"}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Should  fire as we are outside window on inverse term
	ev4 := LogEntry{Timestamp: clock + 10, Line: "Match gamma"}
	hits = iq.Scan(ev4)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

}

func TestSeqInverseRelative(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 50
	)

	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]ResetT{
			{Term: "badterm1"},
			{Term: "badterm2"},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	clock += 1
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan second matcher
	clock += 1
	ev2 := LogEntry{Timestamp: clock, Line: "Match beta."}
	hits = iq.Scan(ev2)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail logs equal")
	}

	// Now fire another start event; nothing should fire
	clock += 1
	ev3 := LogEntry{Timestamp: clock, Line: "Match alpha part deux."}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Fire a inverse event
	clock += 1
	ev4 := LogEntry{Timestamp: clock, Line: "This is badterm1"}
	hits = iq.Scan(ev4)
	testNoFire(t, hits)

	// Fire end event, it should not fire due to inverse above
	clock += 1
	ev5 := LogEntry{Timestamp: clock, Line: "beta my friend"}
	hits = iq.Scan(ev5)
	testNoFire(t, hits)

	// Similarly this should fail because start was removed from seq matcher
	clock += 1
	ev6 := LogEntry{Timestamp: clock, Line: "beta my friend"}
	hits = iq.Scan(ev6)
	testNoFire(t, hits)

	// Fire a inverse event
	clock += 1
	ev7 := LogEntry{Timestamp: clock, Line: "This is badterm7"}
	hits = iq.Scan(ev7)
	testNoFire(t, hits)

	// Fire alpha then beta, should fire because inverse was before alpha
	clock += 1
	ev8 := LogEntry{Timestamp: clock, Line: "Match alpha part trois."}
	hits = iq.Scan(ev8)
	testNoFire(t, hits)

	clock += 1
	ev9 := LogEntry{Timestamp: clock, Line: "beta again"}
	hits = iq.Scan(ev9)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev8, ev9}) {
		t.Errorf("Fail logs equal")
	}

}

func TestSeqInverseAbsoluteHit(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		absWindow = int64(time.Second)
	)

	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]ResetT{
			{Term: "badterm1"},
			{Term: "badterm2"},
			{
				Term:     "badterm3",
				Absolute: true,
				Window:   absWindow,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan second matcher, exactly within the window.
	ev2 := LogEntry{Timestamp: clock + int64(window), Line: "Match beta."}
	hits = iq.Scan(ev2)

	// Should not hit  until the absolute window is up
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
	}

	// Assert log one nanosecond before the absolute window
	ev3 := LogEntry{Timestamp: clock + absWindow - 1, Line: "NOOP"}
	hits = iq.Scan(ev3)

	// Should not hit  until the absolute window is up
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
	}

	// Now assert log at exactly the absolute window, should fire
	ev4 := LogEntry{Timestamp: clock + absWindow, Line: "NOOP"}
	hits = iq.Scan(ev4)

	// Should not hit  until the absolute window is up
	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail logs equal")
	}
}

// Create an absolute window and fire an inverse into that window. Should drop.
func TestSeqInverseAbsoluteMiss(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		absWindow = int64(time.Second)
	)

	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]ResetT{
			{Term: "badterm1"},
			{Term: "badterm2"},
			{
				Term:     "badterm3",
				Absolute: true,
				Window:   absWindow,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan second matcher, exactly within the window.
	ev2 := LogEntry{Timestamp: clock + int64(window), Line: "Match beta."}
	hits = iq.Scan(ev2)

	// Should not hit  until the absolute window is up
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
	}

	// Fire a negative term into the absolute window
	nv := LogEntry{Timestamp: clock + absWindow - 2, Line: "badterm3"}
	hits = iq.Scan(nv)

	// Should not hit  until the absolute window is up
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
	}

	// Assert log one nanosecond before the absolute window
	ev3 := LogEntry{Timestamp: clock + absWindow - 1, Line: "NOOP"}
	hits = iq.Scan(ev3)

	// Should not hit  until the absolute window is up
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
	}

	// Now assert log at exactly the absolute window, should fire
	ev4 := LogEntry{Timestamp: clock + absWindow, Line: "NOOP"}
	hits = iq.Scan(ev4)

	// Should not hit due to negative term
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}
}

func TestSeqInversePosRelativeOffset(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		relWindow = int64(time.Second)
	)

	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]ResetT{
			{Term: "badterm1"},
			{Term: "badterm2"},
			{
				Term:     "badterm3",
				Absolute: false,
				Window:   relWindow,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan second matcher, exactly within the window.
	ev2 := LogEntry{Timestamp: clock + int64(window), Line: "Match beta."}
	hits = iq.Scan(ev2)

	// Should not hit  until the relative window is up
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
	}

	// Assert log one nanosecond before the relative window
	relDeadline := ev2.Timestamp - ev1.Timestamp + relWindow
	ev3 := LogEntry{Timestamp: clock + relDeadline - 1, Line: "NOOP"}
	hits = iq.Scan(ev3)

	// Should not hit  until the relative window is up
	if hits.Cnt != 0 {
		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
	}

	// Now assert log at exactly the relative window, should fire
	ev4 := LogEntry{Timestamp: clock + relDeadline, Line: "NOOP"}
	hits = iq.Scan(ev4)

	// Should not hit  until the absolute window is up
	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail logs equal")
	}
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
func TestSeqInverseDupes(t *testing.T) {
	var (
		clock   int64 = 0
		sWindow int64 = 10
	)

	iq, err := NewInverseSeq(
		sWindow,
		[]string{
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Mnesia overloaded",
		},
		[]ResetT{},
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
	// clock+6 is the last hot zero event in the window,
	// (if we were doing strict sequential, clock+4 would be the last hot event)
	// adding sWindow + 1 should be out of window.
	hits = iq.Scan(LogEntry{Timestamp: clock + 6 + sWindow + 1, Line: "Mnesia overloaded"})

	if hits.Cnt != 0 {
		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
	}
}

// -*******************
// -123---------
// -123---------
// -123---------
// ----4----5---

func TestSeqInverseDupesWithResetHit(t *testing.T) {
	var (
		clock   int64 = 0
		sWindow int64 = 10
		rWindow int64 = 20
	)

	iq, err := NewInverseSeq(
		sWindow,
		[]string{
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Mnesia overloaded",
		},
		[]ResetT{
			{
				Term:     "Shutdown initiated",
				Window:   rWindow,
				Absolute: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Emit valid sequence in order, should not fire until inverse timer
	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "Discarding message"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "Discarding message"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 3, Line: "Discarding message"})
	testNoFire(t, hits)

	// Emit last item, should not fire.
	hits = iq.Scan(LogEntry{Timestamp: clock + 4, Line: "Mnesia overloaded"})
	testNoFire(t, hits)

	// Emit extra right before window, should not fire
	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow, Line: "Mnesia overloaded"})
	testNoFire(t, hits)

	// Emit extra right at window, should  fire
	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow + 1, Line: "Mnesia overloaded"})

	if hits.Cnt != 1 {
		t.Fatalf("Expected 1 hits, got: %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+1 ||
		hits.Logs[1].Timestamp != clock+2 ||
		hits.Logs[2].Timestamp != clock+3 ||
		hits.Logs[3].Timestamp != clock+4 {
		t.Errorf("Expected 1,2,3,4 got: %v", hits)
	}

	// Emit way in the future, should not fire
	hits = iq.Eval(clock + sWindow*2)

	if hits.Cnt != 0 {
		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
	}
}

// -*******************
// -123---------
// -123---------
// -123---------
// ----4----5R--

// Test that reset right at the end of the window prevents fire.

func TestSeqInverseDupesWithResetFail(t *testing.T) {
	var (
		clock   int64 = 0
		sWindow int64 = 10
		rWindow int64 = 20
	)

	iq, err := NewInverseSeq(
		sWindow,
		[]string{
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Mnesia overloaded",
		},
		[]ResetT{
			{
				Term:     "Shutdown initiated",
				Window:   rWindow,
				Absolute: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Emit valid sequence in order, should not fire until inverse timer
	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "Discarding message"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "Discarding message"})
	testNoFire(t, hits)

	hits = iq.Scan(LogEntry{Timestamp: clock + 3, Line: "Discarding message"})
	testNoFire(t, hits)

	// Emit last item, should not fire.
	hits = iq.Scan(LogEntry{Timestamp: clock + 4, Line: "Mnesia overloaded"})
	testNoFire(t, hits)

	// Emit extra right before window, should not fire
	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow, Line: "Mnesia overloaded"})
	testNoFire(t, hits)

	// Emit reset on edge of window, should not fire
	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow + 1, Line: "Shutdown initiated"})
	testNoFire(t, hits)

	// Fire in the future, should get nothing
	hits = iq.Eval(clock + 1000)
	testNoFire(t, hits)
}

//*******
// -1------4--------------10----------
// ---2--3----------8---9-----11----
// ----------5--6-7---------------12-
// Should fire {1,2,5}, {4,8,12}

func TestSeqInverseGCOldSecondaryTerms(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 50
	)

	sm, err := NewInverseSeq(window, []string{"alpha", "beta", "gamma"}, []ResetT{})
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

	sm.GarbageCollect(clock + window + 1)

	if sm.nActive != 0 {
		t.Errorf("Expected empty state")
	}

}

// Reset terms should be dropped if no matches and no reset lookback.
func TestSeqInverseResetsIgnoredOnNoMatch(t *testing.T) {

	var (
		N           = 11
		clock int64 = 0
	)

	// Create a seq matcher with a negative window reset term
	sm, err := NewInverseSeq(10, []string{"alpha", "beta", "gamma"}, []ResetT{
		{
			Term: "badterm",
		},
	})
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Fire the bad term N times
	for range N {
		clock += 1
		hits := sm.Scan(LogEntry{Timestamp: clock, Line: "badterm"})
		testNoFire(t, hits)
	}

	// Should have zero resets
	if len(sm.resets[0].resets) != 0 {
		t.Fatalf("Expected 0 negative terms, got %v", len(sm.resets[0].resets))
	}
}

func TestSeqInverseNegativesAreGCed(t *testing.T) {

	var (
		N             = 3
		clock   int64 = 0
		sWindow int64 = 50
		rWindow int64 = 20
		rSlide  int64 = -10
	)

	// Create a seq matcher with a negative window reset term
	sm, err := NewInverseSeq(sWindow, []string{"alpha", "beta", "gamma"}, []ResetT{
		{
			Term:   "badterm",
			Slide:  rSlide,
			Window: rWindow,
		},
	})
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	// Fire the bad term N times
	for range N {
		hits := sm.Scan(LogEntry{Timestamp: clock, Line: "badterm"})
		testNoFire(t, hits)
		clock += 1
	}

	// Negative terms with nothing hot w/o lookback have been optimized out.
	if len(sm.resets[0].resets) != 3 {
		t.Fatalf("Expected 3 negative terms, got %v", len(sm.resets[0].resets))
	}

	// Emit noop at full GC window (see calcGCWindow)
	gcWindow := sWindow + rWindow + rSlide
	if rSlide < 0 {
		gcWindow += -rSlide
	}
	hits := sm.Scan(entry.LogEntry{Timestamp: gcWindow, Line: "NOOP"})
	testNoFire(t, hits)

	// We should have some negative terms
	if len(sm.resets[0].resets) != 3 {
		t.Fatalf("Expected 3 negative terms, got %v", len(sm.resets[0].resets))
	}

	// Emit noop right after window
	hits = sm.Scan(entry.LogEntry{Timestamp: gcWindow + 1, Line: "NOOP"})
	testNoFire(t, hits)

	// Should have peeled off one term
	if len(sm.resets[0].resets) != 2 {
		t.Fatalf("Expected 2 negative terms, got %v", len(sm.resets[0].resets))
	}

	// Emit noop right after window +2
	hits = sm.Scan(entry.LogEntry{Timestamp: gcWindow + 2, Line: "NOOP"})
	testNoFire(t, hits)

	// Should have peeled off one term
	if len(sm.resets[0].resets) != 1 {
		t.Fatalf("Expected 1 negative terms, got %v", len(sm.resets[0].resets))
	}

	// Emit noop right after window +3
	hits = sm.Scan(entry.LogEntry{Timestamp: gcWindow + 3, Line: "NOOP"})
	testNoFire(t, hits)

	// Should have peeled off the last term
	if len(sm.resets[0].resets) != 0 {
		t.Fatalf("Expected 0 negative terms, got %v", len(sm.resets[0].resets))
	}

	// GC should be disabled
	if sm.gcMark != disableGC {
		t.Errorf("Expected GC to be disabled, got :%v", sm.gcMark)
	}
}

// Ignore events fired out of order
func TestSeqInverseTimestampOutofOrder(t *testing.T) {
	var (
		clock  int64 = 1
		window int64 = 10
	)

	sm, err := NewInverseSeq(window, []string{"alpha", "gamma"}, nil)
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

func TestSeqInverseDupeTimestamps(t *testing.T) {
	var (
		clock  int64 = 1
		window int64 = 10
	)

	sm, err := NewInverseSeq(window, []string{"alpha", "gamma"}, nil)
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

// --------------------

func BenchmarkSeqInverseMisses(b *testing.B) {
	sm, err := NewInverseSeq(int64(time.Second), []string{"frank", "burns"}, nil)
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

func BenchmarkSeqInverseMissesWithReset(b *testing.B) {

	resets := []ResetT{
		{
			Term:     "badterm",
			Window:   1000,
			Absolute: true,
		},
	}

	sm, err := NewInverseSeq(int64(time.Second), []string{"frank", "burns"}, resets)
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

func BenchmarkSeqInverseHitSequence(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSeq(int64(time.Second), []string{"frank", "burns"}, nil)
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

func BenchmarkSeqInverseHitOverlap(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSeq(10, []string{"frank", "burns"}, nil)
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

func BenchmarkSeqInverseRunawayMatch(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSeq(1000000, []string{"frank", "burns"}, nil)
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
