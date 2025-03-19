package match

import (
	"testing"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
	"github.com/rs/zerolog"
)

func TestSetInverseBadReset(t *testing.T) {
	var (
		window int64 = 10

		resets = []ResetT{
			{
				Term:   "Shutdown initiated",
				Anchor: 11, // Bad anchor
			},
		}
	)

	_, err := NewInverseSet(window, []string{"alpha", "beta"}, resets)
	if err != errAnchorRange {
		t.Fatalf("Expected err == errAnchorRange, got %v", err)
	}
}

func TestSetInverseSingle(t *testing.T) {

	var (
		clock  int64 = 0
		window int64 = 10
	)
	sm, err := NewInverseSet(window, []string{"alpha"}, nil)
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
func TestSetInverseSimple(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 50
	)
	sm, err := NewInverseSet(window, []string{"alpha", "beta", "gamma"}, nil)
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
func TestSetInverseWindow(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 5
	)
	sm, err := NewInverseSet(window, []string{"alpha", "beta", "gamma"}, nil)
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
func TestSetInverseDupeTimestamps(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 5
	)
	sm, err := NewInverseSet(window, []string{"alpha", "beta", "gamma"}, nil)
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

// *********************
// -A-------------------
// --B------------------
// ---------------------
// Create a match with an inverse that has a long reset window.
// Should still fire even if the last emitted time is way out of window.
func TestSetInverseManualEval(t *testing.T) {
	var (
		clock   int64 = 1
		sWindow int64 = 10
		rWindow int64 = 20
	)

	iq, err := NewInverseSet(
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

	// clock + rWindow == 21
	// First events is at 1. So we are still within the reset window.
	hits = iq.Eval(clock + rWindow)
	if hits.Cnt != 0 {
		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
	}

	// clock + rWindow + 1== 22
	// First events is at 1. So we are now oustide the reset window.
	hits = iq.Eval(clock + rWindow + 1)
	if hits.Cnt != 1 {
		t.Fatalf("Expected 1 hits, got: %v", hits.Cnt)
	}

	if hits.Logs[0].Timestamp != clock+1 ||
		hits.Logs[1].Timestamp != clock+2 {
		t.Errorf("Expected 1,2 got: %v", hits)
	}
}

// -**********
// --A--------D---
// ----B---C------
// -R------------

// Slide left, deny first set, allow second set.
// Should deny {A,B}, {A,C}, but allow {D,B}

func TestSetInverseSlideLeft(t *testing.T) {
	var (
		clock     int64 = 0
		window    int64 = 10
		slide     int64 = -5
		absWindow int64 = 5
	)
	iq, err := NewInverseSet(
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

	// Fire a negative term slightly outside of the left slide.
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
	ev3 := LogEntry{Timestamp: clock + absWindow + 2, Line: "Match beta."}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Scan first matcher right on the slide boundary; should fail due to slide.
	ev4 := LogEntry{Timestamp: clock + absWindow + 3, Line: "Match alpha."}
	hits = iq.Scan(ev4)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev4, ev2}) {
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

func TestSetInverseRelativeResetWindowMiss(t *testing.T) {
	var (
		clock   int64 = 1
		sWindow int64 = 3
		rWindow int64 = 10
	)

	iq, err := NewInverseSet(
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

//***
// -A-------C------
// --B-------D-----
// -----R-----------

// Should fail {A,B} on R, but fire {C,D} after absolute timeout.

func TestSetInverseSlideRight(t *testing.T) {
	var (
		clock     int64 = 0
		window    int64 = 10
		slide     int64 = 20
		absWindow int64 = 15
	)
	iq, err := NewInverseSet(
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
	clock = absWindow + slide

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

func TestSetInverseSlideAnchor(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		absWindow = int64(time.Minute)
	)
	iq, err := NewInverseSet(
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

func TestSetInverseAbsSlideResetContinue(t *testing.T) {

	var (
		clock   int64 = 1
		sWindow int64 = 50
		rWindow int64 = 5
		slide   int64 = -5
	)

	iq, err := NewInverseSet(
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

//***
//-1-3-------8--
//--2--5-6----9-
//----4----7---

// Two relative resets.
// {1,2} should fire.
// {8,9} should fire

func TestSetInverseRelative(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 50
	)

	iq, err := NewInverseSet(
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

	// Similarly this should fail because start was removed from  matcher
	clock += 1
	ev6 := LogEntry{Timestamp: clock, Line: "beta my friend"}
	hits = iq.Scan(ev6)
	testNoFire(t, hits)

	// Fire a inverse event
	clock += 1
	ev7 := LogEntry{Timestamp: clock, Line: "This is badterm2"}
	hits = iq.Scan(ev7)
	testNoFire(t, hits)

	clock += 1
	ev8 := LogEntry{Timestamp: clock, Line: "Match alpha part trois."}
	hits = iq.Scan(ev8)
	testNoFire(t, hits)

	clock += 1
	ev9 := LogEntry{Timestamp: clock, Line: "beta again"}
	hits = iq.Scan(ev9)

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev8, ev9}) {
		t.Errorf("Fail logs equal")
	}

}

//*****
//-A--------
//-----B----

// Simple absolute window HIT test.
// Should not fire until absolute window ends.

func TestSetInverseAbsoluteHit(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		absWindow = int64(time.Second)
	)

	iq, err := NewInverseSet(
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

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail logs equal")
	}
}

// Create an absolute window and fire an inverse into that window. Should drop.
func TestSetInverseAbsoluteMiss(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		absWindow = int64(time.Second)
	)

	iq, err := NewInverseSet(
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

func TestSetInversePosRelativeOffset(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = int64(time.Millisecond * 500)
		relWindow = int64(time.Second)
	)

	iq, err := NewInverseSet(
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

//*******
// -1------4--------------10----------
// ---2--3----------8---9-----11----
// ----------5--6-7---------------12-
// Should fire {1,2,5}, {4,3,6}, {10,8,7}

func TestSetInverseGCOldSecondaryTerms(t *testing.T) {
	var (
		clock  int64 = 0
		window int64 = 50
	)

	sm, err := NewInverseSet(window, []string{"alpha", "beta", "gamma"}, nil)
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

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev4, ev3, ev6}) {
		t.Errorf("Fail log match")
	}

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
	testNoFire(t, hits)

	clock += 1
	ev10 := LogEntry{Timestamp: clock, Line: "alpha"}
	hits = sm.Scan(ev10)

	if hits.Cnt != 1 {
		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev10, ev8, ev7}) {
		t.Errorf("Fail log match")
	}

	clock += 1
	ev11 := LogEntry{Timestamp: clock, Line: "beta"}
	hits = sm.Scan(ev11)
	testNoFire(t, hits)

	clock += 1
	ev12 := LogEntry{Timestamp: clock, Line: "gamma"}
	hits = sm.Scan(ev12)
	testNoFire(t, hits)

	sm.GarbageCollect(clock + window)

	if sm.hotMask.Zeros() {
		t.Errorf("Expected non empty state")
	}

	sm.GarbageCollect(clock + window + 1)

	if !sm.hotMask.Zeros() {
		t.Errorf("Expected  empty state")
	}
}

// Reset terms should be dropped if no matches and no reset lookback.
func TestSetInverseResetsIgnoredOnNoMatch(t *testing.T) {

	var (
		N           = 11
		clock int64 = 0
	)

	// Create a seq matcher with a negative window reset term
	sm, err := NewInverseSet(10, []string{"alpha", "beta", "gamma"}, []ResetT{
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

// ******
// -------- A
// -------- B
// --234--- R

// Create a set with a negative reset window.
// Because there is a negative window, reset terms must
// be kept around, but they should be GC'd after window.

func TestSetInverseResetsAreGCed(t *testing.T) {

	var (
		N             = 3
		clock   int64 = 0
		sWindow int64 = 50
		rWindow int64 = 20
		rSlide  int64 = -10
	)

	// Create a seq matcher with a negative window reset term
	sm, err := NewInverseSet(sWindow, []string{"alpha", "beta", "gamma"}, []ResetT{
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
func TestSetInverseTimestampOutofOrder(t *testing.T) {
	var (
		clock  int64 = 1
		window int64 = 10
	)

	sm, err := NewInverseSet(window, []string{"alpha", "gamma"}, nil)
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

// --------------------

func BenchmarkSetInverseMisses(b *testing.B) {
	sm, err := NewInverseSet(int64(time.Second), []string{"frank", "burns"}, nil)
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

func BenchmarkSetInverseMissesWithReset(b *testing.B) {

	resets := []ResetT{
		{
			Term:     "badterm",
			Window:   1000,
			Absolute: true,
		},
	}

	sm, err := NewInverseSet(int64(time.Second), []string{"frank", "burns"}, resets)
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

func BenchmarkSetInverseHitSequence(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSet(int64(time.Second), []string{"frank", "burns"}, nil)
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

func BenchmarkSetInverseHitOverlap(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSet(10, []string{"frank", "burns"}, nil)
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

func BenchmarkSetInverseRunawayMatch(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSet(1000000, []string{"frank", "burns"}, nil)
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
