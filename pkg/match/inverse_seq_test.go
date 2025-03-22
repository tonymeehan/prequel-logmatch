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

func TestSeqInverse(t *testing.T) {
	type step = stepT[InverseSeq]

	var tests = map[string]struct {
		clock  int64
		window int64
		terms  []string
		reset  []ResetT
		steps  []step
	}{
		"SingleTerm": {
			// -A---------------- alpha
			window: 10,
			terms:  []string{"alpha"},
			steps: []step{
				{line: "alpha", cb: matchStamps(1)},
			},
		},

		"SingleTermResetHit": {
			// -A---------------- alpha
			// ------------------ reset
			terms: []string{"alpha"},
			reset: []ResetT{
				{
					Window: 10,
					Term:   "reset",
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "NOOP", stamp: 10},                      // fire slightly early
				{line: "reset", stamp: 12, cb: matchStamps(1)}, // Fire reset late
			},
		},

		"SingleTermResetMiss": {
			// -A---------------- alpha
			// -----------B------ reset
			terms: []string{"alpha"},
			reset: []ResetT{
				{
					Window: 10,
					Term:   "reset",
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "reset", stamp: 11},
				{line: "NOOP", stamp: 1000},
			},
		},

		"SingleTermDupeTimestampReset": {
			//	-1---------------- alpha
			//	-2---------------- reset
			// An event with a dupe timestamp at the end of the reset window should not fire.
			window: 10,
			terms:  []string{"alpha"},
			reset: []ResetT{{
				Term: "reset",
			}}, // Simple relative reset
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "reset", stamp: 1},
			},
		},

		"DupeTimestamps": {
			// -A----------------
			// -B----------------
			// -C----------------
			// Dupe timestamps are tolerated.
			window: 5,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 1},
				{line: "gamma", stamp: 1, cb: matchStamps(1, 1, 1)},
			},
		},

		"DupeTimestampOnEndOfResetWindow": {
			//	-1---------------- alpha
			//	--2---------------- beta
			//	--3---------------- reset
			// An even with a dupe timestamp at the end of the reset window should not fire.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{{
				Term: "reset",
			}}, // Simple relative reset
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 2},
				{line: "reset", stamp: 2},
			},
		},

		"SimpleWindowMatchWithAbsoluteReset": {
			// --A----------
			// ----------B--
			// Fire B inside window, should delay until past window.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     "reset",
					Window:   50,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 1 + 10},                             // alpha stamp + window + 1
				{line: "NOOP", stamp: 1 + 50},                             // still in absolute reset window},
				{line: "NOOP", stamp: 1 + 50 + 1, cb: matchStamps(1, 11)}, // alpha stamp + window + reset window + 1
			},
		},

		"SimpleWindowMatchHitWithAbsoluteResetAndBigJump": {
			// --A----------
			// ---B--------
			// Fire B inside window, should delay until past reset window.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     "reset",
					Window:   50,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "NOOP", stamp: 10000, cb: matchStamps(1, 2)}, // way out of reset window
			},
		},

		"SimpleWindowMatchMissWithAbsoluteReset": {
			// --A----------
			// ----------B--
			// Fire B outside of window, should delay until past window.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     "reset",
					Window:   50,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 1 + 10 + 1}, // out of the window
				{line: "NOOP", stamp: 10000},      // way out of reset window
			},
		},

		"OverFire": {
			// --A--B--C-----
			// -----------D--
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []step{
				{line: "alpha"},
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta", cb: matchStamps(1, 4)},
			},
		},

		"SlideLeft": {
			// -------23----- alpha
			// ---------4---- beta
			// -1------------ reset
			// Fire a reset with a left shift,
			// should deny {2,4} on winow, but allow {3,4}
			window: 5,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     "reset",
					Slide:    -5,
					Window:   20,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "reset"},
				{line: "alpha", stamp: 6}, // reset window [1,21], no fire
				{line: "alpha"},           // reset window [2,22], should fire after 22
				{line: "beta"},
				{line: "noop", stamp: 22}, // no fire until outside reset window
				{line: "noop", cb: matchStamps(7, 8)},
			},
		},

		"SlideRight": {
			// ---12---------- alpha
			// -----3--------- beta
			// --------------4- reset
			// Fire a reset with a right shift,
			// should deny {2,4} on winow, but allow {3,4}
			window: 5,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     "reset",
					Slide:    5,
					Window:   20,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},            // reset window [6,26], no fire
				{line: "alpha", stamp: 22}, // reset window [27,47], should fire after 47
				{line: "reset"},            // reset ignored because abs window is slide right
				{line: "beta"},
				{line: "reset", stamp: 26}, // right edge of line 1 window
				{line: "noop", stamp: 47},  // right edge of line 2 window
				{line: "noop", cb: matchStamps(22, 24)},
				{line: "noop", stamp: 1000}, // way out of reset window, should not fire
			},
		},

		"RelativeResetWindowMiss": {
			// -A-------------
			// --B------------
			// ---C-----------
			// -------------R-
			// Setup a relative reset window, and assert reset at end of window.
			// Should not fire.
			window: 3,
			terms:  []string{"alpha", "beta", "gamma"},
			reset: []ResetT{
				{
					Term:   "reset",
					Window: 10,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "gamma"},                // should fire but delay on on reset window [1,13]
				{line: "noop", stamp: 13},      // Noop on edge of window, but can't fire until 13+1
				{line: "reset", stamp: 3 + 10}, // reset on right edge window
				{line: "noop", stamp: 1000},    // way out of reset window, should not fire
			},
		},

		"SlideAnchor": {
			// -1------------ alpha
			// ----2--------- beta
			window: 3,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     "reset",
					Window:   10,
					Absolute: true,
					Anchor:   1,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta", stamp: 1 + 3}, //  clock + window, reset window [4, 14]
				{line: "noop", stamp: 14},    // no fire until after window
				{line: "noop", cb: matchStamps(1, 4)},
			},
		},

		"AbsSlideResetContinue": {
			// -A-----------
			// ---B---------
			// ----C--DE----
			// --R----------
			// Anchor absolute reset window with neg slide on line 2.
			// Should disallow [A,B,C] and [A,B,D] but [A,B,E] should fire.
			window: 10,
			terms:  []string{"alpha", "beta", "gamma"},
			reset: []ResetT{
				{
					Term:     "reset",
					Window:   5,
					Absolute: true,
					Anchor:   2,
					Slide:    -5,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "reset"},
				{line: "beta"},
				{line: "gamma"},           // reset window [-1, 4], no fire
				{line: "gamma", stamp: 7}, // reset window [2, 7], no fire
				{line: "gamma", stamp: 8}, // reset window [3, 8], should fire on 9
				{line: "noop", stamp: 9, cb: matchStamps(1, 3, 8)},
			},
		},

		"Relative": {
			// -1-3---8-A--- alpha
			// --2--56-9-B-- beta
			// ----4-------- reset1
			// ----------C-- reset2
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: "reset1"},
				{Term: "reset2"},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta"}, // Should match, but cannot fire until next event due to reset
				{line: "alpha", cb: matchStamps(1, 2)},
				{line: "reset1"},
				{line: "beta"},
				{line: "beta"},
				{line: "noop"},
				{line: "alpha"},
				{line: "beta"}, // Should match, but cannot fire until next event due to reset
				{line: "alpha", cb: matchStamps(8, 9)},
				{line: "beta"},
				{line: "reset2", stamp: 11}, // same timestamp as 11, should deny [10,11]
				{line: "noop", stamp: 1000},
			},
		},

		"Absolute": {
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: "reset1"},
				{Term: "reset2"},
				{
					Term:     "reset3",
					Window:   10,
					Absolute: true,
				},
			},
			steps: []step{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sm, err := NewInverseSeq(tc.window, tc.terms, tc.reset)
			if err != nil {
				t.Fatalf("Expected err == nil, got %v", err)
			}

			clock := tc.clock

			for idx, step := range tc.steps {

				clock += 1
				stamp := clock
				if step.stamp != 0 {
					stamp = step.stamp
					clock = stamp
				}

				if step.line != "" {
					var (
						entry = entry.LogEntry{Timestamp: stamp, Line: step.line}
						hits  = sm.Scan(entry)
					)

					if step.cb == nil {
						checkNoFire(t, idx+1, hits)
					} else {
						step.cb(t, idx+1, hits)
					}
				}

				if step.postF != nil {
					step.postF(t, idx+1, sm)
				}
			}
		})
	}
}

// // Create a match with an inverse that has a long reset window.
// // Should still fire even if the last emitted time is way out of window.
// func TestSeqInverseSequenceManualEval(t *testing.T) {
// 	var (
// 		clock   int64 = 1
// 		sWindow int64 = 10
// 		rWindow int64 = 20
// 	)

// 	iq, err := NewInverseSeq(
// 		sWindow,
// 		[]string{"alpha", "beta"},
// 		[]ResetT{
// 			{
// 				Term:     "Shutdown initiated",
// 				Window:   rWindow,
// 				Absolute: true,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Fail constructor: %v", err)
// 	}

// 	// Emit valid sequence in order, should not fire until inverse timer
// 	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "alpha"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "beta"})
// 	testNoFire(t, hits)

// 	hits = iq.Eval(clock + 10000)

// 	if hits.Cnt != 1 {
// 		t.Fatalf("Expected 1 hits, got: %v", hits.Cnt)
// 	}

// 	if hits.Logs[0].Timestamp != clock+1 ||
// 		hits.Logs[1].Timestamp != clock+2 {
// 		t.Errorf("Expected 1,2 got: %v", hits)
// 	}
// }

// func TestSeqInverseAbsoluteHit(t *testing.T) {
// 	var (
// 		clock     = time.Now().UnixNano()
// 		window    = int64(time.Millisecond * 500)
// 		absWindow = int64(time.Second)
// 	)

// 	iq, err := NewInverseSeq(
// 		window,
// 		[]string{"alpha", "beta"},
// 		[]ResetT{
// 			{Term: "badterm1"},
// 			{Term: "badterm2"},
// 			{
// 				Term:     "badterm3",
// 				Absolute: true,
// 				Window:   absWindow,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Fail constructor: %v", err)
// 	}

// 	// Scan first matcher
// 	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
// 	hits := iq.Scan(ev1)
// 	testNoFire(t, hits)

// 	// Scan second matcher, exactly within the window.
// 	ev2 := LogEntry{Timestamp: clock + int64(window), Line: "Match beta."}
// 	hits = iq.Scan(ev2)

// 	// Should not hit  until the absolute window is up
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
// 	}

// 	// Assert log one nanosecond before the absolute window
// 	ev3 := LogEntry{Timestamp: clock + absWindow - 1, Line: "NOOP"}
// 	hits = iq.Scan(ev3)

// 	// Should not hit  until the absolute window is up
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
// 	}

// 	// Now assert log at exactly the absolute window, should fire
// 	ev4 := LogEntry{Timestamp: clock + absWindow, Line: "NOOP"}
// 	hits = iq.Scan(ev4)

// 	// Should not hit  until the absolute window is up
// 	if hits.Cnt != 1 {
// 		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
// 	}

// 	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
// 		t.Errorf("Fail logs equal")
// 	}
// }

// // Create an absolute window and fire an inverse into that window. Should drop.
// func TestSeqInverseAbsoluteMiss(t *testing.T) {
// 	var (
// 		clock     = time.Now().UnixNano()
// 		window    = int64(time.Millisecond * 500)
// 		absWindow = int64(time.Second)
// 	)

// 	iq, err := NewInverseSeq(
// 		window,
// 		[]string{"alpha", "beta"},
// 		[]ResetT{
// 			{Term: "badterm1"},
// 			{Term: "badterm2"},
// 			{
// 				Term:     "badterm3",
// 				Absolute: true,
// 				Window:   absWindow,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Fail constructor: %v", err)
// 	}

// 	// Scan first matcher
// 	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
// 	hits := iq.Scan(ev1)
// 	testNoFire(t, hits)

// 	// Scan second matcher, exactly within the window.
// 	ev2 := LogEntry{Timestamp: clock + int64(window), Line: "Match beta."}
// 	hits = iq.Scan(ev2)

// 	// Should not hit  until the absolute window is up
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
// 	}

// 	// Fire a negative term into the absolute window
// 	nv := LogEntry{Timestamp: clock + absWindow - 2, Line: "badterm3"}
// 	hits = iq.Scan(nv)

// 	// Should not hit  until the absolute window is up
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
// 	}

// 	// Assert log one nanosecond before the absolute window
// 	ev3 := LogEntry{Timestamp: clock + absWindow - 1, Line: "NOOP"}
// 	hits = iq.Scan(ev3)

// 	// Should not hit  until the absolute window is up
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
// 	}

// 	// Now assert log at exactly the absolute window, should fire
// 	ev4 := LogEntry{Timestamp: clock + absWindow, Line: "NOOP"}
// 	hits = iq.Scan(ev4)

// 	// Should not hit due to negative term
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
// 	}
// }

// func TestSeqInversePosRelativeOffset(t *testing.T) {
// 	var (
// 		clock     = time.Now().UnixNano()
// 		window    = int64(time.Millisecond * 500)
// 		relWindow = int64(time.Second)
// 	)

// 	iq, err := NewInverseSeq(
// 		window,
// 		[]string{"alpha", "beta"},
// 		[]ResetT{
// 			{Term: "badterm1"},
// 			{Term: "badterm2"},
// 			{
// 				Term:     "badterm3",
// 				Absolute: false,
// 				Window:   relWindow,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Fail constructor: %v", err)
// 	}

// 	// Scan first matcher
// 	ev1 := LogEntry{Timestamp: clock, Line: "Match alpha."}
// 	hits := iq.Scan(ev1)
// 	testNoFire(t, hits)

// 	// Scan second matcher, exactly within the window.
// 	ev2 := LogEntry{Timestamp: clock + int64(window), Line: "Match beta."}
// 	hits = iq.Scan(ev2)

// 	// Should not hit  until the relative window is up
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
// 	}

// 	// Assert log one nanosecond before the relative window
// 	relDeadline := ev2.Timestamp - ev1.Timestamp + relWindow
// 	ev3 := LogEntry{Timestamp: clock + relDeadline - 1, Line: "NOOP"}
// 	hits = iq.Scan(ev3)

// 	// Should not hit  until the relative window is up
// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected cnt 0, got: %v", hits.Cnt)
// 	}

// 	// Now assert log at exactly the relative window, should fire
// 	ev4 := LogEntry{Timestamp: clock + relDeadline, Line: "NOOP"}
// 	hits = iq.Scan(ev4)

// 	// Should not hit  until the absolute window is up
// 	if hits.Cnt != 1 {
// 		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
// 	}

// 	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
// 		t.Errorf("Fail logs equal")
// 	}
// }

// // -**********
// // --1----3--4-5-6-----
// // --1----3--4-5-6-----
// // --1----3--4 5-6-----
// // ----2-----------7--8

// // Because we are using a duplicate term, there is a possibility
// // of overlapping fire events.  This test should ensure that
// // the sequence matcher is able to handle this case.
// // Above should fire {1,3,4,7} and {3,4,5,8}
// func TestSeqInverseDupes(t *testing.T) {
// 	var (
// 		clock   int64 = 0
// 		sWindow int64 = 10
// 	)

// 	iq, err := NewInverseSeq(
// 		sWindow,
// 		[]string{
// 			"Discarding message",
// 			"Discarding message",
// 			"Discarding message",
// 			"Mnesia overloaded",
// 		},
// 		[]ResetT{},
// 	)
// 	if err != nil {
// 		t.Fatalf("Fail constructor: %v", err)
// 	}

// 	// Emit first row.
// 	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	// Emit last item, should not fire.
// 	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "Mnesia overloaded"})
// 	testNoFire(t, hits)

// 	// Emit first item 4 times; should not fire until "Mnesia overloaded again"
// 	hits = iq.Scan(LogEntry{Timestamp: clock + 3, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 4, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 5, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 6, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	// Emit last item, should fire once
// 	hits = iq.Scan(LogEntry{Timestamp: clock + 7, Line: "Mnesia overloaded"})

// 	if hits.Cnt != 1 {
// 		t.Errorf("Expected 1 hits, got: %v", hits.Cnt)
// 	}

// 	if hits.Logs[0].Timestamp != clock+1 ||
// 		hits.Logs[1].Timestamp != clock+3 ||
// 		hits.Logs[2].Timestamp != clock+4 ||
// 		hits.Logs[3].Timestamp != clock+7 {
// 		t.Fatalf("Expected 1,3,4,7 got: %v", hits)
// 	}

// 	// Should emit another
// 	hits = iq.Scan(LogEntry{Timestamp: clock + 8, Line: "Mnesia overloaded"})

// 	if hits.Cnt != 1 {
// 		t.Fatalf("Expected 1 hits, got: %v", hits.Cnt)
// 	}

// 	if hits.Logs[0].Timestamp != clock+3 ||
// 		hits.Logs[1].Timestamp != clock+4 ||
// 		hits.Logs[2].Timestamp != clock+5 ||
// 		hits.Logs[3].Timestamp != clock+8 {
// 		t.Errorf("Expected 3,4,5,8 got: %v", hits)
// 	}

// 	hits = iq.Eval(clock + sWindow*2)

// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
// 	}

// 	// Should fail out of window;
// 	// clock+6 is the last hot zero event in the window,
// 	// (if we were doing strict sequential, clock+4 would be the last hot event)
// 	// adding sWindow + 1 should be out of window.
// 	hits = iq.Scan(LogEntry{Timestamp: clock + 6 + sWindow + 1, Line: "Mnesia overloaded"})

// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
// 	}
// }

// // -*******************
// // -123---------
// // -123---------
// // -123---------
// // ----4----5---

// func TestSeqInverseDupesWithResetHit(t *testing.T) {
// 	var (
// 		clock   int64 = 0
// 		sWindow int64 = 10
// 		rWindow int64 = 20
// 	)

// 	iq, err := NewInverseSeq(
// 		sWindow,
// 		[]string{
// 			"Discarding message",
// 			"Discarding message",
// 			"Discarding message",
// 			"Mnesia overloaded",
// 		},
// 		[]ResetT{
// 			{
// 				Term:     "Shutdown initiated",
// 				Window:   rWindow,
// 				Absolute: true,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Fail constructor: %v", err)
// 	}

// 	// Emit valid sequence in order, should not fire until inverse timer
// 	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 3, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	// Emit last item, should not fire.
// 	hits = iq.Scan(LogEntry{Timestamp: clock + 4, Line: "Mnesia overloaded"})
// 	testNoFire(t, hits)

// 	// Emit extra right before window, should not fire
// 	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow, Line: "Mnesia overloaded"})
// 	testNoFire(t, hits)

// 	// Emit extra right at window, should  fire
// 	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow + 1, Line: "Mnesia overloaded"})

// 	if hits.Cnt != 1 {
// 		t.Fatalf("Expected 1 hits, got: %v", hits.Cnt)
// 	}

// 	if hits.Logs[0].Timestamp != clock+1 ||
// 		hits.Logs[1].Timestamp != clock+2 ||
// 		hits.Logs[2].Timestamp != clock+3 ||
// 		hits.Logs[3].Timestamp != clock+4 {
// 		t.Errorf("Expected 1,2,3,4 got: %v", hits)
// 	}

// 	// Emit way in the future, should not fire
// 	hits = iq.Eval(clock + sWindow*2)

// 	if hits.Cnt != 0 {
// 		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
// 	}
// }

// // -*******************
// // -123---------
// // -123---------
// // -123---------
// // ----4----5R--

// // Test that reset right at the end of the window prevents fire.

// func TestSeqInverseDupesWithResetFail(t *testing.T) {
// 	var (
// 		clock   int64 = 0
// 		sWindow int64 = 10
// 		rWindow int64 = 20
// 	)

// 	iq, err := NewInverseSeq(
// 		sWindow,
// 		[]string{
// 			"Discarding message",
// 			"Discarding message",
// 			"Discarding message",
// 			"Mnesia overloaded",
// 		},
// 		[]ResetT{
// 			{
// 				Term:     "Shutdown initiated",
// 				Window:   rWindow,
// 				Absolute: true,
// 			},
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Fail constructor: %v", err)
// 	}

// 	// Emit valid sequence in order, should not fire until inverse timer
// 	hits := iq.Scan(LogEntry{Timestamp: clock + 1, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 2, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	hits = iq.Scan(LogEntry{Timestamp: clock + 3, Line: "Discarding message"})
// 	testNoFire(t, hits)

// 	// Emit last item, should not fire.
// 	hits = iq.Scan(LogEntry{Timestamp: clock + 4, Line: "Mnesia overloaded"})
// 	testNoFire(t, hits)

// 	// Emit extra right before window, should not fire
// 	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow, Line: "Mnesia overloaded"})
// 	testNoFire(t, hits)

// 	// Emit reset on edge of window, should not fire
// 	hits = iq.Scan(LogEntry{Timestamp: clock + rWindow + 1, Line: "Shutdown initiated"})
// 	testNoFire(t, hits)

// 	// Fire in the future, should get nothing
// 	hits = iq.Eval(clock + 1000)
// 	testNoFire(t, hits)
// }

// //*******
// // -1------4--------------10----------
// // ---2--3----------8---9-----11----
// // ----------5--6-7---------------12-
// // Should fire {1,2,5}, {4,8,12}

// func TestSeqInverseGCOldSecondaryTerms(t *testing.T) {
// 	var (
// 		clock  int64 = 0
// 		window int64 = 50
// 	)

// 	sm, err := NewInverseSeq(window, []string{"alpha", "beta", "gamma"}, []ResetT{})
// 	if err != nil {
// 		t.Fatalf("Expected err == nil, got %v", err)
// 	}

// 	clock += 1
// 	ev1 := LogEntry{Timestamp: clock, Line: "alpha"}
// 	hits := sm.Scan(ev1)
// 	testNoFire(t, hits)

// 	clock += 1
// 	ev2 := LogEntry{Timestamp: clock, Line: "beta"}
// 	hits = sm.Scan(ev2)
// 	testNoFire(t, hits)

// 	clock += 1
// 	ev3 := LogEntry{Timestamp: clock, Line: "beta"}
// 	hits = sm.Scan(ev3)
// 	testNoFire(t, hits)

// 	clock += 1
// 	ev4 := LogEntry{Timestamp: clock, Line: "alpha"}
// 	hits = sm.Scan(ev4)
// 	testNoFire(t, hits)

// 	clock += 1
// 	ev5 := LogEntry{Timestamp: clock, Line: "gamma"}
// 	hits = sm.Scan(ev5)

// 	if hits.Cnt != 1 {
// 		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
// 	}

// 	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2, ev5}) {
// 		t.Errorf("Fail log match")
// 	}

// 	clock += 1
// 	ev6 := LogEntry{Timestamp: clock, Line: "gamma"}
// 	hits = sm.Scan(ev6)
// 	testNoFire(t, hits)

// 	clock += 1
// 	ev7 := LogEntry{Timestamp: clock, Line: "gamma"}
// 	hits = sm.Scan(ev7)
// 	testNoFire(t, hits)

// 	clock += 1
// 	ev8 := LogEntry{Timestamp: clock, Line: "beta"}
// 	hits = sm.Scan(ev8)
// 	testNoFire(t, hits)

// 	clock += 1
// 	ev9 := LogEntry{Timestamp: clock, Line: "beta"}
// 	hits = sm.Scan(ev9)

// 	clock += 1
// 	ev10 := LogEntry{Timestamp: clock, Line: "alpha"}
// 	hits = sm.Scan(ev10)

// 	clock += 1
// 	ev11 := LogEntry{Timestamp: clock, Line: "beta"}
// 	hits = sm.Scan(ev11)

// 	clock += 1
// 	ev12 := LogEntry{Timestamp: clock, Line: "gamma"}
// 	hits = sm.Scan(ev12)

// 	if hits.Cnt != 1 {
// 		t.Fatalf("Expected hits.Cnt == 1, got %v", hits.Cnt)
// 	}

// 	if !testEqualLogs(t, hits.Logs, []LogEntry{ev4, ev8, ev12}) {
// 		t.Errorf("Fail log match")
// 	}

// 	sm.GarbageCollect(clock + window + 1)

// 	if sm.nActive != 0 {
// 		t.Errorf("Expected empty state")
// 	}

// }

// // Reset terms should be dropped if no matches and no reset lookback.
// func TestSeqInverseResetsIgnoredOnNoMatch(t *testing.T) {

// 	var (
// 		N           = 11
// 		clock int64 = 0
// 	)

// 	// Create a seq matcher with a negative window reset term
// 	sm, err := NewInverseSeq(10, []string{"alpha", "beta", "gamma"}, []ResetT{
// 		{
// 			Term: "badterm",
// 		},
// 	})
// 	if err != nil {
// 		t.Fatalf("Expected err == nil, got %v", err)
// 	}

// 	// Fire the bad term N times
// 	for range N {
// 		clock += 1
// 		hits := sm.Scan(LogEntry{Timestamp: clock, Line: "badterm"})
// 		testNoFire(t, hits)
// 	}

// 	// Should have zero resets
// 	if len(sm.resets[0].resets) != 0 {
// 		t.Fatalf("Expected 0 negative terms, got %v", len(sm.resets[0].resets))
// 	}
// }

// func TestSeqInverseNegativesAreGCed(t *testing.T) {

// 	var (
// 		N             = 3
// 		clock   int64 = 0
// 		sWindow int64 = 50
// 		rWindow int64 = 20
// 		rSlide  int64 = -10
// 	)

// 	// Create a seq matcher with a negative window reset term
// 	sm, err := NewInverseSeq(sWindow, []string{"alpha", "beta", "gamma"}, []ResetT{
// 		{
// 			Term:   "badterm",
// 			Slide:  rSlide,
// 			Window: rWindow,
// 		},
// 	})
// 	if err != nil {
// 		t.Fatalf("Expected err == nil, got %v", err)
// 	}

// 	// Fire the bad term N times
// 	for range N {
// 		hits := sm.Scan(LogEntry{Timestamp: clock, Line: "badterm"})
// 		testNoFire(t, hits)
// 		clock += 1
// 	}

// 	// Negative terms with nothing hot w/o lookback have been optimized out.
// 	if len(sm.resets[0].resets) != 3 {
// 		t.Fatalf("Expected 3 negative terms, got %v", len(sm.resets[0].resets))
// 	}

// 	// Emit noop at full GC window (see calcGCWindow)
// 	gcWindow := sWindow + rWindow + rSlide
// 	if rSlide < 0 {
// 		gcWindow += -rSlide
// 	}
// 	hits := sm.Scan(entry.LogEntry{Timestamp: gcWindow, Line: "NOOP"})
// 	testNoFire(t, hits)

// 	// We should have some negative terms
// 	if len(sm.resets[0].resets) != 3 {
// 		t.Fatalf("Expected 3 negative terms, got %v", len(sm.resets[0].resets))
// 	}

// 	// Emit noop right after window
// 	hits = sm.Scan(entry.LogEntry{Timestamp: gcWindow + 1, Line: "NOOP"})
// 	testNoFire(t, hits)

// 	// Should have peeled off one term
// 	if len(sm.resets[0].resets) != 2 {
// 		t.Fatalf("Expected 2 negative terms, got %v", len(sm.resets[0].resets))
// 	}

// 	// Emit noop right after window +2
// 	hits = sm.Scan(entry.LogEntry{Timestamp: gcWindow + 2, Line: "NOOP"})
// 	testNoFire(t, hits)

// 	// Should have peeled off one term
// 	if len(sm.resets[0].resets) != 1 {
// 		t.Fatalf("Expected 1 negative terms, got %v", len(sm.resets[0].resets))
// 	}

// 	// Emit noop right after window +3
// 	hits = sm.Scan(entry.LogEntry{Timestamp: gcWindow + 3, Line: "NOOP"})
// 	testNoFire(t, hits)

// 	// Should have peeled off the last term
// 	if len(sm.resets[0].resets) != 0 {
// 		t.Fatalf("Expected 0 negative terms, got %v", len(sm.resets[0].resets))
// 	}

// 	// GC should be disabled
// 	if sm.gcMark != disableGC {
// 		t.Errorf("Expected GC to be disabled, got :%v", sm.gcMark)
// 	}
// }

// // Ignore events fired out of order
// func TestSeqInverseTimestampOutofOrder(t *testing.T) {
// 	var (
// 		clock  int64 = 1
// 		window int64 = 10
// 	)

// 	sm, err := NewInverseSeq(window, []string{"alpha", "gamma"}, nil)
// 	if err != nil {
// 		t.Fatalf("Expected err == nil, got %v", err)
// 	}

// 	// Set up partial match, should not fire
// 	ev1 := LogEntry{Timestamp: clock, Line: "alpha"}
// 	hits := sm.Scan(ev1)
// 	testNoFire(t, hits)

// 	// Fire second matcher at same time; should fire
// 	// since we are not enforcing strict ordering.
// 	ev2 := LogEntry{Timestamp: clock - 1, Line: "gamma"}
// 	hits = sm.Scan(ev2)
// 	testNoFire(t, hits)
// }

// // Fire events on same timestamp, should match
// // as we are currently not enforcing strict ordering.

// func TestSeqInverseDupeTimestamps(t *testing.T) {
// 	var (
// 		clock  int64 = 1
// 		window int64 = 10
// 	)

// 	sm, err := NewInverseSeq(window, []string{"alpha", "gamma"}, nil)
// 	if err != nil {
// 		t.Fatalf("Expected err == nil, got %v", err)
// 	}

// 	// Set up partial match, should not fire
// 	ev1 := LogEntry{Timestamp: clock, Line: "alpha"}
// 	hits := sm.Scan(ev1)
// 	testNoFire(t, hits)

// 	// Fire second matcher at same time; should fire
// 	// since we are not enforcing strict ordering.
// 	ev2 := LogEntry{Timestamp: clock, Line: "gamma"}
// 	hits = sm.Scan(ev2)

// 	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
// 		t.Errorf("Fail log match")
// 	}
// }

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
