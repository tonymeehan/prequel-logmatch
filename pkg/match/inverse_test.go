package match

import (
	"testing"
	"time"
)

func TestInverseSequenceSlideLeft(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = time.Millisecond * 500
		slide     = int64(-1 * time.Second)
		absWindow = int64(time.Second)
	)
	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]InverseTerm{
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

func TestInverseSequenceSlideRight(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = time.Millisecond * 500
		slide     = int64(time.Second)
		absWindow = int64(time.Second)
	)
	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]InverseTerm{
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

func TestInverseSequenceSlideAnchor(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = time.Millisecond * 500
		absWindow = int64(time.Minute)
	)
	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]InverseTerm{
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

func TestInverseSequenceRelative(t *testing.T) {
	window := time.Millisecond * 500
	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]InverseTerm{
			{Term: "badterm1"},
			{Term: "badterm2"},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Throw in some NOOPS just for fun
	fireNoops(t, iq, 100)

	// Scan second matcher
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match beta."}
	hits = iq.Scan(ev2)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail logs equal")
	}

	// Now fire another start event; nothing should fire
	ev3 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha part deux."}
	hits = iq.Scan(ev3)
	testNoFire(t, hits)

	// Fire a inverse event
	ev4 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "This is badterm1"}
	hits = iq.Scan(ev4)
	testNoFire(t, hits)

	// Fire end event, it should not fire due to inverse above
	ev5 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "beta my friend"}
	hits = iq.Scan(ev5)
	testNoFire(t, hits)

	// Similarly this should fail because start was removed from seq matcher
	ev6 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "beta my friend"}
	hits = iq.Scan(ev6)
	testNoFire(t, hits)

	// Fire a inverse event
	ev7 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "This is badterm7"}
	hits = iq.Scan(ev7)
	testNoFire(t, hits)

	// Fire alpha then beta, should fire because inverse was before alpha
	ev8 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha part trois."}
	hits = iq.Scan(ev8)
	testNoFire(t, hits)

	ev9 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "beta again"}
	hits = iq.Scan(ev9)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev8, ev9}) {
		t.Errorf("Fail logs equal")
	}

	// Force gc
	iq.garbageCollect(time.Now().UnixNano())

	// GC should be clean
	if iq.nTerms != 0 {
		t.Errorf("Expected 0 terms, got: %v", iq.nTerms)
	}
}

func TestInverseSequenceAbsoluteHit(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = time.Millisecond * 500
		absWindow = int64(time.Second)
	)

	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]InverseTerm{
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
func TestInverseSequenceAbsoluteMiss(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = time.Millisecond * 500
		absWindow = int64(time.Second)
	)

	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]InverseTerm{
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

func TestInverseSequencePosRelativeOffset(t *testing.T) {
	var (
		clock     = time.Now().UnixNano()
		window    = time.Millisecond * 500
		relWindow = int64(time.Second)
	)

	iq, err := NewInverseSeq(
		window,
		[]string{"alpha", "beta"},
		[]InverseTerm{
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

// Tony Meehan
// 12:56 PM
// i'm thinking for now, we can focus on just a few rules:
// I want to send a detection when I see 10 or more "Discarding message"
//   logs followed by 1 or more "Mnesia overloaded" logs in a container
//   named "rabbitmq" when there is also no "Shutdown initiated" log (in any order) in the same container -- all within 10s"

var baseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

var replayTonyOK = []LogEntry{
	{
		Timestamp: baseTime.UnixNano(),
		Line:      "Rabbit starts",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 1).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 2).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 3).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 4).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 5).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 6).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 7).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 8).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 9).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 10).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 11).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 12).UnixNano(),
		Line:      "Discarding message",
	},
	{
		Timestamp: baseTime.Add(time.Millisecond * 13).UnixNano(),
		Line:      "Mnesia overloaded",
	},
	{
		Timestamp: baseTime.Add(time.Second * 9).UnixNano(),
		Line:      "Mnesia overloaded",
	},
}

func TestTonySequence(t *testing.T) {
	window := time.Second * 10
	iq, err := NewInverseSeq(
		window,
		[]string{
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Mnesia overloaded",
		},
		[]InverseTerm{
			{
				Term:     "Shutdown initiated",
				Window:   int64(window),
				Absolute: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan our events, should not fire until timer us up.
	for _, ev := range replayTonyOK {
		hits := iq.Scan(ev)
		testNoFire(t, hits)
	}

	// Fire a noop event at exactly the first window
	ev := LogEntry{Timestamp: replayTonyOK[1].Timestamp + int64(window)}
	hits := iq.Scan(ev)

	if hits.Cnt != 1 {
		t.Errorf("Expected 3 hits, got: %v", hits.Cnt)
	}

	// Fire a noop event at exactly the second window
	ev = LogEntry{Timestamp: replayTonyOK[2].Timestamp + int64(window)}
	hits = iq.Scan(ev)

	if hits.Cnt != 1 {
		t.Errorf("Expected 3 hits, got: %v", hits.Cnt)
	}

	// Fire a noop event at exactly the third window
	ev = LogEntry{Timestamp: replayTonyOK[3].Timestamp + int64(window)}
	hits = iq.Scan(ev)

	if hits.Cnt != 1 {
		t.Errorf("Expected 3 hits, got: %v", hits.Cnt)
	}

	// Fire hour into the future, should get nothing
	ev = LogEntry{Timestamp: replayTonyOK[3].Timestamp + int64(time.Hour)}
	hits = iq.Scan(ev)

	if hits.Cnt != 0 {
		t.Errorf("Expected 0 hits, got: %v", hits.Cnt)
	}
}

func TestTonySequenceFail(t *testing.T) {
	window := time.Second * 10
	iq, err := NewInverseSeq(
		window,
		[]string{
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Discarding message",
			"Mnesia overloaded",
		},
		[]InverseTerm{
			{
				Term:     "Shutdown initiated",
				Window:   int64(window),
				Absolute: true,
			},
		},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan our events, should not fire until timer us up.
	for _, ev := range replayTonyOK {
		hits := iq.Scan(ev)
		testNoFire(t, hits)
	}

	// an inverse event, should wipe the whole thing
	ev1 := LogEntry{Timestamp: replayTonyOK[len(replayTonyOK)-1].Timestamp + 1, Line: "Shutdown initiated"}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// fire a noop event, past the window (could also run poll)
	// Should not fire due to inverse condition
	ev2 := LogEntry{Timestamp: replayTonyOK[0].Timestamp + int64(window)}
	hits = iq.Scan(ev2)
	testNoFire(t, hits)
}
