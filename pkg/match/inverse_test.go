package match

import (
	"testing"
	"time"
)

func TestInverseAloneRealtime(t *testing.T) {

	window := time.Millisecond * 500
	ia, err := NewInverseAlone(window, "alpha", "beta")
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Non-matching event should put matcher in active state
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "clean"}
	hits := ia.Scan(ev1)
	testNoFire(t, hits)

	// Matcher will not hit until edge condition; fire some non-matchers
	fireNoops(t, ia, 11)

	// Wait until timer first.
	time.Sleep(window)

	// Scan clean event, should fire active
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "clean"}
	hits = ia.Scan(ev2)

	if hits.Cnt != 1 {
		t.Errorf("Expected 1 cnt: got %v", hits.Cnt)
	}

	// Log fire should be empty entry with timestamp on the activating timestamp
	if !testEqualLogs(t, hits.Logs, []LogEntry{{Timestamp: ev1.Timestamp}}) {
		t.Errorf("Fail equal logs")
	}

	// Should not fire again until next timeout
LOOP:
	for {
		now := time.Now().UnixNano()
		ev := LogEntry{Timestamp: now, Line: "clean"}
		hits = ia.Scan(ev)

		if now-ev2.Timestamp >= int64(window) {
			if hits.Cnt != 1 {
				t.Errorf("Expected 1 cnt: got %v", hits.Cnt)
			}

			// Log fire should be empty entry with timestamp on the activating timestamp
			if !testEqualLogs(t, hits.Logs, []LogEntry{{Timestamp: ev2.Timestamp}}) {
				t.Errorf("Fail equal logs")
			}
			break LOOP
		} else {
			testNoFire(t, hits)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestInverseSequenceRealtime(t *testing.T) {
	window := time.Millisecond * 500
	iq, err := NewInverseSeq(
		window,
		[]string{"badterm1", "badterm2"},
		[]string{"alpha", "beta"},
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan first matcher, should start the active timer.
	ev1 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha."}
	hits := iq.Scan(ev1)
	testNoFire(t, hits)

	// Scan second matcher, should not fire yet cause time hasn't hit.
	ev2 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match beta."}
	hits = iq.Scan(ev2)
	testNoFire(t, hits)

	// Throw in some NOOPS just for fun
	fireNoops(t, iq, 100)

	// Wait for the window to expire
	time.Sleep(window)

	// Now fire another start event; this should fire the original events as we are now activated.
	ev3 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha part deux."}
	hits = iq.Scan(ev3)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev1, ev2}) {
		t.Errorf("Fail logs equal")
	}

	// We are now in an active state, if we scan a 'beta' match it should fire immediately.
	ev4 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match beta part deux."}
	hits = iq.Scan(ev4)

	if hits.Cnt != 1 {
		t.Errorf("Expected cnt 1, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs, []LogEntry{ev3, ev4}) {
		t.Errorf("Fail logs equal")
	}

	// Match alpha again, but we are going to hit the inverse condition on next event and negate
	ev5 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha part trois."}
	hits = iq.Scan(ev5)
	testNoFire(t, hits)

	// Match inverse condition
	ev6 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "This is badterm1"}
	hits = iq.Scan(ev6)
	testNoFire(t, hits)

	// A beta scan should not emit now, and in fact should be ignore.
	ev7 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match beta part trois."}
	hits = iq.Scan(ev7)
	testNoFire(t, hits)

	// The timer restarted on ev7; scan alpha then beta again.
	// Match should not fire until timer expires.
	ev8 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha part quatre."}
	hits = iq.Scan(ev8)
	testNoFire(t, hits)

	// Should cause overlap fires on beta after timeout.
	ev9 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match alpha part cinq."}
	hits = iq.Scan(ev9)
	testNoFire(t, hits)

	// A beta scan should not emit now.
	ev10 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Match beta part quatre."}
	hits = iq.Scan(ev10)
	testNoFire(t, hits)

	// Wait for window timeout
	time.Sleep(window)

	// Fire a innocuous non matching event
	ev11 := LogEntry{Timestamp: time.Now().UnixNano(), Line: "Innocuous"}
	hits = iq.Scan(ev11)

	// Expect the two overlapping pending events fired
	if hits.Cnt != 2 {
		t.Errorf("Expected cnt 2, got: %v", hits.Cnt)
	}

	if !testEqualLogs(t, hits.Logs[:2], []LogEntry{ev8, ev10}) {
		t.Errorf("Fail logs equal")
	}

	if !testEqualLogs(t, hits.Logs[2:], []LogEntry{ev9, ev10}) {
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
		[]string{"Shutdown initiated"},
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
	)
	if err != nil {
		t.Fatalf("Fail constructor: %v", err)
	}

	// Scan our events, should not fire until timer us up.
	for _, ev := range replayTonyOK {
		hits := iq.Scan(ev)
		testNoFire(t, hits)
	}

	// fire a noop event, past the window (could also run poll)
	ev := LogEntry{Timestamp: replayTonyOK[0].Timestamp + int64(window)}
	hits := iq.Scan(ev)

	if hits.Cnt != 3 {
		t.Errorf("Expected 3 hits, got: %v", hits.Cnt)
	}
}

func TestTonySequence2(t *testing.T) {
	window := time.Second * 10
	iq, err := NewInverseSeq(
		window,
		[]string{"Shutdown initiated"},
		[]string{
			"Discarding message",
			"Mnesia overloaded",
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

	// fire a noop event, past the window (could also run poll)
	ev := LogEntry{Timestamp: replayTonyOK[0].Timestamp + int64(window)}
	hits := iq.Scan(ev)

	if hits.Cnt != 12 {
		t.Errorf("Expected 3 hits, got: %v", hits.Cnt)
	}
}

func TestTonySequenceFail(t *testing.T) {
	window := time.Second * 10
	iq, err := NewInverseSeq(
		window,
		[]string{"Shutdown initiated"},
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
