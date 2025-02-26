package match

import (
	"testing"
	"time"
)

func fireEmptyHits(t *testing.T, dd *Dedupe, n int) {

	for i := 0; i < n; i++ {
		logs, hint := dd.MaybeFire(time.Now().UnixNano(), Hits{})
		if logs != nil {
			t.Fatalf("Expected nil, got %v", logs)
		}
		if hint != 0 {
			t.Fatalf("Expected 0, got %v", hint)
		}
	}
}

func testEqualLogs(t *testing.T, a, b []LogEntry) bool {

	if len(a) != len(b) {
		t.Errorf("Expected logs length %v, got %v", len(a), len(b))
		return false
	}
	for i := range a {
		if a[i].Line != b[i].Line || a[i].Timestamp != b[i].Timestamp {
			t.Errorf("Expected log entry %v, got %v", a[i], b[i])
			return false
		}
	}
	return true
}

func TestDedupeSimple(t *testing.T) {
	dd := NewDedupe(time.Second)

	// Empty hits
	fireEmptyHits(t, dd, 11)

	// First real hit
	hit1 := Hits{
		Cnt: 1,
		Logs: []LogEntry{
			{
				Line:      "Shrubbery",
				Timestamp: time.Now().UnixNano(),
			},
			{
				Line:      "Kaiser",
				Timestamp: time.Now().UnixNano(),
			},
		},
	}

	logs, hint := dd.MaybeFire(time.Now().UnixNano(), hit1)

	if hint != 0 {
		t.Errorf("Expected 0, got %v", hint)
	}

	testEqualLogs(t, logs, hit1.Logs)

	// Empty hits
	fireEmptyHits(t, dd, 11)

	// Fire a second hit
	hit2 := Hits{
		Cnt: 1,
		Logs: []LogEntry{
			{
				Line:      "George",
				Timestamp: time.Now().UnixNano(),
			},
			{
				Line:      "Ringo",
				Timestamp: time.Now().UnixNano(),
			},
		},
	}
	var saveHint time.Duration
	logs, saveHint = dd.MaybeFire(time.Now().UnixNano(), hit2)

	if saveHint == 0 {
		t.Errorf("Expected non-zero hint")
	}

	if logs != nil {
		t.Errorf("Expected logs marked pending")
	}

	// Fire a third hit, double tuple, the second should promote to pending
	hit3 := Hits{
		Cnt: 2,
		Logs: []LogEntry{
			{
				Line:      "Bob",
				Timestamp: time.Now().UnixNano(),
			},
			{
				Line:      "Carol",
				Timestamp: time.Now().UnixNano(),
			},
			{
				Line:      "Ted",
				Timestamp: time.Now().UnixNano(),
			},
			{
				Line:      "Alice",
				Timestamp: time.Now().UnixNano(),
			},
		},
	}

	logs, hint = dd.MaybeFire(time.Now().UnixNano(), hit3)

	if hint != 0 {
		t.Errorf("Expected zero hint on promote, got %v", hint)
	}

	if logs != nil {
		t.Errorf("Expected logs marked pending")
	}

	time.Sleep(saveHint)

	// Fire an empty request, should get the pending item
	logs, hint = dd.MaybeFire(time.Now().UnixNano(), Hits{})

	if hint != 0 {
		t.Errorf("Expected zero hint, got %v", hint)
	}

	// The final two logs should get fired.
	testEqualLogs(t, logs, hit3.Logs[2:])
}

func TestDedupePoll(t *testing.T) {
	dd := NewDedupe(time.Second)

	// Two hits, the second should be pending.
	hit := Hits{
		Cnt: 2,
		Logs: []LogEntry{
			{
				Line:      "Shrubbery",
				Timestamp: time.Now().UnixNano(),
			},
			{
				Line:      "Kaiser",
				Timestamp: time.Now().UnixNano() + int64(time.Millisecond)*500, // in the future so doesn't expire out on poll
			},
		},
	}

	logs, hint := dd.MaybeFire(time.Now().UnixNano(), hit)

	if hint == 0 {
		t.Errorf("Expected non-zero hint")
	}

	if !testEqualLogs(t, logs, hit.Logs[:1]) {
		t.Errorf("Fail pre-poll")
	}

	time.Sleep(hint)

	// check poll
	logs = dd.PollFire()

	if !testEqualLogs(t, logs, hit.Logs[1:]) {
		t.Errorf("Fail after poll")
	}

}

func BenchmarkDupeMisses(b *testing.B) {
	dd := NewDedupe(time.Second)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dd.MaybeFire(time.Now().UnixNano(), Hits{})
	}
}

func BenchmarkDupeHits(b *testing.B) {
	dd := NewDedupe(time.Millisecond)

	hit := Hits{
		Cnt: 1,
		Logs: []LogEntry{
			{
				Line:      "Shrubber",
				Timestamp: time.Now().UnixNano(),
			},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		hit.Logs[0].Timestamp = time.Now().UnixNano()
		dd.MaybeFire(time.Now().UnixNano(), hit)
	}
}
