package format

import (
	"bytes"
	"testing"
	"time"
)

func TestRegex(t *testing.T) {

	exp := `^\b[A-Z][a-z]{2} [A-Z][a-z]{2} [ _]?\d{1,2} \d{2}:\d{2}:\d{2} \d{4}\b`
	factory, err := NewRegexFactory(exp, time.ANSIC)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	line := []byte(`Mon Jan  9 15:04:05 2020 Funky log line indeed.`)

	entry, err := f.ReadEntry(line)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if entry.Timestamp != 1578582245000000000 {
		t.Errorf("Expected %d got %d", 1578582245000000000, entry.Timestamp)
	}

	if entry.Line != string(line) {
		t.Errorf("Expected %s got %s", string(line), entry.Line)
	}
}

func TestRegexReadTimestamp(t *testing.T) {

	exp := `^(\d{2}) ([A-Za-z]{3}) (\d{2}) (\d{2}:\d{2}) ([+-]\d{4})`
	factory, err := NewRegexFactory(exp, time.RFC822Z)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	line := []byte(`10 Jan 12 15:04 -0700 Testy stamp.`)
	ts, err := f.ReadTimestamp(bytes.NewReader(line))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if ts != 1326233040000000000 {
		t.Errorf("Expected %d got %d", 1326233040000000000, ts)
	}
}
