package format

import (
	"testing"
	"time"
)

func TestJsonCustomFunkyField(t *testing.T) {

	factory, err := NewJsonCustomFactory("$.childof.funky", time.RFC822)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	line := []byte(`{"childof": {"funky": "11 Jan 12 08:15 EST"}}`)

	entry, err := f.ReadEntry(line)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if entry.Timestamp != 1326287700000000000 {
		t.Errorf("Expected %d got %d", 1326287700000000000, entry.Timestamp)
	}

	if entry.Line != string(line) {
		t.Errorf("Expected %s got %s", string(line), entry.Line)
	}
}
