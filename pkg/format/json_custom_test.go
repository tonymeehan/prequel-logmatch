package format

import (
	"strings"
	"testing"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/scanner"
)

func TestJsonCustomCorrupt(t *testing.T) {
	var (
		maxSz = 1024 * 1024
		sr    = scanner.NewStdReadScan(maxSz)
		rdr   = strings.NewReader(corrupted)
	)

	factory, err := NewJsonCustomFactory("$.time", time.RFC3339Nano)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	err = scanner.ScanForward(rdr, f.ReadEntry, sr.Scan, scanner.WithMaxSize(maxSz))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if len(sr.Logs) != 6 {
		t.Errorf("Expected %d entries, got %d", 6, len(sr.Logs))
	}

	if sr.Clip {
		t.Errorf("Expected no clip")
	}
}

func TestJsonCustomLogsExtraLF(t *testing.T) {

	var (
		maxSz = 1024 * 1024
		sr    = scanner.NewStdReadScan(maxSz)
		rdr   = strings.NewReader("\n\n\n" + corrupted + "\n\n\n" + extra + "\n\n")
	)

	factory, err := NewJsonCustomFactory("$.time", time.RFC3339Nano)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	err = scanner.ScanForward(rdr, f.ReadEntry, sr.Scan, scanner.WithMaxSize(maxSz))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if len(sr.Logs) != 8 {
		t.Errorf("Expected %d entries, got %d", 8, len(sr.Logs))
	}

	if sr.Clip {
		t.Errorf("Expected no clip")
	}
}

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

	if entry.Timestamp != 1326269700000000000 {
		t.Errorf("Expected %d got %d", 1326293700000000000, entry.Timestamp)
	}

	if entry.Line != string(line) {
		t.Errorf("Expected %s got %s", string(line), entry.Line)
	}
}
