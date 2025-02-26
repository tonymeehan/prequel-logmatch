package format

import (
	"bufio"
	"math"
	"strings"
	"testing"

	"github.com/prequel-dev/prequel-logmatch/pkg/scanner"
)

// Actual corrupted data
const corrupted = `
{"log":"[INFO] 10.244.0.201:40566 - 10187 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000460728s\n","stream":"stdout","time":"2024-02-13T15:12:44.355965799Z"}
{"log":"[INFO] 10.244.0.201:56774 - 17984 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000293089s\n","stream":"stdout","time":"2024-02-13T15:12:44.380212573Z"}
{"log":"[INFO] 10.244.0.201:51114 - 31602 \"AAAA IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000114441s\n","stream":"stdout","time":"2024-02-13T15:24:09.841391183Z"}
{"log":"[INFO] 10.244.0.201:43521 - 42646 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000128607s\n"{"log":"[INFO] 10.244.0.201:33715 - 27599 \"AAAA IN prequel-collector-service.pre
quel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000115177s\n","stream":"stdout","time":"2024-02-13T15:24:09.856166904Z"}
{"log":"[INFO] 10.244.0.201:33686 - 15589 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.00014057s\n","stream":"stdout","time":"2024-02-13T15:24:09.856823439Z"}
{"log":"[INFO] 10.244.0.201:37826 - 38610 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000075439s\n","stream":"stdout","time":"2024-02-13T15:24:09.857799908Z"}
{"log":"[INFO] 10.244.0.201:52267 - 22828 \"AAAA IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000566709s\n","stream":"stdout","time":"2024-02-13T15:24:09.858265371Z"}
`

const extra = `
{"log":"[INFO] 10.244.0.201:37826 - 38610 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000075439s\n","stream":"stdout","time":"2024-02-13T15:25:09.857799908Z"}
{"log":"[INFO] 10.244.0.201:52267 - 22828 \"AAAA IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000566709s\n","stream":"stdout","time":"2024-02-13T15:26:09.858265371Z"}
`

func TestJsonLogsCorrupt(t *testing.T) {
	var (
		f     = jsonFmtT{}
		maxSz = 1024 * 1024
		sr    = scanner.NewStdReadScan(maxSz)
		rdr   = strings.NewReader(corrupted)
	)

	err := f.ScanForward(rdr, maxSz, math.MaxInt64, sr.Scan)

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

func TestJsonLogsExtraLF(t *testing.T) {

	var (
		f     = jsonFmtT{}
		maxSz = 1024 * 1024
		sr    = scanner.NewStdReadScan(maxSz)
		rdr   = strings.NewReader("\n\n\n" + corrupted + "\n\n\n" + extra + "\n\n")
	)

	err := f.ScanForward(rdr, maxSz, math.MaxInt64, sr.Scan)

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

func TestJsonLogsMaxSize(t *testing.T) {

	tests := map[string]struct {
		maxSz       int
		stop        int64
		wantErr     error
		wantLen     int
		wantUsed    int
		wantClipped bool
	}{
		"too small":         {maxSz: 10, wantClipped: false, wantErr: bufio.ErrTooLong},
		"one_less_output":   {maxSz: 177, wantClipped: true},
		"exact_output":      {maxSz: 178, wantLen: 1, wantUsed: 178, wantClipped: true},
		"one_less++":        {maxSz: 178 + 177, wantLen: 1, wantUsed: 178, wantClipped: true},
		"exact++":           {maxSz: 178 + 178, wantLen: 2, wantUsed: 178 + 178, wantClipped: true},
		"exact++_with_stop": {maxSz: 178 + 178, wantLen: 2, wantUsed: 178 + 178, wantClipped: false, stop: 1707837164380212573},
		"no_clip_on_EOF":    {maxSz: 1024 * 1024, wantLen: 6, wantUsed: 1073, wantClipped: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f := jsonFmtT{}

			stop := tc.stop
			if stop == 0 {
				stop = math.MaxInt64
			}

			sr := scanner.NewStdReadScan(tc.maxSz)

			rdr := strings.NewReader(corrupted)
			err := f.ScanForward(rdr, tc.maxSz, stop, sr.Scan)

			if err != tc.wantErr {
				t.Errorf("Expected %v error got %v", tc.wantErr, err)
			}

			if len(sr.Logs) != tc.wantLen {
				t.Errorf("Expected %d entries, got %d", tc.wantLen, len(sr.Logs))
			}

			if sr.Clip != tc.wantClipped {
				t.Errorf("Expected %v clip, got %v", tc.wantClipped, sr.Clip)
			}

			if sr.Sz != tc.wantUsed {
				t.Errorf("Expected %v BufUsed, got %v", tc.wantUsed, sr.Sz)
			}
		})
	}
}
