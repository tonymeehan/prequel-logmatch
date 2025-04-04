package scanner

import (
	"errors"
	"strings"

	"github.com/prequel-dev/prequel-logmatch/pkg/format"
)

func bindFold(scanF ScanFuncT, errF ErrFuncT) (ScanFuncT, ErrFuncT, flushFuncT) {

	var (
		pending LogEntry
		builder strings.Builder
	)

	nScanF := func(entry LogEntry) (done bool) {
		// Cache on first spin
		if pending.Timestamp == 0 {
			pending = entry
			return
		}

		if builder.Len() > 0 {
			pending.Line = builder.String()
			builder.Reset()
		}

		// Scan pending entry
		switch done = scanF(pending); done {
		case true:
			// Scan done; avoid emit on flush
			pending.Timestamp = 0
		default:
			// Promote current entry to pending
			pending = entry
		}

		return
	}

	// On error, append line to pending entry
	nErrF := func(line []byte, err error) error {
		if errors.Is(err, format.ErrMatchTimestamp) {
			if builder.Len() == 0 {
				builder.WriteString(pending.Line)
			}
			builder.Write(line)
		}
		return errF(line, err)
	}

	// On final flush, emit pending entry if exists
	nFlushF := func() (done bool) {
		if pending.Timestamp != 0 {
			done = scanF(pending)
		}
		return
	}

	return nScanF, nErrF, nFlushF
}
