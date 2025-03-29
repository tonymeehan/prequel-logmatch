package scanner

import (
	"bufio"
	"io"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

func ScanForward(rdr io.Reader, parseF ParseFuncT, scanF ScanFuncT, opts ...ScanOptT) error {

	var (
		buf     []byte
		o       = parseOpts(opts)
		scanner = bufio.NewScanner(rdr)
	)

	if o.maxSz == MaxRecordSize {
		ptr := pool.PoolAlloc()
		defer pool.PoolFree(ptr)
		buf = *ptr
	} else {
		buf = make([]byte, o.maxSz)
	}

	scanF, errF, flushF := bindCallbacks(scanF, o)

	// Scanner will bail with bufio.ErrTooLong
	// if it encounters a line that is > o.maxSz.
	scanner.Buffer(buf, o.maxSz)

LOOP:
	for scanner.Scan() {

		entry, parseErr := parseF(scanner.Bytes())
		if parseErr != nil {
			if err := errF(scanner.Bytes(), parseErr); err != nil {
				return err
			}
			continue
		}

		if entry.Timestamp > o.stop {
			break LOOP
		}

		if scanF(entry) {
			break LOOP
		}
	}

	if flushF != nil {
		flushF()
	}

	return scanner.Err()
}

type flushFuncT func()

func bindCallbacks(scanF ScanFuncT, o scanOpt) (ScanFuncT, ErrFuncT, flushFuncT) {
	if !o.fold {
		return scanF, o.errF, nil
	}

	var pending LogEntry

	nScanF := func(entry LogEntry) bool {
		// Cache on first spin
		if pending.Timestamp == 0 {
			pending = entry
			return false
		}

		done := scanF(pending)
		pending = entry
		return done
	}

	nErrF := func(line []byte, err error) error {
		pending.Line += string(line)
		return o.errF(line, err)
	}

	flushF := func() {
		if pending.Timestamp != 0 {
			scanF(pending)
		}
	}

	return nScanF, nErrF, flushF
}
