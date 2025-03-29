package scanner

import (
	"math"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
	"github.com/prequel-dev/prequel-logmatch/pkg/entry"

	"github.com/rs/zerolog/log"
)

type LogEntry = entry.LogEntry

const (
	MaxRecordSize = pool.MaxRecordSize
	pageSize      = 4096
)

type ParseFuncT func([]byte) (LogEntry, error)
type ScanFuncT func(entry LogEntry) bool
type ErrFuncT func([]byte, error) error
type ScanOptT func(*scanOpt)

type scanOpt struct {
	maxSz int
	fold  bool
	start int64
	stop  int64
	mark  int64
	errF  ErrFuncT
}

func defaultErrFunc(line []byte, err error) error {
	// Tolerate badly formed lines
	log.Error().
		Err(err).
		Str("line", string(line)).
		Msg("Fail parse.  Continue...")
	return nil
}

func parseOpts(opts []ScanOptT) scanOpt {
	o := scanOpt{
		maxSz: MaxRecordSize,
		errF:  defaultErrFunc,
		stop:  math.MaxInt64, // Default to scan to end of file; fixup for reverse scan since less common
	}

	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func WithFold(fold bool) ScanOptT {
	return func(o *scanOpt) {
		o.fold = fold
	}
}

func WithMaxSize(maxSz int) ScanOptT {
	return func(o *scanOpt) {
		// Allocate buffer large enough to hold the requested payload,
		// plus an extra 25% to account for formatting overhead in the
		// original payload.  When used in bufio.Scan(), size dictates
		// the maximum line size allowed, so must be large enough to
		// accommodate overhead of a line that post processing is less
		// than or equal to maxSize.
		bufSz := maxSz + (maxSz / 4)

		// Check for underflow/overflow
		if bufSz <= 0 || bufSz > MaxRecordSize {
			bufSz = MaxRecordSize
		}

		o.maxSz = bufSz
	}
}

func WithStart(start int64) ScanOptT {
	return func(o *scanOpt) {
		o.start = start
	}
}

func WithStop(stop int64) ScanOptT {
	return func(o *scanOpt) {
		o.stop = stop
	}
}

func WithMark(mark int64) ScanOptT {
	return func(o *scanOpt) {
		o.mark = mark
	}
}

func WithErrFunc(errF ErrFuncT) ScanOptT {
	return func(o *scanOpt) {
		o.errF = errF
	}
}
