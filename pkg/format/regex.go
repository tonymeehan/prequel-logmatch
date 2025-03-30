package format

import (
	"bufio"
	"io"
	"regexp"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

type TimeFormatCbT func(m []byte) (int64, error)

type regexFmtT struct {
	expTime *regexp.Regexp
	cb      TimeFormatCbT
}

type regexFactoryT struct {
	expTime *regexp.Regexp
	cb      TimeFormatCbT
}

func WithTimeFormat(fmtTime string) TimeFormatCbT {
	return func(m []byte) (int64, error) {
		var (
			t   time.Time
			err error
		)

		if t, err = time.Parse(fmtTime, string(m)); err != nil {
			return 0, err
		}

		return t.UTC().UnixNano(), nil
	}
}

func NewRegexFactory(expTime string, cb TimeFormatCbT) (FactoryI, error) {

	var (
		exp *regexp.Regexp
		err error
	)

	// Expression must compile
	if exp, err = regexp.Compile(expTime); err != nil {
		return nil, err
	}

	return &regexFactoryT{
		expTime: exp,
		cb:      cb,
	}, nil
}

func (f *regexFactoryT) New() ParserI {
	return &regexFmtT{expTime: f.expTime, cb: f.cb}
}

func (f *regexFactoryT) String() string {
	return "regex"
}

func (f *regexFmtT) ReadTimestamp(rdr io.Reader) (ts int64, err error) {

	var (
		scanner = bufio.NewScanner(rdr)
	)

	ptr := pool.PoolAlloc()
	defer pool.PoolFree(ptr)
	buf := *ptr

	scanner.Buffer(buf, pool.MaxRecordSize)

	// Scanner will bail with bufio.ErrTooLong
	// if it encounters a line that is > o.maxSz.

	if scanner.Scan() {
		m := f.expTime.FindSubmatch(scanner.Bytes())
		if len(m) <= 1 {
			err = ErrMatchTimestamp
			return
		}

		ts, err = f.cb(m[1])

	} else {
		err = scanner.Err()
	}

	return
}

// Read custom format
func (f *regexFmtT) ReadEntry(data []byte) (entry LogEntry, err error) {
	m := f.expTime.FindSubmatch(data)
	if len(m) <= 1 {
		err = ErrMatchTimestamp
		return
	}

	ts, err := f.cb(m[1])
	if err != nil {
		return
	}

	entry.Line = string(data)
	entry.Timestamp = ts
	return
}
