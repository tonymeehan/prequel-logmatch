package format

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

type FormatCbT func(exp *regexp.Regexp, m []byte) (int64, error)

type regexFmtT struct {
	expTime *regexp.Regexp
	cb      FormatCbT
}

type regexFactoryT struct {
	expTime *regexp.Regexp
	cb      FormatCbT
}

func WithTimeFormat(fmtTime string) FormatCbT {
	return func(exp *regexp.Regexp, m []byte) (int64, error) {
		var (
			t   time.Time
			err error
		)

		if t, err = time.Parse(fmtTime, string(m)); err != nil {
			return 0, err
		}

		return t.UnixNano(), nil
	}
}

func NewRegexFactory(expTime string, cb FormatCbT) (FactoryI, error) {

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

		m := f.expTime.Find(scanner.Bytes())
		if m == nil {
			fmt.Println("no match")
			err = ErrNoTimestamp
			return
		}

		ts, err = f.parseTime(m)

	} else {
		err = scanner.Err()
	}

	return
}

func (f *regexFmtT) parseTime(m []byte) (ts int64, err error) {
	return f.cb(f.expTime, m)
}

// Read custom format
func (f *regexFmtT) ReadEntry(data []byte) (entry LogEntry, err error) {
	m := f.expTime.Find(data)
	if m == nil {
		err = ErrNoTimestamp
		return
	}

	ts, err := f.parseTime(m)
	if err != nil {
		return
	}

	entry.Line = string(data)
	entry.Timestamp = ts
	return
}
