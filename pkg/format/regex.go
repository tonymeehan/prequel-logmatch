package format

import (
	"bufio"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

type regexFmtT struct {
	expTime *regexp.Regexp
	fmtTime string
}

type regexFactoryT struct {
	expTime *regexp.Regexp
	fmtTime string
}

func NewRegexFactory(expTime, fmtTime string) (FactoryI, error) {
	// Expression must compile
	exp, err := regexp.Compile(expTime)
	if err != nil {
		return nil, err
	}

	return &regexFactoryT{
		expTime: exp,
		fmtTime: fmtTime,
	}, nil
}

func (f *regexFactoryT) New() ParserI {

	return &regexFmtT{expTime: f.expTime, fmtTime: f.fmtTime}
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
	t, err := time.Parse(f.fmtTime, string(m))
	if err != nil {
		err = errors.Join(ErrParseTimesamp, err)
		return
	}

	return t.UnixNano(), nil
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
