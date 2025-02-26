package format

import (
	"bufio"
	"errors"
	"io"
)

type FmtType int

const (
	FmtTypeJSON FmtType = iota + 1
	FmtTypeCri
)

// Adhere to the fmt.Stringer interface
func (f FmtType) String() string {
	switch f {
	case FmtTypeJSON:
		return "json"
	case FmtTypeCri:
		return "cri"
	}
	return "unknown"
}

type LogFmt interface {
	Type() FmtType
	ReadTimestamp(rdr io.Reader) (int64, error)
	ReadEntry(line []byte) (LogEntry, error)
}

var ErrFormatDetect = errors.New("fail to detect log format")

type DetectFormatFunc func(line []byte) (LogFmt, int64, error)

var supportedFormats = []DetectFormatFunc{
	detectJSON,
	detectCri,
}

const (
	DefBufferSize = 4 << 10 // 4K
	MaxRecordSize = 4 << 20 // 4 Megabyte max record size
)

type fmtOptT struct {
	exp   string
	field string
}

func NewParser(ty FmtType) LogFmt {
	switch ty {
	case FmtTypeCri:
		return &criFmtT{}
	case FmtTypeJSON:
		return &jsonFmtT{}
	}
	return nil
}

func Detect(rdr io.Reader) (LogFmt, int64, error) {

	var (
		elist = []error{ErrFormatDetect}
		lr    = io.LimitReader(rdr, MaxRecordSize)
		bio   = bufio.NewReaderSize(lr, DefBufferSize)
	)

	line, err := bio.ReadBytes('\n')
	if err != nil {
		return nil, -1, err
	}

	for _, try := range supportedFormats {
		fmt, ts, err := try(line)
		if err == nil {
			return fmt, ts, err
		}
		elist = append(elist, err)
	}

	return nil, -1, errors.Join(elist...)
}
