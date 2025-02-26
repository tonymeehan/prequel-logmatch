package format

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/icza/backscanner"
	"github.com/rs/zerolog/log"
)

const (
	delimiter   = ' '
	pageSize    = 4096
	tsBufSize   = 64
	avgLogSize  = 256
	tokenStdout = "stdout"
	tokenStderr = "stderr"
)

var (
	ErrNoTimestamp   = errors.New("no timestamp delimeter")
	ErrNoStreamType  = errors.New("no stream delimeter")
	ErrNoTag         = errors.New("no tag delimeter")
	ErrUnknownStream = errors.New("unknown stream type")
	ErrParseTimesamp = errors.New("fail parse timestamp")

	sliceStdout = []byte(tokenStdout)
	sliceStderr = []byte(tokenStderr)
)

type criFmtT struct {
}

func (f *criFmtT) Type() FmtType {
	return FmtTypeCri
}

func (f *criFmtT) ReadTimestamp(rdr io.Reader) (ts int64, err error) {

	ptr := poolAlloc()
	defer poolFree(ptr)
	buf := (*ptr)[:tsBufSize]

	n, err := io.ReadFull(rdr, buf)
	switch err {
	case nil:
	case io.EOF, io.ErrUnexpectedEOF:
		if n == 0 {
			return
		}
	default:
		return
	}

	return scanCriTimestamp(buf[:n])
}

func (f *criFmtT) ReadEntry(line []byte) (LogEntry, error) {
	return readCriEntry(line)
}

func (f *criFmtT) ScanForward(rdr io.Reader, maxSz int, stop int64, scanF ScanFuncT) error {

	var (
		buf     []byte
		bufSz   = calcBufSize(maxSz)
		scanner = bufio.NewScanner(rdr)
	)

	if bufSz == MaxRecordSize {
		ptr := poolAlloc()
		defer poolFree(ptr)
		buf = *ptr
	} else {
		buf = make([]byte, bufSz)
	}

	// Scanner will bail with bufio.ErrTooLong if it encounters a line that is > bufSz.
	scanner.Buffer(buf, bufSz)

LOOP:
	for scanner.Scan() {

		entry, lerr := readCriEntry(scanner.Bytes())
		if lerr != nil {
			// Tolerate badly formed lines
			log.Error().
				Err(lerr).
				Str("line", scanner.Text()).
				Msg("Fail CRI decode.  Continue...")
			continue
		}

		if entry.Timestamp > stop {
			break LOOP
		}

		if scanF(entry) {
			break LOOP
		}
	}

	return scanner.Err()
}

func (f *criFmtT) ScanReverse(src io.ReaderAt, maxSz int, stop, mark int64, scanF ScanFuncT) error {

	var (
		err   error
		bufSz = (maxSz/pageSize + 1) * pageSize
		opts  = backscanner.Options{
			ChunkSize:     bufSz,
			MaxBufferSize: maxSize(bufSz),
		}
		scanner = backscanner.NewOptions(src, int(mark), &opts)
	)

LOOP:
	for {
		var line []byte
		line, _, err = scanner.LineBytes()

		switch err {
		case nil:
		case io.EOF:
			err = nil
			break LOOP
		default:
			break LOOP
		}

		entry, lerr := readCriEntry(line)
		if lerr != nil {
			// Tolerate badly formed lines
			log.Error().
				Err(err).
				Str("line", string(line)).
				Msg("Fail CRI decode.  Continue...")
			continue
		}

		if entry.Timestamp < stop {
			break LOOP
		}

		if scanF(entry) {
			break LOOP
		}
	}

	return err
}

// Read CRI Entry line
// (see https://github.com/kubernetes/kubernetes/blob/v1.29.1/pkg/kubelet/kuberuntime/logs/logs.go#L129)
// Expects format:
//	2016-10-06T00:17:09.669794202Z stdout P log content 1
//	2016-10-06T00:17:09.669794203Z stderr F log content 2

func readCriEntry(line []byte) (entry LogEntry, err error) {

	idx := bytes.IndexByte(line, delimiter)
	if idx < 0 {
		entry = LogEntry{}
		err = ErrNoTimestamp
		return
	}

	ts, err := time.Parse(time.RFC3339Nano, string(line[:idx]))
	if err != nil {
		err = errors.Join(ErrParseTimesamp, err)
		return
	}

	entry.Timestamp = ts.UnixNano()

	line = line[idx+1:]
	idx = bytes.IndexByte(line, delimiter)
	if idx < 0 {
		entry = LogEntry{}
		err = ErrNoStreamType
		return
	}

	// Avoid extra allocation of string to hold stream;
	// Use existing const which is copied as a reference.
	stream := line[:idx]
	switch {
	case bytes.Equal(stream, sliceStdout):
		entry.Stream = tokenStdout
	case bytes.Equal(stream, sliceStderr):
		entry.Stream = tokenStderr
	default:
		entry = LogEntry{}
		err = fmt.Errorf("unknown stream %s: %w", stream, ErrUnknownStream)
		return
	}

	// Search past tag; we are ignoring for now
	line = line[idx+1:]
	idx = bytes.IndexByte(line, delimiter)
	if idx < 0 {
		entry = LogEntry{}
		err = ErrNoTag
		return
	}

	entry.Line = string(line[idx+1:])
	return
}

func scanCriTimestamp(buf []byte) (int64, error) {
	offset := bytes.IndexByte(buf, delimiter)

	if offset < 0 {
		return -1, ErrNoTimestamp
	}

	ts, err := time.Parse(time.RFC3339Nano, string(buf[:offset]))
	if err != nil {
		return -1, err
	}
	return ts.UnixNano(), nil
}

func detectCri(line []byte) (LogFmt, int64, error) {

	var cf criFmtT
	entry, err := cf.ReadEntry(line)

	if err != nil {
		return nil, -1, err
	}

	return &cf, entry.Timestamp, nil
}
