package format

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/goccy/go-json"
	"github.com/icza/backscanner"
	"github.com/rs/zerolog/log"
)

type jsonLogT struct {
	Log    string    `json:"log"`
	Stream string    `json:"stream"`
	Time   time.Time `json:"time"`
}

type jsonFmtT struct {
}

func (f *jsonFmtT) Type() FmtType {
	return FmtTypeJSON
}

func (f *jsonFmtT) ReadTimestamp(rdr io.Reader) (ts int64, err error) {

	var (
		line struct {
			Time time.Time `json:"time"`
		}
		decoder = json.NewDecoder(rdr)
	)

	if err = decoder.Decode(&line); err != nil {
		return
	}

	return line.Time.UnixNano(), nil
}

func (f *jsonFmtT) ReadEntry(data []byte) (entry LogEntry, err error) {
	var line jsonLogT

	if err = json.Unmarshal(data, &line); err != nil {
		err = fmt.Errorf("fail JSON unmarshal: %w", err)
		return
	}

	entry.Line = line.Log
	entry.Stream = line.Stream
	entry.Timestamp = line.Time.UnixNano()

	return
}

// Read Docker JSON format
// (see https://github.com/kubernetes/kubernetes/blob/v1.29.1/pkg/kubelet/kuberuntime/logs/logs.go#L189)
// Expect:
//	{"log":"content 1","stream":"stdout","time":"2016-10-20T18:39:20.57606443Z"}
//	{"log":"content 2","stream":"stderr","time":"2016-10-20T18:39:20.57606444Z"}

func (f *jsonFmtT) ScanForward(rdr io.Reader, maxSz int, stop int64, scanF ScanFuncT) error {

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

		var line jsonLogT

		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			// Tolerate occassional bad records
			log.Error().
				Err(err).
				Str("line", scanner.Text()).
				Msg("Fail JSON decode.  Continue...")
			continue
		}

		entry := LogEntry{
			Line:      line.Log,
			Stream:    line.Stream,
			Timestamp: line.Time.UnixNano(),
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

func maxSize(chunkSz int) int {
	maxSize := 1024 * 1024
	if maxSize < chunkSz {
		maxSize = chunkSz * 2
	}
	return maxSize
}

func (f *jsonFmtT) ScanReverse(src io.ReaderAt, maxSz int, stop, mark int64, scanF ScanFuncT) error {

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

		entry, lerr := f.ReadEntry(line)
		if lerr != nil {
			// Tolerate occassional bad records
			log.Error().
				Err(lerr).
				Str("line", string(line)).
				Msg("Fail JSON decode.  Continue...")
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

func detectJSON(line []byte) (LogFmt, int64, error) {

	var jf jsonFmtT
	entry, err := jf.ReadEntry(line)

	if err != nil {
		return nil, -1, err
	}

	return &jf, entry.Timestamp, nil
}
