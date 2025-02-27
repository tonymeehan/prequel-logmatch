package format

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/goccy/go-json"
)

var ErrJsonUnmarshal = fmt.Errorf("fail JSON unmarshal")

func NewJsonFactory() FactoryI {
	return &jsonFactoryT{}
}

type jsonLogT struct {
	Log    string    `json:"log"`
	Stream string    `json:"stream"`
	Time   time.Time `json:"time"`
}

type jsonFmtT struct {
}

type jsonFactoryT struct {
}

func (f *jsonFactoryT) New() ParserI {
	return &jsonFmtT{}
}

func (f *jsonFactoryT) String() string {
	return "json"
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

// Read Docker JSON format
// (see https://github.com/kubernetes/kubernetes/blob/v1.29.1/pkg/kubelet/kuberuntime/logs/logs.go#L189)
// Expect:
//	{"log":"content 1","stream":"stdout","time":"2016-10-20T18:39:20.57606443Z"}
//	{"log":"content 2","stream":"stderr","time":"2016-10-20T18:39:20.57606444Z"}

func (f *jsonFmtT) ReadEntry(data []byte) (entry LogEntry, err error) {
	var line jsonLogT

	if err = json.Unmarshal(data, &line); err != nil {
		err = errors.Join(ErrJsonUnmarshal, err)
		return
	}

	entry.Line = line.Log
	entry.Stream = line.Stream
	entry.Timestamp = line.Time.UnixNano()

	return
}

func detectJSON(line []byte) (FactoryI, int64, error) {

	var jf jsonFmtT
	entry, err := jf.ReadEntry(line)

	if err != nil {
		return nil, -1, err
	}

	var unsetTime time.Time

	// Validate that we have a CRI JSON log entry
	if entry.Line == "" || entry.Timestamp == unsetTime.UnixNano() {
		return nil, -1, ErrFormatDetect
	}

	switch entry.Stream {
	case tokenStdout:
	case tokenStderr:
	default:
		// Uknown stream type
		return nil, -1, ErrFormatDetect
	}

	return &jsonFactoryT{}, entry.Timestamp, nil
}
