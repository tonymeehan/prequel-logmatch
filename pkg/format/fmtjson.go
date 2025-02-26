package format

import (
	"fmt"
	"io"
	"time"

	"github.com/goccy/go-json"
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

// Read Docker JSON format
// (see https://github.com/kubernetes/kubernetes/blob/v1.29.1/pkg/kubelet/kuberuntime/logs/logs.go#L189)
// Expect:
//	{"log":"content 1","stream":"stdout","time":"2016-10-20T18:39:20.57606443Z"}
//	{"log":"content 2","stream":"stderr","time":"2016-10-20T18:39:20.57606444Z"}

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

func detectJSON(line []byte) (LogFmt, int64, error) {

	var jf jsonFmtT
	entry, err := jf.ReadEntry(line)

	if err != nil {
		return nil, -1, err
	}

	return &jf, entry.Timestamp, nil
}
