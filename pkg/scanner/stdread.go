package scanner

import "github.com/prequel-dev/prequel-logmatch/pkg/entry"

type LogEntry = entry.LogEntry

const avgLogSize = 256

type StdReadScan struct {
	Sz    int
	MaxSz int
	Clip  bool
	Logs  []LogEntry
}

func NewStdReadScan(maxSz int) *StdReadScan {
	return &StdReadScan{
		MaxSz: maxSz,
		Logs:  make([]LogEntry, 0, maxSz/avgLogSize),
	}
}

func (sr *StdReadScan) Scan(entry LogEntry) bool {
	sz := entry.Size()
	if sr.Sz += sz; sr.Sz > sr.MaxSz {
		sr.Clip = true
		sr.Sz -= sz
		return true
	}

	sr.Logs = append(sr.Logs, entry)
	return false
}
