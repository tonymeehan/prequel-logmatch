package match

import (
	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
)

type MatchSingle struct {
	matcher MatchFunc
}

func NewMatchSingle(str string) (*MatchSingle, error) {
	m, err := makeMatchFunc(str)
	if err != nil {
		return nil, err
	}

	return &MatchSingle{matcher: m}, nil
}

func (r *MatchSingle) Scan(e entry.LogEntry) (hits Hits) {

	if r.matcher(e.Line) {
		hits.Cnt = 1
		hits.Logs = []entry.LogEntry{e}
	}

	return
}
