package match

import (
	"time"

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

func (r *MatchSingle) State() []byte {
	return nil
}

func (r *MatchSingle) Poll() (h Hits, d time.Duration) {
	return
}
