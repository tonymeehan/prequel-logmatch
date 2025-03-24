package match

import (
	"math"

	"github.com/rs/zerolog/log"
)

const disableGC int64 = math.MaxInt64

type MatchSet struct {
	clock   int64
	window  int64
	gcMark  int64
	hotMask bitMaskT
	terms   []termT
}

func NewMatchSet(window int64, setTerms ...string) (*MatchSet, error) {
	var (
		nTerms = len(setTerms)
		terms  = make([]termT, nTerms)
	)

	for i, term := range setTerms {
		if m, err := makeMatchFunc(term); err != nil {
			return nil, err
		} else {
			terms[i].matcher = m
		}
	}

	return &MatchSet{
		terms:  terms,
		window: window,
		gcMark: disableGC,
	}, nil
}

func (r *MatchSet) Scan(e LogEntry) (hits Hits) {
	if e.Timestamp < r.clock {
		log.Warn().
			Str("line", e.Line).
			Int64("stamp", e.Timestamp).
			Int64("clock", r.clock).
			Msg("MatchSet: Out of order event.")
		return
	}
	r.clock = e.Timestamp

	r.maybeGC(e.Timestamp)

	// For a set, must scan all terms.
	// Cannot short circuit like a sequence.
	for i, term := range r.terms {
		if term.matcher(e.Line) {
			r.terms[i].asserts = append(r.terms[i].asserts, e)
			r.hotMask.Set(i)
			if e.Timestamp < r.gcMark {
				r.gcMark = e.Timestamp
			}
		}
	}

	if !r.hotMask.FirstN(len(r.terms)) {
		return // no match
	}

	// We have a full frame; fire and prune.
	hits.Cnt = 1
	hits.Logs = make([]LogEntry, 0, len(r.terms))

	r.gcMark = disableGC
	for i, term := range r.terms {
		m := term.asserts
		hits.Logs = append(hits.Logs, m[0])
		if len(m) == 1 && cap(m) <= capThreshold {
			m = m[:0]
		} else {
			m = m[1:]
		}
		r.terms[i].asserts = m

		if len(m) == 0 {
			r.hotMask.Clr(i)
		} else if m[0].Timestamp < r.gcMark {
			r.gcMark = m[0].Timestamp
		}
	}

	return
}

func (r *MatchSet) maybeGC(clock int64) {
	if r.hotMask.Zeros() || clock-r.gcMark <= r.window {
		return
	}

	r.GarbageCollect(clock)
}

// Remove all terms that are older than the window.
func (r *MatchSet) GarbageCollect(clock int64) {

	deadline := clock - r.window

	r.gcMark = disableGC

	for i, term := range r.terms {

		var cnt int

		for _, assert := range term.asserts {
			if assert.Timestamp >= deadline {
				break
			}
			cnt += 1
		}

		if cnt > 0 {
			shiftLeft(r.terms, i, cnt)
		}

		m := r.terms[i].asserts
		if len(m) == 0 {
			r.hotMask.Clr(i)
		} else if v := m[0].Timestamp; v < r.gcMark {
			r.gcMark = v
		}
	}
}

// Because match sequence is edge triggered, there won't be hits.  But can GC.
func (r *MatchSet) Eval(clock int64) (h Hits) {
	return
}
