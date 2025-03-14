package match

// A somewhat expensive matcher that handles inverse terms with sliding windows.
//
// This should be optimized:
// - Can build a much simpler matcher if all inverse terms are lined up.
// - If not matching inverse terms in the past, there is no reason to keep a history and do garbage collection.
// - The gc trigger is arbitrary.   Need a cleaner solution.
// - Need to add thresholds for memory usage.  If a user specifies a huge lookup and we get a lot of inverse
//   hits, we could be holding on to way to0 much ram.   At a certain threshold, should drop samples.
//
// That said, seems to be working for the moment.

import (
	"cmp"
	"errors"
	"slices"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
	"github.com/rs/zerolog/log"
)

type InverseSeq struct {
	mseq     *MatchSeq
	nTerms   int
	gcSpins  int
	terms    [][]int64
	matchers []matcherT
	pending  []pendingT
}

type InverseTerm struct {
	Term     string // Inverse term
	Window   int64  // Window size; defaults to 0 which in combination with !Absolute means the window is the range of the matched sequence.
	Slide    int64  // Slide the anchor, +/- relative to the anchor term
	Anchor   int    // Anchor term; defaults to first event in match sequence
	Absolute bool   // Absolute window time or relative to the range of the matched sequence.
}

type matcherT struct {
	mfunc    MatchFunc
	window   int64
	slide    int64
	anchor   int
	absolute bool
}

type pendingT struct {
	hit      []entry.LogEntry
	fireMark int64
}

var (
	gcMaxSpins = 256
)

func NewInverseSeq(window time.Duration, seqTerms []string, inverseTerms []InverseTerm) (*InverseSeq, error) {

	var (
		nTerms   = len(inverseTerms)
		matchers = make([]matcherT, 0, nTerms)
	)

	for _, term := range inverseTerms {
		m, err := makeMatchFunc(term.Term)
		if err != nil {
			return nil, err
		}
		if term.Anchor < 0 || term.Anchor >= len(seqTerms) {
			return nil, errors.New("anchor out of range")
		}
		matchers = append(matchers, matcherT{
			mfunc:    m,
			window:   term.Window,
			slide:    term.Slide,
			anchor:   term.Anchor,
			absolute: term.Absolute,
		})
	}

	mseq, err := NewMatchSeq(window, seqTerms...)
	if err != nil {
		return nil, err
	}

	return &InverseSeq{
		mseq:     mseq,
		matchers: matchers,
		terms:    make([][]int64, nTerms),
	}, nil
}

func (r *InverseSeq) Scan(e entry.LogEntry) (hits Hits) {

	// Run the inverse matchers
	for i, m := range r.matchers {
		if m.mfunc(e.Line) {
			r.terms[i] = append(r.terms[i], e.Timestamp)
			r.nTerms += 1
		}
	}

	// Run the sequence matchers.
	if nHits := r.mseq.Scan(e); nHits.Cnt > 0 {
		r.processHits(nHits)
	}

	return r.Eval(e.Timestamp)
}

// For each hit, check if can be dropped due to inverse.
// Otherwise, determine its fireTime and add to pending.

func (r *InverseSeq) processHits(hits Hits) {

	for i := 0; i < hits.Cnt; i++ {
		hit := hits.Index(i)

		fireMark, drop := r.calcMark(hit)
		if drop {
			log.Debug().Any("hits", hits).Msg("Dropping hit")
			continue
		}

		pending := pendingT{
			hit:      hit,
			fireMark: fireMark,
		}

		// t.pending is sorted in ascending order of fireMark
		nPending := len(r.pending)
		if nPending == 0 || fireMark >= r.pending[nPending-1].fireMark {
			r.pending = append(r.pending, pending)
		} else {
			idx, _ := slices.BinarySearchFunc(r.pending, fireMark, func(a pendingT, b int64) int {
				return cmp.Compare(a.fireMark, b)
			})
			r.pending = slices.Insert(r.pending, idx, pending)
		}
	}
}

func (r *InverseSeq) checkDrop(hit []LogEntry) bool {
	// Short-circuit on no terms
	if r.nTerms == 0 {
		return false
	}

	for i, m := range r.matchers {
		start, stop := m.calcWindow(hit)

		// Check if we have a negative in this window
		// TODO: O(n); could probably do binary search here
		for _, stamp := range r.terms[i] {
			if stamp >= start && stamp <= stop {
				return true
			}
		}
	}
	return false
}

func (r *InverseSeq) calcMark(hit []LogEntry) (int64, bool) {

	var hitMark int64

	for i, m := range r.matchers {
		start, stop := m.calcWindow(hit)
		if stop > hitMark {
			hitMark = stop
		}

		// Check if we have a negative in this window
		// TODO: O(n); could probably do binary search here
		for _, stamp := range r.terms[i] {
			if stamp >= start && stamp <= stop {
				return 0, true
			}
		}
	}
	return hitMark, false
}

// Assert  clock, may used to close out matcher
func (r *InverseSeq) Eval(clock int64) (hits Hits) {

	// Can we fire any pending hits?
	for _, pending := range r.pending {

		if pending.fireMark > clock {
			break
		}

		if drop := r.checkDrop(pending.hit); drop {
			log.Trace().
				Any("hits", pending.hit).
				Msg("Dropping hit on last check due to inverse conditions")
		} else {
			hits.Cnt += 1
			hits.Logs = append(hits.Logs, pending.hit...)
		}
		r.pending = r.pending[1:]
	}

	r.maybeGarbageCollect(clock)

	return
}

// // Poll for state change.
// // Returns HIT if currently active.
// // Should poll again in 'dur'.
// // Poll doesn't work consistently on historic data, has to be a live feed.

// func (r *InverseSeq) Poll() (hits Hits, hint time.Duration) {
// 	hint = time.Duration(r.window)
// 	if r.active == 0 {
// 		return
// 	}

// 	now := time.Now().UnixNano()

// 	if now-r.active >= r.window {
// 		hits = r.pending
// 		r.pending.Cnt = 0
// 		r.pending.Logs = nil
// 	} else {
// 		hint = time.Duration(now - r.active)
// 	}

// 	return
// }

func (r *InverseSeq) State() []byte {
	return nil
}

func (r *InverseSeq) maybeGarbageCollect(clock int64) {
	// Short circuit if under threshold
	if r.gcSpins += 1; r.gcSpins > gcMaxSpins {
		r.gcSpins = 0
		r.garbageCollect(clock)
	}
}

// Dump gc terms before the window margin.
func (r *InverseSeq) garbageCollect(clock int64) {
	if r.nTerms == 0 {
		return
	}

	// Can only safe old terms if all the pending hit have been checked.
	// Pending hits are checked on insertion and firemark.
	// Need to validate before garbage collect.
	// Iterate backwards to avoid index shifting.
	for i := len(r.pending) - 1; i >= 0; i-- {
		pending := r.pending[i]

		if drop := r.checkDrop(pending.hit); drop {
			log.Trace().
				Any("hits", pending.hit).
				Msg("Dropping hit due to inverse conditions")
			r.pending = slices.Delete(r.pending, i, i+1)
		}
	}

	// Window margin is the earliest possible timestamp for a hit given defined neg windows.
	// O(n) on matchers
	// TODO: cache this?
	var margin int64 = 0
	for _, m := range r.matchers {
		if m.slide < margin {
			margin = m.slide
		}
	}

	// O(n) on terms
	deadline := clock + margin
	for i, stamps := range r.terms {
		if len(stamps) == 0 {
			continue
		}

		if stamps[0] <= deadline {
			cnt := 1
			for j := 1; j < len(stamps); j++ {
				if stamps[j] > deadline {
					break
				}
				cnt += 1
			}
			r.terms[i] = stamps[cnt:]
			r.nTerms -= cnt
		}
	}
}

func (m matcherT) calcWindow(hit []entry.LogEntry) (start int64, stop int64) {
	var (
		width  = m.window
		anchor = hit[m.anchor].Timestamp
	)

	// Slide the anchor if necessary
	anchor += m.slide

	// Determine the width of the window
	if !m.absolute {
		width += hit[len(hit)-1].Timestamp - hit[0].Timestamp
	}
	if width <= 0 {
		// Effectively disables the negative window
		width = 0
	}

	// Calculate the start and stop times
	start = anchor
	stop = anchor + width
	return
}
