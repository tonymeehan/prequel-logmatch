package match

// MatchSeq implements a simplistic state machine where transaction from one
// state (slot) to the next is a succesful match.  When machine reaches final state
// (ie. all slots active), a match is emitted.
//
// The state matchine will reset if the intial matching slot ages out of the time window.
// The machine is edge triggered, state can only change on a new event.  As such,
// it works properly when scanning a log that is not aligned with real time.

import (
	"errors"
	"time"
)

type MatchSeq struct {
	window     int64
	hotFrame   frameT
	pendFrames []frameT
	matchers   []MatchFunc
}

type frameT struct {
	nActive int
	slots   []LogEntry
}

type slotMask uint64

func (m *slotMask) Set(slot int) {
	*m |= (1 << uint64(slot))
}

func (m slotMask) IsSet(slot int) bool {
	return (m & slotMask(1<<slot)) != 0
}

func NewMatchSeq(window time.Duration, terms ...string) (*MatchSeq, error) {
	var (
		nTerms   = len(terms)
		matchers = make([]MatchFunc, nTerms)
	)

	switch {
	case nTerms <= 1:
		return nil, errors.New("sequence matcher requires more than one term")
	case nTerms > 64:
		return nil, errors.New("sequence matcher only supports up to 64 terms")
	}

	for i, term := range terms {
		if m, err := makeMatchFunc(term); err != nil {
			return nil, err
		} else {
			matchers[i] = m
		}
	}

	return &MatchSeq{
		hotFrame: frameT{slots: make([]LogEntry, nTerms)},
		window:   int64(window),
		matchers: matchers,
	}, nil
}

func (r *MatchSeq) Scan(e LogEntry) (hits Hits) {

	// If the hot frame is expired, clear and promote if necessary
	if r.hotFrameExpired(e.Timestamp) {
		r.promotePending(e.Timestamp)
	}

	// Check if line matches first term; this is a special case
	// because a first term match starts a new frame.
	matchZero := r.matchers[0](e.Line)

	// If the hotFrame is inactive, short-circuit.
	if r.hotFrame.nActive == 0 {
		if matchZero {
			// Mark hotFrame as active
			r.hotFrame.nActive = 1
			r.hotFrame.slots[0] = e
		}
		return
	}

	// HotFrame was already previously active.
	// May or may not have pending frames.

	if len(r.pendFrames) == 0 && !matchZero {
		// HotFrame only short circuit.
		// Check if only the nActive matches; and possily hit on success.
		if r.matchers[r.hotFrame.nActive](e.Line) {
			r.hotFrame.slots[r.hotFrame.nActive] = e
			r.hotFrame.nActive += 1
			if len(r.hotFrame.slots) == r.hotFrame.nActive {
				hits = r.fireFrames()
			}
		}
		return
	}

	// At this point we have at least one pending frame,
	// although construction may be deferred until after the pending test.

	// Calcuulate the slot mask.
	var mask slotMask
	for i := 1; i < len(r.matchers); i++ {
		if r.matchers[i](e.Line) {
			mask.Set(i)
		}
	}

	// For each pending frame, check to see if the active slot matches.
	for f, frame := range r.pendFrames {
		if mask.IsSet(frame.nActive) {
			frame.slots[frame.nActive] = e
			frame.nActive += 1
			r.pendFrames[f] = frame
		}
	}

	if matchZero {
		// Append new pending frame; this was deferred until after
		// the check on the existing pending frames to avoid matching
		// this line more than once for this frame.
		nFrame := frameT{
			nActive: 1,
			slots:   make([]LogEntry, len(r.matchers)),
		}
		nFrame.slots[0] = e
		r.pendFrames = append(r.pendFrames, nFrame)
	}

	// Finally, test hotFrame.  If success, fire one or more frames.
	if mask.IsSet(r.hotFrame.nActive) {
		r.hotFrame.slots[r.hotFrame.nActive] = e
		r.hotFrame.nActive += 1
		if len(r.hotFrame.slots) == r.hotFrame.nActive {
			hits = r.fireFrames()
		}
	}

	return
}

func (r *MatchSeq) State() []byte {
	return nil
}

func (r *MatchSeq) Poll() (h Hits, d time.Duration) {
	return
}

func (r *MatchSeq) reset() {
	r.hotFrame.nActive = 0
	r.pendFrames = nil
}

func (r *MatchSeq) hotFrameExpired(ts int64) bool {
	return (r.hotFrame.nActive > 0) && ((ts - r.hotFrame.slots[0].Timestamp) > r.window)
}

// hotFrame is expired, promote pending frames if not expired
func (r *MatchSeq) promotePending(ts int64) {

	// If no pending frames, simply reset sttate.
	if len(r.pendFrames) == 0 {
		r.reset()
		return
	}

	// Promote pendFrames if not expired
	for i, frame := range r.pendFrames {

		// If not expired; promote this frame to hotFrame and following
		if (ts - frame.slots[0].Timestamp) <= r.window {
			r.hotFrame = frame
			r.pendFrames = r.pendFrames[i+1:]
			if len(r.pendFrames) == 0 {
				r.pendFrames = nil
			}
			return
		}
	}

	// Everything is expired; clear state.
	r.reset()
}

// Hot frame is active; fire hot frame and any pending that is active.
// Promote any subsequent.

func (r *MatchSeq) fireFrames() (hits Hits) {

	hits.Cnt = 1
	hits.Logs = append([]LogEntry{}, r.hotFrame.slots...)
	r.hotFrame.nActive = 0

	if r.pendFrames == nil {
		return
	}

LOOP:
	for _, frame := range r.pendFrames {

		if frame.nActive == len(frame.slots) {
			// Is active, fire the frame and shift pending
			hits.Cnt += 1
			hits.Logs = append(hits.Logs, frame.slots...)

			// This is ok since range works on a copy
			r.pendFrames = r.pendFrames[1:]
		} else {
			// Not active; promote to hotFrame
			r.hotFrame = frame
			r.pendFrames = r.pendFrames[1:]
			break LOOP
		}
	}

	// Deallocate if empty; otherwise the slice will never be GC'd
	if len(r.pendFrames) == 0 {
		r.pendFrames = nil
	}

	return
}
