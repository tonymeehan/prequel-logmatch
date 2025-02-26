package match

// Attempts to fire a hit only once in a specific window,
// and fire subsequent hits to extend window.

import (
	"time"
)

type Dedupe struct {
	window  int64
	active  int64
	pendHit []LogEntry
}

// Extra time to deal with inaccuracies of timer on poll hint
const kSlop = time.Duration(time.Millisecond * 10)

func NewDedupe(window time.Duration) *Dedupe {
	return &Dedupe{
		window: int64(window),
	}
}

func (dd *Dedupe) maybeFirePending(clock int64) (fire []LogEntry) {
	switch {
	case clock < dd.active:
		// Active window still valid
	case dd.pendHit == nil:
		// Active window expired, no pending hit
		dd.active = 0
	default:
		// Acive window expired, but we have a pending hit
		fire = dd.pendHit
		dd.pendHit = nil
		dd.active = fire[0].Timestamp + dd.window
	}
	return
}

func (dd *Dedupe) MaybeFire(clock int64, hits Hits) (fire []LogEntry, hint time.Duration) {
	if hits.Cnt <= 0 {
		if dd.active > 0 {
			fire = dd.maybeFirePending(clock)
		}
		return
	}
	return dd._maybeFire(clock, hits)
}

func (dd *Dedupe) _maybeFire(clock int64, hits Hits) (fire []LogEntry, hint time.Duration) {

	if dd.active == 0 {
		dd.active = hits.Logs[0].Timestamp + dd.window
		fire = hits.PopFront()
	} else if clock >= dd.active {
		// active has expired, fire the latest hit
		dd.active = hits.Logs[0].Timestamp + dd.window
		fire = hits.PopFront()
	}

	// If any this left, the last is pending
	if hits.Cnt > 0 {
		// Only return 'hint'' on the first pending hit
		if dd.pendHit == nil {
			tdiff := dd.active - time.Now().UnixNano()
			if tdiff > 0 {
				hint = time.Duration(tdiff) + kSlop
			} else {
				hint = 1 // non-zero; fire now.
			}
		}
		dd.pendHit = hits.Last()
	}

	return
}

// Handle case where expiration of active window does not occur
// naturally and we have a pending hit.
// Only works accurately when log is running at real time.

func (dd *Dedupe) PollFire() []LogEntry {
	// No active window, nothing to do
	if dd.active == 0 {
		return nil
	}

	// Active window still valid
	now := time.Now().UnixNano()
	if now < dd.active {
		return nil
	}

	// Active window expired, promote pending hit if any
	if dd.pendHit == nil {
		dd.active = 0
		return nil
	}

	// If pending hit is also expired, clear state
	if dd.pendHit[0].Timestamp+dd.window < now {
		dd.active = 0
		dd.pendHit = nil
		return nil
	}

	// Fire the pending hit, make it active
	fire := dd.pendHit
	dd.pendHit = nil
	dd.active = fire[0].Timestamp + dd.window
	return fire
}
