package scanner

// Reorder is a simple FIFO queue that reorders log entries
// based on their timestamps.  It is used to ensure that
// log entries are processed in the order they were
// received, even if they arrive out of order.  This is
// important for log entries that are processed in a
// distributed system, where log entries may arrive at
// different times due to network latency or other factors.

// TODO:
// Add circuit breakers on RAM usage

import (
	"errors"
	"math"
	"sync"

	"github.com/rs/zerolog/log"
)

var (
	ErrInvalidWindow   = errors.New("invalid window")
	ErrInvalidCallback = errors.New("invalid callback")
)

type reorderT struct {
	cb     ScanFuncT
	window int64
	clock  int64
	inList *rListT
	ooList *rListT
}

// Specify lookback window in nanoseconds; entries will
// be reordred within this window.  This implies that
// entires will not be delivered to 'cb' until they shift
// outside the window.

func NewReorder(window int64, cb ScanFuncT) (*reorderT, error) {
	if window <= 0 {
		return nil, ErrInvalidWindow
	}
	if cb == nil {
		return nil, ErrInvalidCallback
	}

	return &reorderT{
		cb:     cb,
		window: window,
		inList: newRList(),
		ooList: newRList(),
	}, nil
}

func (r *reorderT) Append(entry LogEntry) bool {

	// Check entry is out of order
	if entry.Timestamp < r.clock {
		r.queueOutofOrder(entry)
		return false
	}

	// We are in order, queue and continue
	r.clock = entry.Timestamp
	r.inList.pushBack(entry)
	return r._flush()
}

func (r *reorderT) _flush() bool {
	ooHead := r.ooList.front()
	if ooHead == nil {
		return r.fastPath()
	}
	return r.slowPath(ooHead.entry.Timestamp)
}

func (r *reorderT) slowPath(ooTimestamp int64) bool {

	var (
		deadline = r.clock - r.window
	)

	// Iterate across pending entries
	for inHead := r.inList.front(); inHead != nil; inHead = r.inList.front() {
		if inHead.entry.Timestamp > deadline {
			break
		}

		// Entry is outside window and should be delivered.
		// Deliver any pending out of order entries that are
		// older than the in order entry.
		for ooTimestamp < inHead.entry.Timestamp {
			var (
				node = r.ooList.popFront()
				done = r.cb(node.entry)
			)
			rPoolFree(node)

			if done {
				r.drain()
				return true
			}

			if ooHead := r.ooList.front(); ooHead == nil {
				ooTimestamp = math.MaxInt64
			} else {
				ooTimestamp = ooHead.entry.Timestamp
			}
		}

		if done := r.cb(inHead.entry); done {
			r.drain()
			return true
		}

		r.inList.remove(inHead)
		rPoolFree(inHead)
	}

	// Flush out any out of order entries that are older than window
	// Entry is outside window and should be delivered.
	for ooTimestamp <= deadline {
		var (
			node = r.ooList.popFront()
			done = r.cb(node.entry)
		)
		rPoolFree(node)

		if done {
			r.drain()
			return true
		}

		if ooHead := r.ooList.front(); ooHead == nil {
			ooTimestamp = math.MaxInt64
		} else {
			ooTimestamp = ooHead.entry.Timestamp
		}
	}

	return false
}

func (r *reorderT) AdvanceClock(stamp int64) bool {
	if stamp < r.clock {
		log.Info().
			Int64("clock", r.clock).
			Int64("stamp", stamp).
			Msg("Reorder: ignore  setclock stamp regression")
		return false
	}
	r.clock = stamp
	return r._flush()
}

func (r *reorderT) Flush() (done bool) {
	done = r.AdvanceClock(math.MaxInt64)
	r.drain()
	return done
}

func (r *reorderT) fastPath() bool {

	deadline := r.clock - r.window
	for head := r.inList.front(); head != nil; head = r.inList.front() {
		if head.entry.Timestamp > deadline {
			break
		}

		if done := r.cb(head.entry); done {
			r.drain()
			return true
		}

		r.inList.remove(head)
		rPoolFree(head)
	}

	return false
}

// O(n): Maintain order invariant on insert.
// This is linear, but given how the data typically arrives,
// it is unlikely to be a problem.  Could use a tree structure
// if inserts become expensive.
func (r *reorderT) queueOutofOrder(entry LogEntry) {
	deadline := r.clock - r.window
	if entry.Timestamp < deadline {
		log.Debug().
			Int64("clock", r.clock).
			Int64("stamp", entry.Timestamp).
			Int64("deadline", deadline).
			Str("line", entry.Line).
			Msg("Reorder: ignore out of order entry")
		return
	}

	node := r.ooList.back()
	for ; node != nil; node = r.ooList.prev(node) {
		if node.entry.Timestamp <= entry.Timestamp {
			break
		}
	}
	if node == nil {
		r.ooList.pushFront(entry)
	} else {
		r.ooList.insert(entry, node)
	}
}

func (r *reorderT) drain() {
	r.ooList.free()
	r.inList.free()
	r.clock = 0
}

// ----

var rpool = sync.Pool{
	New: func() any {
		return new(rnodeT)
	},
}

func rPoolAlloc() *rnodeT {
	return rpool.Get().(*rnodeT)
}
func rPoolFree(ptr *rnodeT) {
	ptr.next = nil
	ptr.prev = nil
	rpool.Put(ptr)
}

type rnodeT struct {
	entry LogEntry
	next  *rnodeT
	prev  *rnodeT
}

type rListT struct {
	root rnodeT
}

func newRList() *rListT {
	ll := &rListT{}
	ll.root.next = &ll.root
	ll.root.prev = &ll.root
	return ll
}

// insert e after at
func (ll *rListT) _insert(e, at *rnodeT) *rnodeT {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	return e
}

func (ll *rListT) free() {
	for head := ll.popFront(); head != nil; head = ll.popFront() {
		rPoolFree(head)
	}
}

func (ll *rListT) popFront() *rnodeT {
	if ll.root.next == &ll.root {
		return nil
	}
	return ll.remove(ll.root.next)
}

func (ll *rListT) pushBack(v LogEntry) *rnodeT {
	return ll.insert(v, ll.root.prev)
}

func (ll *rListT) pushFront(v LogEntry) *rnodeT {
	return ll.insert(v, &ll.root)
}

func (ll *rListT) insert(v LogEntry, at *rnodeT) *rnodeT {
	e := rPoolAlloc()
	e.entry = v
	return ll._insert(e, at)
}

func (ll *rListT) remove(e *rnodeT) *rnodeT {
	e.prev.next = e.next
	e.next.prev = e.prev
	return e
}

func (ll *rListT) front() *rnodeT {
	if ll.root.next == &ll.root {
		return nil
	}
	return ll.root.next
}

func (ll *rListT) back() *rnodeT {
	if ll.root.prev == &ll.root {
		return nil
	}
	return ll.root.prev
}

func (ll *rListT) prev(e *rnodeT) *rnodeT {
	if p := e.prev; p != &ll.root {
		return p
	}
	return nil
}

func (ll *rListT) empty() bool {
	return ll.root.next == &ll.root
}
