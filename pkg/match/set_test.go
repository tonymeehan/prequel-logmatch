package match

import (
	"testing"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
)

func TestSet(t *testing.T) {

	type step = stepT[MatchSet]

	var tests = map[string]struct {
		clock  int64
		window int64
		terms  []string
		steps  []step
	}{
		"SingleTerm": {
			// -A---------------- alpha
			window: 10,
			terms:  []string{"alpha"},
			steps: []step{
				{line: "alpha", cb: matchStamps(1)},
			},
		},

		"IgnoreOutOfOrder": {
			// -1------ alpha
			// 2------- beta
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []step{
				{line: "alpha", stamp: 2},
				{line: "beta", stamp: 1},
			},
		},

		"Simple": {
			// A--------E-------
			// -----C-------G-H--
			// --B-----D--F------
			// Should see {A,C,B} {E,G,D}
			window: 50,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha"},
				{line: "gamma"},
				{line: "beta", cb: matchStamps(1, 3, 2)},
				{line: "gamma"},
				{line: "alpha"},
				{line: "gamma"},
				{line: "beta", cb: matchStamps(5, 7, 4), postF: checkHotMask[MatchSet](0b100)},
				{line: "beta", postF: checkHotMask[MatchSet](0b110)},
			},
		},

		"Window": {
			// A----------D------
			// --------C---------
			// -----B-------E----
			// With window of 5. should see {D,C,B}
			window: 5,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha"},
				{line: "gamma", stamp: 4},
				{line: "beta", stamp: 7},
				{line: "alpha", stamp: 8, cb: matchStamps(8, 7, 4)},
				{line: "gamma", stamp: 9, postF: checkHotMask[MatchSet](0b100)},
			},
		},

		"DupeTimestamps": {
			// -A----------------
			// -B----------------
			// -C----------------
			// Dupe timestamps are tolerated.
			window: 5,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "gamma", stamp: 1},
				{line: "beta", stamp: 1, cb: matchStamps(1, 1, 1)},
			},
		},

		"GarbageCollectOldTerms": {
			// -1------4--------------10----------
			// ---2--3----------8---9-----11----
			// ----------5--6-7---------------12-
			// Should fire {1,2,5}, {4,3,6}, {10,8,7}
			window: 50,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "beta"},
				{line: "alpha"},
				{line: "gamma", cb: matchStamps(1, 2, 5)},
				{line: "gamma", cb: matchStamps(4, 3, 6)},
				{line: "gamma"},
				{line: "beta"},
				{line: "beta"},
				{line: "alpha", cb: matchStamps(10, 8, 7)},
				{line: "beta"},
				{line: "gamma", postF: garbageCollect[*MatchSet](50)}, // window
				{postF: checkHotMask[MatchSet](0b110)},
				{postF: garbageCollect[*MatchSet](73)},
				{postF: checkHotMask[MatchSet](0b0)},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sm, err := NewMatchSet(tc.window, makeTerms(tc.terms)...)
			if err != nil {
				t.Fatalf("Expected err == nil, got %v", err)
			}

			clock := tc.clock

			for idx, step := range tc.steps {

				clock += 1
				stamp := clock
				if step.stamp != 0 {
					stamp = step.stamp
					clock = stamp
				}

				if step.line != "" {
					var (
						entry = entry.LogEntry{Timestamp: stamp, Line: step.line}
						hits  = sm.Scan(entry)
					)

					if step.cb == nil {
						checkNoFire(t, idx+1, hits)
					} else {
						step.cb(t, idx+1, hits)
					}
				}

				if step.postF != nil {
					step.postF(t, idx+1, sm)
				}
			}
		})
	}
}

// Dupes not yet implemented.
func TestSetDupes(t *testing.T) {

	_, err := NewMatchSet(10, makeTermsA("alpha", "alpha")...)
	if err != ErrDuplicateTerm {
		t.Fatalf("Expected err == ErrDuplicateTerm, got %v", err)
	}
}
