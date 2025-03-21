package match

import "testing"

type stepT[T any] struct {
	stamp int64
	line  string
	cb    func(*testing.T, int, Hits)
	postF func(*testing.T, int, *T)
}

func matchStamps(stamps ...int64) func(*testing.T, int, Hits) {
	return matchStampsN(1, stamps...)
}

func matchStampsN(cnt int, stamps ...int64) func(*testing.T, int, Hits) {
	return func(t *testing.T, step int, hits Hits) {
		t.Helper()
		if cnt != hits.Cnt {
			t.Errorf("Step %v: Expected %v hits, got %v", step, cnt, hits.Cnt)
			return
		}

		for i, stamp := range stamps {
			if hits.Logs[i].Timestamp != stamp {
				t.Errorf("Step %v: Expected %v, got %v on index %v", step, stamp, hits.Logs[i].Timestamp, i)
			}
		}
	}
}

// Define a type constraint that only allows string or int64

func checkHotMask[T any](mask int64) func(*testing.T, int, *T) {
	return func(t *testing.T, step int, sm *T) {
		t.Helper()
		var hotMask bitMaskT
		switch v := any(sm).(type) {
		case *InverseSet:
			hotMask = v.hotMask
		case *MatchSet:
			hotMask = v.hotMask
		default:
			panic("Invalid type")
		}

		if hotMask != bitMaskT(mask) {
			t.Errorf("Step %v: Expected hotMask == %b, got %b", step, mask, hotMask)
		}
	}
}

func checkEval(clock int64, cb func(*testing.T, int, Hits)) func(*testing.T, int, *InverseSet) {
	return func(t *testing.T, step int, sm *InverseSet) {
		t.Helper()
		hits := sm.Eval(clock)
		cb(t, step, hits)
	}
}

func checkNoFire(t *testing.T, step int, hits Hits) {
	t.Helper()
	if hits.Cnt != 0 {
		t.Errorf("Step %v: Expected 0 hits, got %v", step, hits.Cnt)
	}
}

func checkResets(idx int, cnt int) func(*testing.T, int, *InverseSet) {
	return func(t *testing.T, step int, sm *InverseSet) {
		t.Helper()
		if len(sm.resets[idx].resets) != cnt {
			t.Errorf(
				"Step %v: Expected %v resets on idx: %v, got %v",
				step,
				cnt,
				idx,
				len(sm.resets[idx].resets),
			)
		}
	}
}

func garbageCollect[T Matcher](clock int64) func(*testing.T, int, T) {
	return func(t *testing.T, step int, sm T) {
		t.Helper()
		sm.GarbageCollect(clock)
	}
}

func checkGCMark(mark int64) func(*testing.T, int, *InverseSet) {
	return func(t *testing.T, step int, sm *InverseSet) {
		t.Helper()
		if sm.gcMark != mark {
			t.Errorf("Step %v: Expected gcMark == %v, got %v", step, mark, sm.gcMark)
		}
	}
}
