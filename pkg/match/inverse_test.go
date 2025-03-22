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

func matchLines(lines ...string) func(*testing.T, int, Hits) {
	return matchLinesN(1, lines...)
}

func matchLinesN(cnt int, lines ...string) func(*testing.T, int, Hits) {
	return func(t *testing.T, step int, hits Hits) {
		t.Helper()
		if cnt != hits.Cnt {
			t.Errorf("Step %v: Expected %v hits, got %v", step, cnt, hits.Cnt)
			return
		}

		for i, line := range lines {
			if hits.Logs[i].Line != line {
				t.Errorf("Step %v: Expected %v, got %v on index %v", step, line, hits.Logs[i].Line, i)
			}
		}
	}
}

func checkActive[T any](nActive int) func(*testing.T, int, *T) {
	return func(t *testing.T, step int, sm *T) {
		t.Helper()
		var active int
		switch v := any(sm).(type) {
		case *MatchSeq:
			active = v.nActive
		case *InverseSeq:
			active = v.nActive
		default:
			panic("Invalid type")
		}

		if active != nActive {
			t.Errorf("Step %v: Expected nActive == %v, got %v", step, active, nActive)
		}
	}
}

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

func checkNoFire(t *testing.T, step int, hits Hits) {
	t.Helper()
	if hits.Cnt != 0 {
		t.Errorf("Step %v: Expected 0 hits, got %v", step, hits.Cnt)
	}
}

func checkResets[T any](idx int, cnt int) func(*testing.T, int, *T) {
	return func(t *testing.T, step int, sm *T) {
		t.Helper()
		var resetCnt int
		switch v := any(sm).(type) {
		case *InverseSet:
			resetCnt = len(v.resets[idx].resets)
		case *InverseSeq:
			resetCnt = len(v.resets[idx].resets)
		default:
			panic("Invalid type")
		}

		if resetCnt != cnt {
			t.Errorf(
				"Step %v: Expected %v resets on idx: %v, got %v",
				step,
				cnt,
				idx,
				resetCnt,
			)
		}
	}
}

func checkGCMark[T any](mark int64) func(*testing.T, int, *T) {
	return func(t *testing.T, step int, sm *T) {
		t.Helper()
		var gcMark int64
		switch v := any(sm).(type) {
		case *InverseSet:
			gcMark = v.gcMark
		case *InverseSeq:
			gcMark = v.gcMark
		default:
			panic("Invalid type")
		}

		if gcMark != mark {
			t.Errorf("Step %v: Expected gcMark == %v, got %v", step, mark, gcMark)
		}
	}
}

func checkEval[T Matcher](clock int64, cb func(*testing.T, int, Hits)) func(*testing.T, int, T) {
	return func(t *testing.T, step int, sm T) {
		t.Helper()
		hits := sm.Eval(clock)
		cb(t, step, hits)
	}
}

func garbageCollect[T Matcher](clock int64) func(*testing.T, int, T) {
	return func(t *testing.T, step int, sm T) {
		t.Helper()
		sm.GarbageCollect(clock)
	}
}
