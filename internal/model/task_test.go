package model

import "testing"

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to TaskState
		want     bool
	}{
		{StatePending, StateDispatched, true},
		{StateDispatched, StateSucceeded, true},
		{StateDispatched, StateFailed, true},
		{StateFailed, StatePending, true},
		{StateFailed, StateDead, true},
		// 非法迁移
		{StatePending, StateSucceeded, true},  // 允许（零投递即取消/成功场景）
		{StateSucceeded, StatePending, false}, // 终态不可回退
		{StateDead, StatePending, false},      // 终态不可回退
		{StateDispatched, StatePending, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s->%s) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestTaskState_IsValid(t *testing.T) {
	for _, s := range []TaskState{StatePending, StateDispatched, StateSucceeded, StateFailed, StateDead} {
		if !s.IsValid() {
			t.Errorf("state %s should be valid", s)
		}
	}
	if TaskState("unknown").IsValid() {
		t.Fatal("unknown state should be invalid")
	}
}
