package raft

import "time"

type GoodTimer struct {
	t_ *time.Timer
	ready bool // whether t.C has already been read
}

func (gt *GoodTimer) NewGoodTimer(t *time.Timer) *GoodTimer {
	return &GoodTimer{t_ : t}
}

func (gt *GoodTimer) Reset(d time.Duration) {

}

func (gt *GoodTimer) Stop() bool {
	stopped := gt.t_.Stop()
	return stopped
}
