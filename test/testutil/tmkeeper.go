package testutil

import (
	"time"

	"juno/third_party/forked/golang/glog"
)

type timeKeeper struct {
	startTime time.Time
}

func NewTimeKeeper() *timeKeeper {
	t := &timeKeeper{startTime: time.Now()}
	return t
}

func (t *timeKeeper) SecondPassed() uint32 {
	return uint32(time.Now().Sub(t.startTime).Seconds())
}

func (t *timeKeeper) RemainingTTL(ttl uint32) uint32 {
	l := t.SecondPassed()
	if l > ttl {
		glog.Warningf("** TTL (%d)  for this test environment might be too short **", ttl)
		return 0
	}
	return ttl - l
}
