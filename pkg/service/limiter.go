package service

import ()

type (
	ILimiter interface {
		LimitReached() bool
		Throttle()
	}
)
