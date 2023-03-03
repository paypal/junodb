package main

import (
	"juno/pkg/client"
	"juno/pkg/logging/cal/config"
	"juno/pkg/sec"
)

type (
	Config struct {
		client.Config

		Sec sec.Config
		Cal config.Config

		PayloadLen     int
		TimeToLive     int
		RequestPattern string
		HttpMonAddr    string

		NumExecutor     int
		NumReqPerSecond int
		RunningTime     int
		StatOutputRate  int
		isVariable      bool
		disableGetTTL   bool
	}
)
