package main

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type (
	RequestStat struct {
		mtx       sync.Mutex
		hist      *hdrhistogram.Histogram
		total     time.Duration
		numErrors int64
	}

	Statistics struct {
		all      RequestStat
		requests [kNumRequestTypes]RequestStat
		tmStart  time.Time
	}
	StatsData struct {
		throughput   float32
		avgLatency   time.Duration
		minLatency   time.Duration
		maxLatency   time.Duration
		p50Latency   time.Duration
		p95Latency   time.Duration
		p99Latency   time.Duration
		p9999Latency time.Duration
		numRequests  int64
	}
)

func (s *RequestStat) GetHistogram() (hgram *hdrhistogram.Histogram) {
	if s.hist == nil {
		s.Init()
	}
	return s.hist
}

func (s *RequestStat) Put(tm time.Duration, err error) {
	if s.hist == nil {
		s.Init()
	}
	s.mtx.Lock()
	s.hist.RecordValues(int64(tm), 1)
	s.total += tm
	if err != nil {
		s.numErrors++
	}

	s.mtx.Unlock()
}

func (s *RequestStat) GetStats() (stat StatsData) {
	if s.hist == nil {
		s.Init()
	}
	//	tmLen := time.Since(s.t)
	s.mtx.Lock()
	stat.numRequests = s.hist.TotalCount()
	stat.minLatency = time.Duration(s.hist.Min())
	stat.maxLatency = time.Duration(s.hist.Max())
	stat.p50Latency = time.Duration(s.hist.ValueAtQuantile(50.))
	stat.p95Latency = time.Duration(s.hist.ValueAtQuantile(95.))
	stat.p99Latency = time.Duration(s.hist.ValueAtQuantile(99.))
	stat.p9999Latency = time.Duration(s.hist.ValueAtQuantile(99.99))
	s.mtx.Unlock()

	if stat.numRequests != 0 {
		v := float32(s.total) / float32(stat.numRequests)

		stat.avgLatency = time.Duration(v)
		stat.throughput = 1.0e9 / v
	} else {
		stat.avgLatency = time.Duration(0)
		stat.throughput = 0
	}
	return
}

func (s *RequestStat) GetNumRequestPerSecond() (rt float32) {
	if s.hist == nil {
		s.Init()
	}
	s.mtx.Lock()
	if s.total == 0 {
		return 0
	}
	num := float32(s.hist.TotalCount())
	value := float32(s.total / time.Second)
	rt = num / value
	s.mtx.Unlock()
	return
}

func (s *RequestStat) GetTotalCount() (num int64) {
	if s.hist == nil {
		s.Init()
	}
	s.mtx.Lock()
	num = s.hist.TotalCount()
	s.mtx.Unlock()
	return
}
func (s *RequestStat) Init() {
	if s.hist == nil {
		s.mtx.Lock()
		s.hist = hdrhistogram.New(1, int64(3600*time.Second), 3)
		s.mtx.Unlock()
	}
}

func (s *RequestStat) Reset() {
	if s.hist == nil {
		s.Init()
	}
	s.mtx.Lock()
	s.hist.Reset()
	s.numErrors = 0
	s.total = 0
	s.mtx.Unlock()
}

func (s *Statistics) Init() {
	s.all.Init()
	for i := 0; i < int(kNumRequestTypes); i++ {
		s.requests[i].Init()
	}
	s.tmStart = time.Now()
}

func (s *Statistics) Reset() {
	s.Init()
	s.all.Reset()
	for i := 0; i < int(kNumRequestTypes); i++ {
		s.requests[i].Reset()
	}
	s.tmStart = time.Now()

}

func (s *Statistics) Put(typ RequestType, tm time.Duration, err error) {
	s.all.Put(tm, err)
	s.requests[typ].Put(tm, err)
}

func (s *Statistics) GetNumRequests() int64 {
	return s.all.GetTotalCount()
}

func (s *Statistics) PrettyPrint(w io.Writer) {
	msfunc := func(d time.Duration) time.Duration {
		return d.Round(time.Microsecond)
	}

	fmt.Fprintln(w,
		`
 request/s  |                             request latency                                              |  number of |            |              | number of
  average   | average    | min        | max        |        50% |      95%   |      99%   |     99.99% |  requests  | percentage | request type |  errors
------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+--------------+-------------`)
	wstatFunc := func(stat *StatsData, percentage float32, reqType string, numErrors int64) {
		fmt.Fprintf(w, "%12.2f %12s %12s %12s %12s %12s %12s %12s %12d %12.2f %12s %12d\n",
			stat.throughput, msfunc(stat.avgLatency), msfunc(stat.minLatency), msfunc(stat.maxLatency), msfunc(stat.p50Latency), msfunc(stat.p95Latency),
			msfunc(stat.p99Latency), msfunc(stat.p9999Latency),
			stat.numRequests,
			percentage, reqType, numErrors)
	}
	stat4all := s.all.GetStats()

	for i := 0; i < int(kNumRequestTypes); i++ {
		stat := s.requests[i].GetStats()

		if stat.numRequests != 0 {
			wstatFunc(&stat, 100.0*float32(stat.numRequests)/float32(stat4all.numRequests), RequestType(i).String(), s.requests[i].numErrors)
		}
	}
	fmt.Fprintln(w,
		"------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+--------------+-------------")
	wstatFunc(&stat4all, 100.0, "All", s.all.numErrors)
}
