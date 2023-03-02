// -*- tab-width: 2 -*-

package sherlock

import "fmt"
import "testing"
import "math/rand"
import "sort"
import "time"

func sleepABit() {
	sleep := int(rand.NormFloat64()*0.05 + 0.05)
	time.Sleep(time.Duration(sleep) * time.Second)
}

func testMark(ts TimeSeriesRecorder) {
	for i := 0; i <= 10000000; i++ {
		ts.MarkDistribution(rand.NormFloat64()*10+50, "now_minus_then")
		ts.MarkDistribution(rand.NormFloat64()*10+50, "now_minus_start")
	}
}

func testPercentiles(ts TimeSeriesRecorder) {
	data := ts.GetData("now_minus_then")
	percs := percentiles(defaultPercentiles,
		data)
	for i, v := range percs {
		fmt.Printf("%f percentile is %f\n", defaultPercentiles[i], v)
	}
}

func testTimedF(ts TimeSeriesRecorder) {
	for i := 0; i <= 1000; i++ {
		ts.WithSherlockTimed(sleepABit)
	}
}

func TestSortTime(t *testing.T) {
	fmt.Println("Starting", time.Now())
	d := make([]float64, 200000)
	for i := 0; i < 1200000; i++ {
		d = append(d, rand.NormFloat64()*300+900)
	}
	fmt.Println("Created", time.Now())
	sort.Float64s(d)
	if d[0] < d[1] {
		fmt.Println("Done making and sorting 1.2M floats", time.Now())
	}
	fmt.Println("Done making and sorting 1.2M floats", time.Now())
}

func TestMetric(t *testing.T) {

	timeToSleep = 10

	c, err := NewFrontierClientNormalEndpoints("PyInfra",
		"PythonInfraDev")
	if err != nil {
		t.Log("Can't connect to frontier", err)
		t.Fail()
		return
	}
	d := Dims{"host": "test", "thing1": "3"}
	ts := NewTimeSeriesRecorder(c, d)
	go func() {
		testMark(ts)
		testPercentiles(ts)
	}()
	go testTimedF(ts)
	time.Sleep(time.Duration(timeToSleep+10) * time.Second)
	ts.Stop()
	c.Stop()
}
