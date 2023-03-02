package sherlock

import (
	"context"
	"fmt"
	"juno/third_party/forked/golang/glog"
	"math/rand"
	"testing"
	"time"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/sfxclient"
)

func CreateSfxClient() *sfxclient.HTTPSink {
	client := sfxclient.NewHTTPSink()
	// modify endpoints if needed
	// client.DatapointEndpoint = "https://ingest.{REALM}.signalfx.com/v2/datapoint"
	// client.EventEndpoint = "https://ingest.{REALM}.signalfx.com/v2/event"
	// client.TraceEndpoint = "https://ingest.{REALM}.signalfx.com/v1/trace"
	// client.AuthToken = "ORG_TOKEN"

	client.DatapointEndpoint = "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/datapoint"
	client.EventEndpoint = "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/event"
	// client.TraceEndpoint = "https://ingest.{REALM}.signalfx.com/v1/trace"
	client.AuthToken = "9HM5CH5kr1_P7fiQ1HVOww"
	return client
}

var client *sfxclient.HTTPSink = CreateSfxClient()

func TestSendHelloWorld(t *testing.T) {
	//	client := CreateSfxClient()
	ctx := context.Background()
	client.AddDatapoints(ctx, []*datapoint.Datapoint{
		sfxclient.GaugeF("hello.world", nil, 1.0),
	})
	dims := make(map[string]string)
	client.AddEvents(ctx, []*event.Event{
		event.New("juno.helloworld", event.USERDEFINED, dims, time.Time{}),
	})
}

var dims map[string]string = map[string]string{
	"host": "slckvstore",
	"colo": "ccg11",
	"pool": "auth",
}

func TestCumulativeP(t *testing.T) {
	// client := CreateSfxClient()
	ctx := context.Background()

	var countThing int64 = 100
	err := client.AddDatapoints(ctx, []*datapoint.Datapoint{
		sfxclient.CumulativeP("juno.request_count", dims, &countThing),
	})
	if err != nil {
		panic("Could not send datapoints")
	}
}

func TestGauge(t *testing.T) {
	ctx := context.Background()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 300; i++ {
		fmt.Println(i)
		err := client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.Gauge("juno.cpu", dims, rand.Int63n(100)),
		})
		if err != nil {
			panic("Could not send datapoints")
		}
		time.Sleep(1 * time.Second)
	}
}

func setupSfxConfigRetry() {
	ShrLockConfig = &Config{
		Enabled:    true,
		ClientType: "sfxclient",
		// bad url
		DatapointEndpoint: "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/datapoint:10101",
		EventEndpoint:     "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/event",

		MainWriteQueueSize:  10,
		RetryWriteQueueSize: 10,
		RetryCount:          5,
		RmCount:             5,
		Timeout:             1 * time.Second,
		// This value has corelation to how frequent metric been pushed.
		// If the service are totally unreachable, it will be better match
		MaxBackoff: 2000 * time.Millisecond,
	}
}

func setupSfxConfig() {
	ShrLockConfig = &Config{
		Enabled:    true,
		ClientType: "sfxclient",

		DatapointEndpoint:   "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/datapoint",
		EventEndpoint:       "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/event",
		MainWriteQueueSize:  100,
		RetryWriteQueueSize: 100,
	}
}

func TestSfxInterface(t *testing.T) {
	setupSfxConfig()
	InitWithConfig(ShrLockConfig)
	// give sometime to establish a session
	time.Sleep(1 * time.Second)
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < 500; i++ {
		glog.Infof("---- %d ---- \n", i)
		data := make([]FrontierData, 1)
		d := FrontierData{"CPU", Gauge, rand.Float64() * 100}
		data[0] = d
		// dims["host"] = dims["host"] + strconv.FormatInt(int64(i%30), 10) + "a"
		err := SherlockClient.SendMetric(dims, data, time.Now())
		if err != nil {
			glog.Errorln(err)
		}
		// If we change to millisecond, we can see error in metric sending
		if i%10 == 0 {
			select {
			case <-time.After(time.Duration(500+rand.Int63n(500)) * time.Millisecond):
			}
		} else {
			select {
			case <-time.After(time.Duration(rand.Int63n(10)) * time.Millisecond):
			}
		}
	}
	time.Sleep(30 * time.Second)
	SherlockClient.Stop()
}

func TestSfxRetry(t *testing.T) {
	setupSfxConfigRetry()
	Initialize(ShrLockConfig)
	// give sometime to establish a session
	time.Sleep(1 * time.Second)
	for i := 0; i < 50; i++ {
		glog.Infof("-------------------------- %d ------------------\n", i)
		data := make([]FrontierData, 1)
		d := FrontierData{"CPU", Gauge, rand.Float64() * 100}
		data[0] = d
		//dims["host"] = dims["host"] + strconv.FormatInt(int64(i%30), 10) + "a"
		err := SherlockClient.SendMetric(dims, data, time.Now())
		if err != nil {
			glog.Errorln(err)
		}
		// If we change to millisecond, we can see error in metric sending
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
	SherlockClient.Stop()
	time.Sleep(5 * time.Second)
}
