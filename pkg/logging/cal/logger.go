package cal

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging/cal/config"
	"juno/pkg/logging/cal/net/io"
	"juno/pkg/logging/cal/net/protocol"
)

// CAL message Status field values
const (
	StatusSuccess       string = "0"
	StatusFatal         string = "1"
	StatusSystemFailure string = "1" // "system failure that lead to a failure in processing"
	StatusError         string = "2"
	StatusInputError    string = "2" // "input errors that lead to failure in processing"
	StatusWarning       string = "3"
	StatusUnknown       string = "U"
)

// some well known TxnType values, but anything is allowed
const (
	TxnTypeAPI      string = "API"  // root Txn for mid-tier service
	TxnTypeRAPI     string = "RAPI" // root Txn for mid-tier service
	TxnTypeAccept   string = "ACCEPT"
	TxnTypeURL      string = "URL"      // root Txn for web service
	TxnTypeConnect  string = "CONNECT"  // nested Txn: connecting to svc
	TxnTypeSend     string = "SEND"     // nested Txn: sending req to svc
	TxnTypeRecv     string = "RECV"     // nested Txn: reading resp from svc
	TxnTypeCall     string = "CALL"     // nested svc call (CONNECT+SEND+RECV)
	TxnTypeDispatch string = "DISPATCH" // nested Txn: handling request
	TxnTypeClose    string = "CLOSE"
)

var (
	client         io.Client
	logLevel       int = -1
	logInfoPercent float32
)

func Initialize(args ...interface{}) (err error) {
	sz := len(args)
	if sz == 0 {
		err = fmt.Errorf("config argument expected")
		glog.Error(err)
		return
	}
	var c *config.Config
	var ok bool
	if c, ok = args[0].(*config.Config); !ok {
		err = fmt.Errorf("wrong argument type")
		glog.Error(err)
		return
	}
	if len(args) > 1 && !args[1].(bool) {
		c.NumberConnections = 1
	}
	InitWithConfig(c)
	return
}

func InitWithConfig(conf *config.Config) {
	if client == nil {
		if conf != nil {
			config.CalConfig = conf
			config.CalConfig.Default()
			config.CalConfig.Validate()
			setLogLevel(config.CalConfig.LogLevel)
			setLogInfoPercent(config.CalConfig.LogInfoPercent)
			if config.CalConfig.Enabled {
				glog.Info("Initializing CAL client.")
				client = io.NewClient()
				glog.Info("CAL initialized.")
			} else {
				glog.Info("CAL disabled.")
			}
		}
	}
}

func LogAtomicTransaction(txnType, eventName, status string, duration time.Duration, eventData map[string]interface{}) {
	// TODO guard against bad eventType/eventName (perhaps in NewMsg())
	if client == nil {
		glog.V(2).Info("CAL: Client not initialized.")
		return
	}
	msg := protocol.NewMsg(protocol.AtomicTxn, txnType, eventName)
	msg.Status = status
	msg.Duration = duration
	if eventData != nil {
		var buf bytes.Buffer
		val := url.Values{}
		for k, v := range eventData {
			val.Set(k, fmt.Sprintf("%v", v))
		}
		buf.WriteString(val.Encode())
		msg.Data = buf.Bytes()
	}
	client.Send(msg)
}

//SendEvent logs an event of eventType with a sub-classification of
// eventName.  The event may optionally contain extra eventData.
func LogEvent(eventType, eventName, status string, eventData map[string]interface{}) {
	// TODO guard against bad eventType/eventName (perhaps in NewMsg())
	if client == nil {
		glog.V(2).Info("CAL: Client not initialized.")
		return
	}
	msg := protocol.NewMsg(protocol.Event, eventType, eventName)
	if eventData != nil {
		var buf bytes.Buffer
		val := url.Values{}
		for k, v := range eventData {
			val.Set(k, fmt.Sprintf("%v", v))
		}
		buf.WriteString(val.Encode())
		msg.Data = buf.Bytes()
	}
	client.Send(msg)
}

func CalClient() io.Client {
	return client
}

func IsEnabled() bool {
	return client != nil
}

func AtomicTransaction(txnType string, name string, status string, duration time.Duration, data []byte) {
	if client == nil {
		glog.V(2).Info("CAL: Client not initialized.")
		return
	}
	msg := &protocol.CalMessage{
		Class:     protocol.AtomicTxn,
		CreatedAt: time.Now(),
		Type:      txnType,
		Name:      name,
		Status:    status,
		Duration:  duration,
		Data:      data,
	}

	client.Send(msg)
}

func GetCalDropCount() uint64 {
	if client == nil {
		return 0
	}
	return client.GetCalDropCount()
}

func Event(eventType string, name string, status string, data []byte) {
	if client == nil {
		glog.V(2).Info("CAL: Client not initialized.")
		return
	}
	msg := &protocol.CalMessage{
		Class:     protocol.Event,
		CreatedAt: time.Now(),
		Type:      eventType,
		Name:      name,
		Status:    status,
		Data:      data,
	}

	client.Send(msg)
}

func StateLog(name string, data []byte) {
	if client == nil {
		glog.V(2).Info("CAL: Client not initialized.")
		return
	}
	msg := &protocol.CalMessage{
		Class:     protocol.Heartbeat,
		CreatedAt: time.Now(),
		Type:      "STATE",
		Name:      name,
		Status:    "0",
		Data:      data,
	}

	client.Send(msg)
}

func ConfigDump() {
	config.CalConfig.Dump()
}

const (
	kLogOff     = -1
	kLogError   = 0
	kLogWarning = 1
	kLogInfo    = 2
	kLogDebug   = 3
	kLogVerbose = 4
)

func setLogLevel(level string) {
	logLevel = kLogError

	s := strings.ToLower(level)
	switch {
	case s == "off":
		logLevel = kLogOff
	case s == "error":
		logLevel = kLogError
	case s == "warning":
		logLevel = kLogWarning
	case s == "info":
		logLevel = kLogInfo
	case s == "debug":
		logLevel = kLogDebug
	case s == "verbose":
		logLevel = kLogVerbose
	}
}

func setLogInfoPercent(percent float32) {
	logInfoPercent = percent
	if logInfoPercent == 0 {
		logInfoPercent = 0.1
	}
}

func LogError() bool {
	return (logLevel >= kLogError)
}

func LogWarning() bool {
	return (logLevel >= kLogWarning)
}

func LogInfo() bool {
	return (logLevel >= kLogInfo)
}

func LogInfoPercent() bool {
	if logLevel > kLogInfo {
		return true
	}
	if logLevel == kLogInfo &&
		rand.Float32() < logInfoPercent {
		return true
	}
	return false
}

func LogDebug() bool {
	return (logLevel >= kLogDebug)
}

func LogVerbose() bool {
	return (logLevel >= kLogVerbose)
}
