package protocol

// This file contains utilities related to the CAL wire format.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	//"strings"
	"time"
)

// AddHeader returns data ready for sending to the CAL daemon
// by returning msg prepended by a CAL message header.
func AddHeader(msg []byte, threadId uint32) []byte {
	h := MsgHeader{
		Timestamp: uint32(time.Now().Unix()),
		MsgLen:    uint32(len(msg)),
	}
	if threadId > 0 {
		h.ThreadId = threadId
	}
	packet := make([]byte, MsgHeaderLen+len(msg))
	h.Encode(packet)
	copy(packet[MsgHeaderLen:], msg)
	return packet
}

// MsgHeader is a header sent before all individual CAL messages,
// including the ClientInfo sent on initial connection.
type MsgHeader struct {
	ThreadId  uint32 // active thread (or goroutine) of logging code
	Timestamp uint32 // time when the message was sent
	MsgLen    uint32 // length of entire encoded message to follow
}

// ParseMsgHeader parses b; b is assumed to have come off the wire.
// ParseMsgHeader panics if len(b) != 12.
func ParseMsgHeader(b []byte) *MsgHeader {
	if len(b) != MsgHeaderLen {
		panic("wrong header length")
	}
	var h MsgHeader
	h.ThreadId = binary.BigEndian.Uint32(b[:4])
	h.Timestamp = binary.BigEndian.Uint32(b[4:8])
	h.MsgLen = binary.BigEndian.Uint32(b[8:12])
	return &h
}

// Encode encodes h into the first MsgHeaderLen bytes
// of b. Encode panics if len(b) < MsgHeaderLen.
func (h MsgHeader) Encode(b []byte) {
	if h.ThreadId > 0 {
		binary.BigEndian.PutUint32(b[:4], h.ThreadId)
	}
	binary.BigEndian.PutUint32(b[4:8], h.Timestamp)
	binary.BigEndian.PutUint32(b[8:12], h.MsgLen)
}

// ClientInfo is info sent once by the client on initial connection
// to the server.
type ClientInfo struct {
	Service     string    // name of service
	Hostname    string    // name of the local host
	Environment string    // defaults to "PayPal"
	Label       string    // defaults to "go;***;default"
	Start       time.Time // time of first connection from client; defaults to time.Now
}

//// ParseClientInfo parses ClientInfo received off the wire.
//func ParseClientInfo(s string) (*ClientInfo, error) {
//	lines := strings.Split(s, "\r\n")
//	if len(lines) < 4 {
//		return nil, fmt.Errorf("expected at least 4 lines, got %d (%q)", len(lines), s)
//	}
//	for l := 4; l < len(lines); l++ {
//		if lines[l] != "" {
//			return nil, fmt.Errorf("expected empty trailing lines, got %q on line %d", lines[l], l)
//		}
//	}
//
//	for i, prefix := range clientInfoPrefixes {
//		if !strings.HasPrefix(lines[i], prefix) {
//			return nil, fmt.Errorf("line %d should have prefix %q, was %q", i, prefix, lines[i])
//		}
//		lines[i] = lines[i][len(prefix):]
//	}
//
//	line0Parts := strings.SplitN(lines[0], ":", 2)
//	if len(line0Parts) != 2 {
//		return nil, fmt.Errorf("line 0 should contain :, was %q", lines[0])
//	}
//
//	t, err := time.Parse(clientInfoTimeFormat, lines[3])
//	if err != nil {
//		return nil, fmt.Errorf("failed to parse start time in line 3 (%q): %v", lines[3], err)
//	}
//
//	ci := &ClientInfo{
//		Service:     line0Parts[0],
//		Hostname:    line0Parts[1],
//		Environment: lines[1],
//		Label:       lines[2],
//		Start:       t,
//	}
//	return ci, nil
//}

// Encode encodes c for sending over the wire. If
// Environment, Label, or Start are not set, Encode
// initializes them to their default values.
func (c *ClientInfo) Encode() []byte {
	if c.Environment == "" {
		c.Environment = defaultEnvironment
	}
	if c.Label == "" {
		c.Label = defaultLabel
	}
	if c.Start.IsZero() {
		c.Start = time.Now()
	}
	b := new(bytes.Buffer)
	fmt.Fprintf(b, "%s%s:%s\r\n", clientInfoPrefixes[0], c.Service, c.Hostname)
	fmt.Fprintf(b, "%s%s\r\n", clientInfoPrefixes[1], c.Environment)
	fmt.Fprintf(b, "%s%s\r\n", clientInfoPrefixes[2], c.Label)
	fmt.Fprintf(b, "%s%s\r\n", clientInfoPrefixes[3], c.Start.Format(clientInfoTimeFormat))
	return b.Bytes()
}

// Default values for ClientInfo fields.
const (
	defaultEnvironment = "PayPal"
	defaultLabel       = "go;***;default" // copied, mutatis mutandis, from Python infra
)

// Length of encoded MsgHeaders.
const MsgHeaderLen = 12

// Format of timestamps in encoded ClientInfo.
// Equivalent to Python's "%d-%m-%Y %H:%M:%S".
// Note the fixed width (zero-padded) fields.
const clientInfoTimeFormat = "01-02-2006 15:04:05"

// Exact prefixes for the lines of encoded ClientInfo.
var clientInfoPrefixes = []string{"SQLLog for ", "Environment: ", "Label: ", "Start: "}

// ServiceCallInfo represents a single node within a chain of service
// calls.  In other CAL clients, this is called PoolInfo.
type ServiceCallInfo struct {
	ServiceName      string
	OperationName    string
	ThreadId         uint32
	RootTxnStartTime *time.Time // nil or the time the root txn started
	Hostname         string     // name of the local host
}

// Chain appends this ServiceCallInfo to the parentChain of call chain
// info.  Use "" for parentChain to start a new chain.  In other CAL
// clients, the resulting string would be called a PoolStack.
func (p *ServiceCallInfo) Chain(parentChain string) string {
	current := p.Encode()
	if parentChain != "" {
		return parentChain + "^" + current
	}
	return current
}

func (p *ServiceCallInfo) Encode() string {
	start := "TopLevelTxn not set"
	if p.RootTxnStartTime != nil {
		unixMillis := p.RootTxnStartTime.UnixNano() / 1000000
		start = fmt.Sprintf("%d", unixMillis)
	}

	return fmt.Sprintf("%s:%s*CalThreadId=%v*TopLevelTxnStartTime=%s*Host=%s",
		p.ServiceName, p.OperationName, p.ThreadId, start, p.Hostname)
}
