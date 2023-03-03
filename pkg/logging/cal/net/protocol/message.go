package protocol

import (
	"bytes"
	"fmt"
	"juno/third_party/forked/golang/glog"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Msg is a single log message.
// TODO: Document (if only by reference) the intended use
// of the fields, and which the user should set and which
// the client sets.
type CalMessage struct {
	Class     byte // ASCII character
	CreatedAt time.Time
	Type      string
	Name      string
	Status    string // TODO make this Status type
	Duration  time.Duration
	Data      []byte
}

func (m *CalMessage) String() string {
	buf := new(bytes.Buffer)

	if m.Class == TxnEnd || m.Class == AtomicTxn {
		fmt.Fprintf(buf, "%.2f\t", m.Duration.Seconds()*1000)
	}
	buf.Write(m.Data)
	return fmt.Sprintf("%c%v\t%s\t%s\t%s\t%s", m.Class, timestamp(m.CreatedAt), m.Type, m.Name, m.Status, buf)
}

func (m *CalMessage) PrettyPrintCalMessage() {
	glog.V(2).Infof("[calmsg] %s", m.String())
}

// NewMsg creates a new Msg with required fields populated.
// NewMsg automatically sets CreatedAt and records the active
// goroutine. If class is not one of the CAL class constants
// defined in this package, NewMsg will return nil.
func NewMsg(class byte, typ, name string) *CalMessage {
	switch class {
	case TxnStart, TxnEnd, AtomicTxn, Event, Heartbeat:
	default:
		return nil
	}
	m := new(CalMessage)
	m.CreatedAt = time.Now()
	m.Class = class
	m.Type = typ
	m.Name = name
	return m
}

// Txn contains a pair of events that MUST ALWAYS be sent in pairs.
// See Logger.NewTxn() for expected usage.
type Txn struct {
	Start *CalMessage
	End   *CalMessage
}

// NewTxn creates a new transaction and populates Txn.Start
// and Txn.End by calling NewMsg with the specified typ and name.
func NewTxn(typ string, name string) *Txn {
	t := new(Txn)
	t.Start = NewMsg(TxnStart, string(typ), name)
	t.End = NewMsg(TxnEnd, string(typ), name)
	return t
}

// Done completes a transaction by setting t.End. t.End
// is created with NewMsg, using the type and name from
// t.Start, and calculating t.End's duration appropriately.
func (t *Txn) Done() {
	t.End.CreatedAt = time.Now().UTC()
	t.End.Duration = t.End.CreatedAt.Sub(t.Start.CreatedAt)
}

// Encode prepares a CAL message for sending over the wire.
// Encode sanitizes Type, Name, and Status, and sets CreatedAt
// and ThreadId if they are not already set.
func (m *CalMessage) Encode() []byte {
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}

	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "%c%v\t%s\t%s", m.Class, timestamp(m.CreatedAt), CleanNamespace(m.Type), CleanNamespace(m.Name))
	hasData := (len(m.Data) > 0)
	buf.WriteByte('\t')
	if m.Status != "" {
		buf.WriteString(CleanStatus(m.Status))
	}

	if m.Duration > 0 && (m.Class == TxnEnd || m.Class == AtomicTxn) {
		buf.WriteByte('\t')
		fmt.Fprintf(buf, "%.2f", m.Duration.Seconds()*1000)
	}

	buf.WriteByte('\t')
	if hasData {
		buf.Write(m.Data)
	}
	b := buf.Bytes()
	// make room for the trailing \r\n, if needed
	if len(b)+2 > MaxMsgLen {
		b = b[:MaxMsgLen-2]
	}
	b = append(b, "\r\n"...)
	return b
}

// ParseMsg parses an encoded CAL message.
// Note that encoded messages do not include the CreatedAt
// date, only the time. ParseMsg selects the most recent
// non-future date available for the provided timestamp.
func ParseMsg(b []byte) (*CalMessage, error) {
	if len(b) < 3 {
		return nil, fmt.Errorf("too short, want at least %d bytes, have %d", 3, len(b))
	}
	if !bytes.HasSuffix(b, []byte("\r\n")) {
		return nil, fmt.Errorf("does not end in '\r\n'")
	}
	msgHdrLen := 12
	m := new(CalMessage)
	m.Class = b[msgHdrLen+0]
	s := string(b[msgHdrLen+1 : len(b)-2])
	fields := strings.SplitN(s, "\t", 6)
	if len(fields) < 3 {
		return nil, fmt.Errorf("too few tab-separated fields, have %d, min 4", len(fields))
	}

	createdAt, err := parseTimestamp(fields[0])
	if err != nil {
		return nil, err
	}
	m.CreatedAt = createdAt

	m.Type = fields[1]
	if CleanNamespace(m.Type) != m.Type {
		return nil, fmt.Errorf("Type contains disallowed characters, was %q, cleaned to %q", m.Type, CleanNamespace(m.Type))
	}

	m.Name = fields[2]
	if CleanNamespace(m.Name) != m.Name {
		return nil, fmt.Errorf("Name contains disallowed characters, was %q, cleaned to %q", m.Name, CleanNamespace(m.Name))
	}

	// CAL messages are fundamentally ambiguous. Grumble grumble bitch and moan.
	// We don't spit out ambiguous messages, but others do. :(

	// TODO: Use the class of message to disambiguate

	if len(fields) > 3 {
		m.Status = fields[3]
		if CleanStatus(m.Status) != m.Status {
			return nil, fmt.Errorf("Status contains disallowed characters, was %q, cleaned to %q", m.Status, CleanStatus(m.Status))
		}
	}
	if len(fields) > 4 {
		var dur int
		if fields[4] == "" {
			dur = 0
		} else {
			dur, err = strconv.Atoi(fields[4])
			if err != nil {
				return nil, fmt.Errorf("failed to parse duration %q: %v", fields[4], err)
			}
		}
		m.Duration = time.Duration(dur) * time.Millisecond
	}

	if len(fields) > 5 {
		m.Data = []byte(fields[5])
	}

	return m, nil
}

// Data is the data that gets sent with CAL messages.
// TODO: Better docs.
type Data map[string]interface{}

const centisecond = time.Second / 100

// timestamp formats a time in the HH:MM:SS.mm format that CAL expects.
func timestamp(t time.Time) string {
	t = t.Round(centisecond)
	h, m, s := t.Clock()
	cs := t.Nanosecond() / int(centisecond)
	return fmt.Sprintf("%02d:%02d:%02d.%02d", h, m, s, cs)
}

const (
	timestampDotOffset         = 2 + 1 + 2 + 1 + 2
	timestampCentisecondOffset = timestampDotOffset + 1
	timestampLength            = timestampCentisecondOffset + 2
	timestampLayout            = "15:04:05" // HH:MM:SS, fixed width fields
)

func parseTimestamp(s string) (time.Time, error) {
	// Assume that s is all ASCII; we're hosed anyway if it is not.
	var zero time.Time
	switch {
	case len(s) != timestampLength:
		return zero, fmt.Errorf("timestamp %q not parseable: wrong length %d, want %d", s, len(s), timestampLength)
	case s[timestampDotOffset] != '.':
		return zero, fmt.Errorf("timestamp %q not parseable: separator between HMS and centiseconds is wrong, have %c, want '.'", s, s[timestampDotOffset])
	}
	cs, err := strconv.Atoi(s[timestampCentisecondOffset:])
	if err != nil {
		return zero, fmt.Errorf("timestamp %q not parseable: failed to parse centiseconds: %v", s, err)
	}
	ts, err := time.ParseInLocation(timestampLayout, s[:timestampDotOffset], time.UTC)
	if err != nil {
		return zero, fmt.Errorf("timestamp %q not parseable: failed to parse H:M:S: %v", s, err)
	}

	// Try to guess whether this was today or yesterday, based on the assumption
	// that messages don't come from the future.
	now := time.Now().UTC().Add(time.Second) // add one second to cover centisecond rounding and clock skew
	hh, mm, ss := ts.Clock()
	t := time.Date(now.Year(), now.Month(), now.Day(), hh, mm, ss, cs*int(centisecond), now.Location())
	if t.After(now) {
		now = now.Add(-24 * time.Hour)
		t = time.Date(now.Year(), now.Month(), now.Day(), hh, mm, ss, cs*int(centisecond), now.Location())
	}
	return t, nil
}

// namespaceMap returns r if r is allowed in a CAL namespace (Type or Name)
// field, and -1 otherwise. The documentation is inconsistent
// about what is actually allowed, but it appears to be
// alphanumeric, '.', '-', '_', and ':'.
func namespaceMap(r rune) rune {
	if unicode.IsLetter(r) || unicode.IsNumber(r) {
		return r
	}
	if r == '.' || r == '-' || r == '_' || r == ':' {
		return r
	}
	return -1
}

// CleanNamespace makes s an appropriate CAL namespace (Type or Name)
// field by dropping disallowed characters, truncating it as necessary,
// and substituting namespaceUnset for "".
func CleanNamespace(s string) string {
	if s == "" {
		return "unset" // this is the cal standard
	}
	s = strings.Map(namespaceMap, s)
	if len(s) > MaxNamespaceLen {
		s = s[:MaxNamespaceLen-1] + "+"
	}
	return s
}

// CleanStatus makes s an appropriate CAL status.
func CleanStatus(s string) string {
	// TODO
	return s
}
