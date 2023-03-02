package cli

import (
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"juno/third_party/forked/golang/glog"

	junoio "juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type (
	Connection struct {
		tracker          *PendingTracker
		conn             net.Conn
		chReaderResponse <-chan *ReaderResponse
		beingRecycle     bool
	}
)

func (c *Connection) Close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Connection) CloseWrite() {
	if c.conn != nil {
		if i, ok := c.conn.(interface {
			CloseWrite() error
		}); ok {
			i.CloseWrite()
		} else {
			c.conn.Close()
		}
		c.conn = nil
	}
}

func (c *Connection) Shutdown() {
	if c.conn != nil {
		if i, ok := c.conn.(interface {
			CloseRead() error
		}); ok {
			i.CloseRead()
		} else {
			c.conn.Close()
		}
		c.conn = nil
	}
}

func (c *Connection) GetReqTimeoutCh() <-chan time.Time {
	if c.tracker == nil {
		return nil
	}
	return c.tracker.GetTimeoutCh()
}

func startResponseReader(r io.ReadCloser) <-chan *ReaderResponse {
	chReaderResponse := make(chan *ReaderResponse, 2)
	go func() {
		defer func() {
			close(chReaderResponse)
			glog.Verbosef("reader exits")
			r.Close()
		}()

		for {
			var raw proto.RawMessage
			var err error

			if _, err = raw.Read(r); err == nil {
				resp := &proto.OperationalMessage{}
				if err = resp.Decode(&raw); err == nil {
					chReaderResponse <- NewReaderResponse(resp)
				}
			}
			if err != nil {
				chReaderResponse <- NewErrorReaderResponse(err)
				if nerr, ok := err.(net.Error); ok {
					if nerr.Timeout() {
						glog.Warningln(err)
						return
					}
				}

				if opErr, ok := err.(*net.OpError); ok {
					if sErr, ok := opErr.Err.(*os.SyscallError); ok {
						if sErr.Err == syscall.ECONNRESET {
							glog.Debugln(err)
							return
						}
					}
					if opErr.Error() != "use of closed network connection" { ///READLLY hate this way
						return
					}
				}

				if err == io.EOF {
					glog.Debugln(err)
				} else {
					glog.Warningln(err)
				}
				return
			}
		}
	}()
	return chReaderResponse
}

func StartRequestProcessor(
	server junoio.ServiceEndpoint,
	sourceName string,
	connectTimeout time.Duration,
	requestTimeout time.Duration,
	connRecycleTimeout time.Duration,
	chDone <-chan bool,
	chRequest <-chan *RequestContext) (chProcessorDone <-chan bool) {

	ch := make(chan bool)
	go doRequestProcess(server, sourceName, connectTimeout, requestTimeout, connRecycleTimeout, chDone, ch, chRequest)
	return ch
}

///TODO backoff if connect fails
func doRequestProcess(
	server junoio.ServiceEndpoint,
	sourceName string,
	connectTimeout time.Duration,
	requestTimeout time.Duration,
	connRecycleTimeout time.Duration,
	chDone <-chan bool,
	chDoneNotify chan<- bool,
	chRequest <-chan *RequestContext) {

	if connRecycleTimeout != 0 {
		if connRecycleTimeout < requestTimeout+requestTimeout {
			connRecycleTimeout = requestTimeout + requestTimeout
			glog.Infof("conntion recycle timeout adjusted to be %s", connRecycleTimeout)
		}
	}
	connRecycleTimer := util.NewTimerWrapper(connRecycleTimeout)
	active := &Connection{}
	recycled := &Connection{}

	connect := func() (err error) {
		var conn net.Conn
		conn, err = junoio.Connect(&server, connectTimeout)
		if err != nil {
			return
		}
		active.conn = conn
		active.tracker = newPendingTracker(requestTimeout)
		active.chReaderResponse = startResponseReader(conn)
		if connRecycleTimeout != 0 {
			glog.Debugf("connection recycle in %s", connRecycleTimeout)
			connRecycleTimer.Reset(connRecycleTimeout)
		} else {
			glog.Debugf("connection won't be recycled")
		}
		return
	}

	var sequence uint32
	defer close(chDoneNotify)

	var err error
	connect()

loop:
	for {
		select {
		case <-chDone:
			glog.Verbosef("proc done channel got notified")
			active.Shutdown() ///TODO to revisit
			break loop
		case _, ok := <-connRecycleTimer.GetTimeoutCh():
			if ok {
				glog.Debug("connection recycle timer fired")
				connRecycleTimer.Stop()
				recycled = active
				recycled.beingRecycle = true
				active = &Connection{}
				err = connect()
				if err != nil {
					glog.Error(err)
				}
			} else {
				glog.Errorf("connection recycle timer not ok")
			}

		case now, ok := <-active.GetReqTimeoutCh():
			if ok {
				active.tracker.OnTimeout(now)
			} else {
				glog.Error("error to get from active request timeout channel")
			}
		case now, ok := <-recycled.GetReqTimeoutCh():
			if ok {
				recycled.tracker.OnTimeout(now)
				if len(recycled.tracker.pendingQueue) == 0 {
					glog.Debugf("close write for the recybled connection as it has handled all the pending request(s)")
					recycled.Shutdown()
				} else {
					glog.Debugf("being recycled request timeout")

				}
			} else {
				glog.Error("error to read from recycled request timeout channel")
			}

		case r, ok := <-chRequest:
			if !ok { // shouldn't happen as it won't be closed
				break loop
			}
			glog.Verbosef("processor got request")
			var err error

			if active.conn == nil {
				err = connect()
			}
			if err == nil {
				conn := active.conn
				saddr := conn.LocalAddr().(*net.TCPAddr)
				req := r.GetRequest()
				if req == nil {
					glog.Error("nil request")
					return
				}
				req.SetSource(saddr.IP, uint16(saddr.Port), []byte(sourceName))
				sequence++
				var raw proto.RawMessage
				if err = req.Encode(&raw); err != nil {
					glog.Errorf("encoding error %s", err) //TODO revisit. may just call panic
					return
				}
				raw.SetOpaque(sequence)

				if _, err = raw.Write(conn); err == nil {
					active.tracker.OnRequestSent(r, sequence)
				} else {
					r.ReplyError(err)
					active.Close()
				}
			} else {
				r.ReplyError(err)
			}
		case readerResp, ok := <-active.chReaderResponse:
			if ok {
				active.tracker.OnResonseReceived(readerResp)
			} else {
				glog.Debug("active reader response channel closed")
				active.tracker.OnResponseReaderClosed()
				active.Close()
				active = &Connection{}
			}
		case readerResp, ok := <-recycled.chReaderResponse:
			if ok {
				recycled.tracker.OnResonseReceived(readerResp)

			} else {
				glog.Debug("recycled reader response channel closed")
				recycled.tracker.OnResponseReaderClosed()
				recycled = &Connection{}
			}
		}
	}
}
