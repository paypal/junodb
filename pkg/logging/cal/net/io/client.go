//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
//  Package utility provides the utility interfaces for mux package
//  
package io

import (
	"bufio"
	"errors"
	"sync"

	//"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging/cal/config"
	"juno/pkg/logging/cal/net/protocol"
	"juno/pkg/logging/cal/util"
)

var logFile *os.File
var logFileWriter *bufio.Writer

var connectionTimeoutSecond int
var clientPool []*client

// NewClient creates a CAL client, using the default CAL port.
// Most users will not need NewClient, and will just use DefaultClient.
func NewClient() Client {
	if !config.CalConfig.Enabled {
		return &client{}
	}
	if config.CalConfig.NumberConnections == 0 {
		config.CalConfig.NumberConnections = 1
	}
	var wg sync.WaitGroup = sync.WaitGroup{}
	calclient := newClient(make(chan *protocol.CalMessage, config.CalConfig.MessageQueueSize), make(chan struct{}), &wg)
	connectionTimeoutSecond = int(config.CalConfig.ConnectionTimeout)
	calclient.initOnce.Do(func() {
		if strings.EqualFold(config.CalConfig.CalType, protocol.CalTypeFile) {
			createCalLogFile()
			go calclient.sendFileLoop()
		} else {
			clientPool = make([]*client, config.CalConfig.NumberConnections)
			for i := 0; i < int(config.CalConfig.NumberConnections); i++ {
				clientPool[i] = newClient(calclient.sendCh, calclient.closeCh, calclient.wg)
				err := clientPool[i].Connect()
				if err != nil {
					glog.V(1).Infof("Cal %v: Failed to connect: %v", i, err)
				}
				go clientPool[i].sendDataLoop()
			}
		}
	})

	return calclient
}

func newClient(sendChan chan *protocol.CalMessage, closeChan chan struct{}, wgIn *sync.WaitGroup) *client {
	return &client{
		connector:  NewConnector(config.CalConfig.Host, int(config.CalConfig.Port)),
		clientInfo: NewClientInfo(config.CalConfig.Poolname, config.CalConfig.Host, config.CalConfig.Label),
		sendCh:     sendChan,
		threadId:   os.Getpid(), // set thread id so that cald could have more connections to publisher
		closeCh:    closeChan,
		msgDrpCnt:  0,
		wg:         wgIn,
		//pid:        os.Getpid(),
	}
}

func createCalLogFile() {
	if len(config.CalConfig.CalLogFile) != 0 {
		logFile, err := os.OpenFile(config.CalConfig.CalLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err == nil {
			logFileWriter = bufio.NewWriter(logFile)
		} else {
			glog.V(2).Infof("Cal: not able to open log file %s error: %s", config.CalConfig.CalLogFile, err)
		}
	}
}

func NewClientInfo(poolName, hostName, label string) *protocol.ClientInfo {
	return &protocol.ClientInfo{
		Service:  util.Poolname(poolName),
		Hostname: util.Hostname(),
		Label:    label,
		Start:    time.Now(),
	}
}

func NewConnector(hostName string, p int) *Connector {
	return &Connector{
		calHost: util.CalHostname(hostName),
		calPort: util.CalPort(p),
	}
}

// connect connects to the CAL server and writes
// the initial client info message.
func (c *client) Connect() error {

	conn, err := net.DialTimeout("tcp", c.CalHost()+":"+strconv.Itoa(c.CalPort()), time.Second*time.Duration(connectionTimeoutSecond))
	if err != nil {
		glog.V(1).Info("Cal: Failed to connect: ", err)
		return err
	}

	enc := c.clientInfo.Encode()
	packet := protocol.AddHeader(enc, uint32(c.threadId))
	//conn.SetWriteDeadline(time.Now().Add(writeTTL))
	c.connector.writer = util.NewBufioWriter(conn)
	if _, err := c.connector.writer.Write(packet); err != nil {
		conn.Close()
		c.connector.writer.Reset(nil)
		return errors.New("Cal: Failed to write client info: " + err.Error())
	}
	c.connector.conn = conn
	return nil
}

// sendloop is responsible for managing the CAL connection
// and all writes to it.
func (c *client) sendDataLoop() {
	// conn is the active CAL connection.
	defer func() {
		c.Shutdown()
	}()
	var dataSent int
	// b tracks exponential backoff for connection attempts.
	// bc is the backoff time channel. It will be non-nil when
	// there has been a connection failure and we are waiting
	// to try again.
	b := Exponential(time.Second, 1.5, time.Second*5)
	var bc <-chan time.Time
	// For now, this loop runs forever. We can add termination,
	// but I don't see a need for that in the short term.
	for {
		// Wait until we can make progress.
		select {
		case <-c.closeCh:
			return
		case <-bc:
			// We were in connect backoff mode; now enough time has
			// elapsed that we can try again.
			bc = nil
		case msg, ok := <-c.sendCh:
			if !ok {
				glog.V(4).Infof("Send Data channel closed")
				return
			}

			if c.connector.conn == nil && bc == nil {
				err := c.Connect()
				if err != nil {
					glog.V(2).Infof("Cal: Failed to connect: %v", err)
					b.BackOff()
					bc = b.C()
				} else {
					b.Reset()
				}
			}
			if msg != nil {
				enc := msg.Encode()
				enc = protocol.AddHeader(enc, uint32(c.threadId))
				if c.connector.conn != nil {
					n, err := c.connector.writer.Write(enc)
					if err != nil {
						glog.V(2).Infof("Cal: error while sending message: %v", err)
						c.connector.conn.Close()
						c.connector.conn = nil
						c.connector.writer.Reset(nil)
					} else {
						dataSent = dataSent + n
					LOOP:
						for n < protocol.MaxMsgBufferSize && c.connector.conn != nil {
							select {
							case msg, ok := <-c.sendCh:
								if !ok {
									glog.V(4).Infof("Send Data channel closed")
									c.wg.Done()
									return
								}
								if msg != nil {
									enc := msg.Encode()
									enc = protocol.AddHeader(enc, uint32(c.threadId))
									dataSent, err := c.connector.writer.Write(enc)
									if err != nil {
										glog.V(2).Infof("Cal: error while sending message: %v", err)
										c.connector.conn.Close()
										c.connector.conn = nil
										c.connector.writer.Reset(nil)
									}
									n += dataSent
								}
								c.wg.Done()
							default:
								break LOOP
							}
						}
						if c.connector.writer != nil {
							c.connector.writer.Flush()
						}
						dataSent = 0
					}
				}
				c.wg.Done()
			}
		}
	}
}

func (c *client) sendFileLoop() {
	// conn is the active CAL connection.
	defer func() {
		c.Shutdown()
	}()
	for {
		select {
		case <-c.closeCh:
			return
		case msg, ok := <-c.sendCh:
			if !ok {
				glog.V(4).Infof("Send Data channel closed")
				return
			}
			if msg != nil {
				if logFileWriter == nil {
					msg.PrettyPrintCalMessage()
				} else {
					logFileWriter.WriteString(msg.String())
					logFileWriter.WriteByte('\n')
					logFileWriter.Flush()
				}
				c.wg.Done()
			}
		}
	}
}

//
// Sender need to make should not send message after Shutdown()
//
func (c *client) Send(m *protocol.CalMessage) {
	if m == nil {
		glog.V(2).Info("Cal: Message can not be nil.")
		return
	}

	if c == nil {
		glog.V(2).Info("Cal: cannot send message using nil Client")
		return
	}
	c.wg.Add(1)
	select {
	case c.sendCh <- m:
	default:
		atomic.AddUint64(&c.msgDrpCnt, 1)
		glog.V(4).Infof("Cal: dropped message : %v", m)
		c.wg.Done()
	}
}

//If you close client connection you wouldnt be able to log anything using logger.
// This should be called when parent process Shutdown.
func (c *client) Shutdown() {
	c.closeOnce.Do(func() {
		c.wg.Wait()
		close(c.sendCh)
		close(c.closeCh)
		if logFile != nil {
			if logFileWriter != nil {
				logFileWriter.Flush()
			}
			logFile.Close()
		}
		for i := range clientPool {
			if clientPool[i].connector.writer != nil {
				clientPool[i].connector.writer.Reset(nil)
			}
			if clientPool[i].connector.conn != nil {
				clientPool[i].connector.conn.Close()
				clientPool[i].connector.writer.Reset(nil)
			}
		}
	})
}

//Flush blocks till the messages are processed by the channel
func (c *client) Flush() {
	c.wg.Wait()
	if logFile != nil {
		if logFileWriter != nil {
			logFileWriter.Flush()
		}
	}
}
