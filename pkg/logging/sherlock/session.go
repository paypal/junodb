// -*- tab-width: 2 -*-

package sherlock

import (
	"errors"
	frontier "juno/pkg/logging/sherlock/sherlockproto"
	"time"

	"juno/third_party/forked/golang/glog"

	proto "github.com/golang/protobuf/proto"
)

// TSDB/frontier implementation for go
// Stolen from https://github.paypal.com/Python/PythonInfrastructure/blob/master/infra/contrib/frontier.py

func (m *FrontierClient) generateSessionRequest() proto.Message {
	tenant := new(string)
	*tenant = "tenant"
	cr := &frontier.ClientRequestProto{}
	id := m.id
	cr.Id = &id
	t := frontier.ReqRespTypeProto_GET_SESSION
	cr.Type = &t
	m.id++
	gs := &frontier.GetSessionReqProto{}
	cr.GetSessionRequest = gs
	pp := &frontier.PairProto{Key: tenant,
		Value: &m.tenent}
	gs.UserPair = append(gs.UserPair, pp)
	env := new(string)
	*env = "env"
	pp = &frontier.PairProto{Key: env,
		Value: &m.env}
	gs.UserPair = append(gs.UserPair, pp)
	appSvc := new(string)
	*appSvc = "app_svc"
	pp = &frontier.PairProto{Key: appSvc,
		Value: &m.appSvc}
	gs.UserPair = append(gs.UserPair, pp)
	return cr
}

func (m *FrontierClient) lockSession() {
	m.sessionMutex.Lock()
}

func (m *FrontierClient) unlockSession() {
	m.sessionMutex.Unlock()
}

func (m *FrontierClient) checkSession() bool {
	m.lockSession()
	defer m.unlockSession()
	return len(m.session) > 0
}

func (m *FrontierClient) setSession(newSession []byte) {
	m.lockSession()
	defer m.unlockSession()
	m.session = newSession
	select {
	case m.sessionChan <- true:
		// good
	case <-time.After(time.Second):
		glog.Info("Sent session to channel got a timeout")
	}
}

func (m *FrontierClient) getSession() []byte {
	m.lockSession()
	defer m.unlockSession()
	return m.session
}

func (m *FrontierClient) clearSession() {
	m.lockSession()
	defer m.unlockSession()
	m.session = []byte{}
}

func (m *FrontierClient) waitForSession() error {
	for {
		if m.checkSession() {
			return nil
		}
		select {
		case <-m.sessionChan:
			continue // then return nil if checkSession
		case <-time.After(time.Second * 60):
			return errors.New("session wait timeout")
		}
	}
}
