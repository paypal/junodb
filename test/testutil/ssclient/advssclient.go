package ssclient

import (
	"errors"
	"net"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/etcd"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type Config struct {
	Target            string
	Namespace         string
	NumShards         uint32
	DefaultTimeToLive int
	ConnectTimeout    util.Duration //Millisecond
	ReadTimeout       util.Duration //Millisecond
	WriteTimeout      util.Duration //Millisecond
}

var defaultConfig = Config{
	Namespace:         "test",
	NumShards:         1024,
	DefaultTimeToLive: 1800,
	ConnectTimeout:    util.Duration{100 * time.Millisecond},
	ReadTimeout:       util.Duration{500 * time.Millisecond},
	WriteTimeout:      util.Duration{500 * time.Millisecond},
}

// Advanced SSClient to mock proxy in the test
type AdvSSClient struct {
	Config
	Conn net.Conn
}

func NewAdvSSClient(server string, ns string) *AdvSSClient {
	return NewAdvSSClientWithNumShards(server, ns, 1024)
}

func NewAdvSSClientWithNumShards(server string, ns string, numShards uint32) *AdvSSClient {
	client := &AdvSSClient{
		Config: Config{
			Target:            server,
			Namespace:         ns,
			NumShards:         numShards,
			DefaultTimeToLive: defaultConfig.DefaultTimeToLive,
			ConnectTimeout:    defaultConfig.ConnectTimeout,
			ReadTimeout:       defaultConfig.ReadTimeout,
			WriteTimeout:      defaultConfig.WriteTimeout,
		},
	}
	return client
}

func (c *AdvSSClient) setRequestID(m *proto.OperationalMessage) {
	m.SetNewRequestID()
}

func (c *AdvSSClient) NewRequest(op proto.OpCode, key []byte, value []byte, ttl uint32) (request *proto.OperationalMessage) {
	request = &proto.OperationalMessage{}
	var payload proto.Payload
	payload.SetWithClearValue(value)
	request.SetRequest(op, key, []byte(c.Namespace), &payload, ttl)
	request.SetShardId(util.GetPartitionId(key, c.NumShards))
	c.setRequestID(request)
	return
}

func (c *AdvSSClient) CheckConnection() (err error) {
	if c.Conn == nil {
		var e error
		if c.Conn, e = net.DialTimeout("tcp", c.Target, c.ConnectTimeout.Duration); e != nil {
			glog.Error("cannot connect: ", e)
			err = e
			return
		}
	}
	return
}

func (c *AdvSSClient) processRequest(rawRequest *proto.RawMessage, rawResponse *proto.RawMessage) (err error) {
	glog.Verboseln("processReqeust")
	if err = c.CheckConnection(); err != nil {
		return
	}

	c.Conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout.Duration))
	if _, e := rawRequest.Write(c.Conn); e != nil {
		c.Conn = nil
		err = e
		return
	}

	c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout.Duration))
	if _, e := rawResponse.Read(c.Conn); e != nil {
		c.Conn = nil
		err = e
		return
	}

	return
}

func (c *AdvSSClient) Process(m *proto.OperationalMessage) (response *proto.OperationalMessage, err error) {
	glog.Verboseln("Process")
	if err = c.CheckConnection(); err != nil {
		return
	}
	c.setMsgSource(m)
	var rawRequest proto.RawMessage

	err = m.Encode(&rawRequest)
	if err != nil {
		return
	}
	var rawResponse proto.RawMessage

	err = c.processRequest(&rawRequest, &rawResponse)
	if err != nil {
		return
	}

	response = &proto.OperationalMessage{}
	err = response.Decode(&rawResponse)
	return
}

func (c *AdvSSClient) setMsgSource(m *proto.OperationalMessage) {
	if c.Conn == nil {
		glog.Error("not connected") ///TODO set local ...
		return
	}
	addr := c.Conn.LocalAddr()
	saddr := addr.(*net.TCPAddr)
	m.SetSource(saddr.IP, uint16(saddr.Port), []byte("mytest"))
}

func (c *AdvSSClient) PrepareCreate(key []byte, value []byte, ttl uint32) (response *proto.OperationalMessage, err error) {
	request := c.NewRequest(proto.OpCodePrepareCreate, key, value, ttl)
	request.SetCreationTime(uint32(time.Now().Unix()))
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	return response, err
}

func (c *AdvSSClient) PrepareCreateWithShardId(shardId uint16, key []byte, value []byte, ttl uint32) (response *proto.OperationalMessage, err error) {
	request := c.NewRequest(proto.OpCodePrepareCreate, key, value, ttl)
	request.SetCreationTime(uint32(time.Now().Unix()))
	request.SetShardId(shardId)
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	return response, err
}

func (c *AdvSSClient) Commit(request *proto.OperationalMessage) (response *proto.OperationalMessage, err error) {
	request.SetAsRequest()
	request.SetOpCode(proto.OpCodeCommit)
	request.SetShardId(util.GetPartitionId(request.GetKey(), c.NumShards))
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	return response, err
}

func (c *AdvSSClient) CommitWithShardId(shardId uint16, request *proto.OperationalMessage) (response *proto.OperationalMessage, err error) {
	request.SetAsRequest()
	request.SetOpCode(proto.OpCodeCommit)
	request.SetShardId(shardId)
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	return response, err
}
func (c *AdvSSClient) Abort(request *proto.OperationalMessage) (response *proto.OperationalMessage, err error) {
	request.SetAsRequest()
	request.SetOpCode(proto.OpCodeAbort)
	request.SetShardId(util.GetPartitionId(request.GetKey(), c.NumShards))
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	return response, err
}

func (c *AdvSSClient) Get(key []byte) (value []byte, response *proto.OperationalMessage, err error) {
	request := c.NewRequest(proto.OpCodeRead, key, nil, 0)
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	if err == nil {
		status := response.GetOpStatus()
		if status == proto.OpStatusNoError {
			value, err = response.GetPayload().GetClearValue()
		} else {
			glog.Debugf("err: %s", status.String())
		}
	}
	return value, response, err
}

func (c *AdvSSClient) GetWithShardId(shardId uint16, key []byte) (value []byte, response *proto.OperationalMessage, err error) {

	request := c.NewRequest(proto.OpCodeRead, key, nil, 0)
	request.SetShardId(shardId)
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	if err == nil {
		status := response.GetOpStatus()
		if status == proto.OpStatusNoError {
			value, err = response.GetPayload().GetClearValue()
		} else {
			glog.Debugf("err: %s", status.String())
		}
	}
	return value, response, err
}

func (c *AdvSSClient) Clone(request *proto.OperationalMessage) (response *proto.OperationalMessage, err error) {
	request.SetAsRequest()
	request.SetOpCode(proto.OpCodeClone)
	request.SetShardId(util.GetPartitionId(request.GetKey(), c.NumShards))
	if response, err = c.Process(request); err != nil {
		glog.Debug(err)
	}

	return response, err
}

//func (c *AdvSSClient) StartRedist(zoneid uint16, nodeid uint16, nodeInfo []string, changeMap map[uint16]uint16) (err error) {
//	cfg := etcd.NewConfig("127.0.0.1:2379")
//	cli := etcd.NewEtcdClient(cfg, "testcluster")
//	if cli == nil {
//		return errors.New("failed to connect to ETCD server")
//	}
//
//	// Set new node
//	for id, node := range nodeInfo {
//		if node == "" {
//			continue
//		}
//
//		key := etcd.KeyRedistNode(int(zoneid), id)
//		err = cli.PutValue(key, node)
//		if err != nil {
//			return err
//		}
//	}
//
//	// Set change map
//	key := etcd.KeyRedist(int(zoneid), int(nodeid))
//	var value string
//	for k, v := range changeMap {
//		if value == "" {
//			value = fmt.Sprintf("%d_%d", k, v)
//		} else {
//			value = value + fmt.Sprintf(",%d_%d", k, v)
//		}
//	}
//	err = cli.PutValue(key, value)
//	if err != nil {
//		return err
//	}
//
//	key = etcd.KeyRedistEnable(int(zoneid))
//	return cli.PutValue(key, "yes")
//}

func (c *AdvSSClient) StopRedist(zoneid uint16) (err error) {
	cfg := etcd.NewConfig("127.0.0.1:2379")
	cli := etcd.NewEtcdClient(cfg, "testcluster")

	if cli == nil {
		return errors.New("failed to connect to ETCD server")
	}

	key := etcd.KeyRedistEnable(int(zoneid))
	return cli.PutValue(key, "no")
}
