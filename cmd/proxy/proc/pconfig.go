package proc

import (
	"time"

    "juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/pkg/cfg"
	"juno/pkg/io"
	"juno/pkg/proto"
)

var (
	confNumWrites                    int
	confNumZones                     int
	confSSRequestTimeout             time.Duration
	confMaxNumFailures               int
	confMaxKeyLength                 int
	confMaxNamespaceLength           int
	confMaxPayloadLength             int
	confMaxTimeToLive                int
	confDefaultTimeToLive            uint32
	confTwoPhaseDestroyEnabled       bool
	confEncryptionEnabled            bool
	confReplicationEncryptionEnabled bool
	confMaxRecordVersion             uint32
)

func InitConfig() {
	confNumWrites = int(config.Conf.GetNumWrites())
	confNumZones = int(config.Conf.ClusterInfo.NumZones)
	confSSRequestTimeout = config.Conf.ReqProc.SSReqTimeout.Duration
	confMaxNumFailures = confNumZones - confNumWrites
	confMaxKeyLength = config.Conf.MaxKeyLength
	confMaxNamespaceLength = config.Conf.MaxNamespaceLength
	confMaxPayloadLength = config.Conf.MaxPayloadLength
	confMaxTimeToLive = config.Conf.MaxTimeToLive
	confDefaultTimeToLive = uint32(config.Conf.DefaultTimeToLive)
	confTwoPhaseDestroyEnabled = config.Conf.TwoPhaseDestroyEnabled
	confEncryptionEnabled = config.Conf.PayloadEncryptionEnabled
	confReplicationEncryptionEnabled = config.Conf.ReplicationEncryptionEnabled
	confMaxRecordVersion = config.Conf.MaxRecordVersion

	storedcfg, err := readStoredLimits()
	if err == nil && storedcfg != nil {
		config.SetLimitsConfig(storedcfg)
	}
}

func readStoredLimits() (conf *cfg.Config, err error) {
	chResponse := make(chan io.IResponseContext)
	ctx := &io.InboundRequestContext{}
	ctx.SetResponseChannel(chResponse)
	ctx.SetTimeout(nil, time.Second*1)
	var opmsg proto.OperationalMessage

	opmsg.SetNamespace([]byte(config.JunoInternalNamespace()))
	opmsg.SetKey([]byte(config.JunoInternalKeyForLimits()))
	opmsg.SetOpCode(proto.OpCodeGet)
	opmsg.SetAsRequest()
	opmsg.SetNewRequestID()
	raw := ctx.GetMessage()
	if err = opmsg.Encode(raw); err != nil {
		return
	}
	processor := NewGetProcessor()
	processor.Init()
	go processor.Process(ctx)
	select {
	case <-ctx.GetCtx().Done():
		err = ctx.GetCtx().Err()
	case resp := <-chResponse:
		var ropmsg proto.OperationalMessage

		if err = ropmsg.Decode(resp.GetMessage()); err != nil {
			glog.Error(err)
			return
		} else {
			if st := ropmsg.GetOpStatus(); st == proto.OpStatusNoError {
				pl := ropmsg.GetPayload()
				if pl != nil {
					var b []byte

					if b, err = pl.GetClearValue(); err == nil {
						conf = &cfg.Config{}
						err = conf.ReadFromTomlBytes(b)
					}
				}
			} else {
				glog.Info("No stored LimitsConfig found")
			}
		}
	}
	return
}

func UpdateLimitsConfig(tm int64) {
	if config.IsLimitsConfigBefore(tm) {
		storedcfg, err := readStoredLimits()
		if err == nil && storedcfg != nil {
			config.SetLimitsConfig(storedcfg)
		}
	}
}
