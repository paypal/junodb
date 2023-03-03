package logging

import ()

const (
	CalMsgTypeDb             string = "DB"
	CalMsgTypePing           string = "PING"
	CalMsgTypeReplicate      string = "Replicate"
	CalMsgTypeJunoSec        string = "JunoSec"
	CalMsgTypeSSConnect      string = "SSCONNECT"
	CalMsgTypeSSConnectError string = "SSCONNECT_FAIL"
	CalMsgTypeWorker         string = "Worker"
	CalMsgTypeManager        string = "Manager"
	CalMsgTypeRidMapping     string = "MAP_RID"
	CalMsgTypeInit           string = "JunoInit"
)

const (
	CalMsgNameDbPut              string = "Put"
	CalMsgNameDbGet              string = "Get"
	CalMsgNameStart              string = "Start"
	CalMsgNameExit               string = "Exit"
	CalMsgNameInbound            string = "In"
	CalMsgNameInboundReplication string = "RIn"
	CalMsgNameOutbound           string = "Out"
	CalMsgNameGetEncrypKey       string = "GetKey"
)
