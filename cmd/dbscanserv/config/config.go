package config

type DbScan struct {
	ListenPort            int
	ReplicationAddr       string
	ReplicationNamespaces string
	PatchDbName           string
	PatchTTL              int
	Debug                 bool
}
