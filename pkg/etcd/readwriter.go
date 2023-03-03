package etcd

type EtcdReadWriter struct {
	EtcdReader
	EtcdWriter
}

// for clustermgr
func NewEtcdReadWriter(cli *EtcdClient) *EtcdReadWriter {
	rw := &EtcdReadWriter{
		EtcdReader: EtcdReader{
			etcdcli: cli,
		},
		EtcdWriter: EtcdWriter{
			etcdcli: cli,
		},
	}

	// self for polymorphism to work
	rw.kvwriter = rw
	return rw
}

// for clustermgr dryrun
type EtcdReadStdoutWriter struct {
	EtcdReader
	StdoutWriter
}

func NewEtcdReadStdoutWriter(cli *EtcdClient, clusterName string) *EtcdReadStdoutWriter {
	rw := &EtcdReadStdoutWriter{
		EtcdReader: EtcdReader{
			etcdcli: cli,
		},
		StdoutWriter: StdoutWriter{
			keyPrefix: cli.config.EtcdKeyPrefix + clusterName + TagCompDelimiter,
		},
	}

	// self for polymorphism to work
	rw.kvwriter = rw
	return rw
}
