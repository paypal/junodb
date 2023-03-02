package sec

import (
	"io/ioutil"
	"juno/third_party/forked/golang/glog"
	"os"
)

type localFileProtectedT struct {
	CertPem string
	KeyPem  string
}

func (p *localFileProtectedT) getCertAndKeyPemBlock(cfg *Config) (certPEMBlock []byte, keyPEMBlock []byte, err error) {
	certPEMBlock, err = ioutil.ReadFile(cfg.CertPemFilePath)
	if err != nil {
		return
	}

	if cfg.ClientAuth {
		if _, err = os.Stat(cfg.CAFilePath); err == nil {
			var caPEMBlock []byte
			caPEMBlock, err = ioutil.ReadFile(cfg.CAFilePath)
			if err != nil {
				glog.Errorln(err)
				return
			}
			str := string(certPEMBlock)
			str += string(caPEMBlock)
			certPEMBlock = []byte(str)
		} else {
			glog.Infof("os.Stat(cfg.CAFilePath) returns %v for filePath: %v", err, cfg.CAFilePath)
		}
	}

	keyPEMBlock, err = ioutil.ReadFile(cfg.KeyPemFilePath)
	if err != nil {
		return
	}
	return
}
