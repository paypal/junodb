package sec

import (
	"encoding/hex"
	"errors"
	"fmt"
	"juno/pkg/proto"
	"juno/third_party/forked/golang/glog"
	"time"

	"github.com/BurntSushi/toml"
)

var (
	ErrFailToGetEncryptionKey           = errors.New("Fail to get encryption key")
	ErrVersionMatchForEncryptionKey     = errors.New("Fail to get version encryption key")
	ErrNoEncryptionKeyFound             = errors.New("No encryption key found.")
	ErrMoreThanOneEnabledEncryptionKeys = errors.New("more than one enabled encryption keys found")
	ErrNoEnabledEncryptionKeys          = errors.New("no enabled encryption keys found")
)

type LocalFileStore struct {
	keys    [][]byte
	numKeys int
}

type localSecretsConfig struct {
	HexKeys []string `toml:"hexKeys"`
}

// Initialize a localFileStore
func initLocalFileStore(cfg *Config) (proto.IEncryptionKeyStore, error) {

	secretcfg := &localSecretsConfig{}
	if _, err := toml.DecodeFile(cfg.KeyStoreFilePath, secretcfg); err != nil {
		return nil, err
	}

	numKeys := len(secretcfg.HexKeys)
	if numKeys <= 0 {
		return nil, fmt.Errorf("No Keys Found in FileKeyStore")
	}

	ks := &LocalFileStore{
		keys:    make([][]byte, numKeys),
		numKeys: numKeys,
	}

	var err error
	for i, str := range secretcfg.HexKeys {
		ks.keys[i], err = hex.DecodeString(str)
		if err != nil {
			glog.Exitf("fail to generate keys for test encryption key store, exiting...")
		}
	}
	return ks, nil
}

func (ks *LocalFileStore) GetEncryptionKey() (key []byte, version uint32, err error) {
	version = uint32(int(time.Now().Unix()) % ks.numKeys)
	key = ks.keys[version]
	return
}

func (ks *LocalFileStore) GetDecryptionKey(version uint32) (key []byte, err error) {
	if int(version) >= ks.numKeys {
		err = ErrFailToGetEncryptionKey
		return
	}
	key = ks.keys[version]
	return
}

func (ks *LocalFileStore) NumKeys() int {
	return ks.numKeys
}
