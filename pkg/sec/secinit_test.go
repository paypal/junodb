package sec

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
)

func loadCfg() (*Config, error) {
	cfg := &Config{}
	if _, err := toml.DecodeFile("./config.toml", cfg); err != nil {
		fmt.Println(err)
		return nil, err
	}
	return cfg, nil
}

func initCfg() error {
	cfg := &Config{}
	if _, err := toml.DecodeFile("./config.toml", cfg); err != nil {
		fmt.Println(err)
		return err
	}
	if err := InitSecConfig(cfg); err != nil { // ####
		return err
	}
	return nil
}

func loadFileCfg(file string) (*Config, error) {
	cfg := &Config{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		fmt.Println(err)
		return nil, err
	}
	return cfg, nil
}

func Test_initializeSec(t *testing.T) {
	type args struct {
		cfg             *Config
		flag            Flag
		isServerManager bool
	}

	testcfg, err := loadCfg()
	if err != nil {
		panic(err)
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"GenSecretsInit", args{testcfg, Flag(KFlagServerTlsEnabled | KFlagClientTlsEnabled), true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := initializeSec(tt.args.cfg, tt.args.flag, tt.args.isServerManager); (err != nil) != tt.wantErr {
				t.Errorf("initializeSec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
