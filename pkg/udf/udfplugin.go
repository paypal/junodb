package udf

import (
	"errors"
	"os"
	"path/filepath"
	"plugin"
	"strings"

	"juno/third_party/forked/golang/glog"
)

func loadOneUDFbyName(dir string, name string) (iudf IUDF, err error) {
	p, err := plugin.Open(dir + "/" + name)
	if err != nil {
		return nil, err
	}

	getinterface, err := p.Lookup("GetUDFInterface")
	if err != nil {
		return nil, err
	}

	udf, err := getinterface.(func() (interface{}, error))()
	if err != nil {
		return nil, err
	}

	iudf, ok := udf.(IUDF)
	if !ok {
		return nil, errors.New("bad UDF")
	}
	glog.Infof("loaded one udf plugin: %s", name)
	return iudf, nil
}

func loadUDFPlugins(udfdir string, mp *UDFMap) {
	if len(udfdir) == 0 {
		return
	}

	file, err := os.Open(udfdir)

	if err != nil {
		glog.Infof("udf not exists under %s", udfdir)
		return
	}
	defer file.Close()

	list, _ := file.Readdirnames(0)
	for _, name := range list {
		// load one udf.
		if filepath.Ext(name) != ".so" {
			continue
		}

		iudf, err := loadOneUDFbyName(udfdir, name)
		if err == nil {
			pluginName := strings.TrimSuffix(name, filepath.Ext(name))
			if _, exists := (*mp)[pluginName]; exists {
				glog.Errorf("udf with same name %s already exists, ignore", pluginName)
			} else {
				(*mp)[pluginName] = iudf
			}
		}
	}
}
