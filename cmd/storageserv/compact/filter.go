package compact

import (
	"bytes"

	"github.com/BurntSushi/toml"

	"juno/third_party/forked/golang/glog"
)

type namespaceFilter struct {
}

type EventConfig struct {
	Type   string
	Delete []NSEntry
}

type NSEntry struct {
	Namespace string
	Prefix    []string
}

type PrefixList [][]byte

var (
	matchCount = uint64(0)
	nsMap      map[string]PrefixList
)

func NewEventConfig(file string) *EventConfig {

	var event EventConfig
	_, err := toml.DecodeFile(file, &event)
	if err != nil {
		glog.Errorf("[ERROR] Bad format in %s: %s", file, err)
		return nil
	}

	for _, val := range event.Delete {
		if len(val.Namespace) == 0 {
			glog.Errorf("[ERROR] Namespace field cannot be empty in %s.", file)
			return nil
		}
	}

	return &event
}

func EncodeEventConfig(buf *bytes.Buffer, event *EventConfig) error {
	err := toml.NewEncoder(buf).Encode(*event)
	return err
}

func initFilter(event *EventConfig) {

	nsMap = make(map[string]PrefixList, 10)

	for _, entry := range event.Delete {
		list, found := nsMap[entry.Namespace]
		if !found {
			list = make(PrefixList, 0, 5)
			nsMap[entry.Namespace] = list
		}

		for _, prefix := range entry.Prefix {
			list = append(list, []byte(prefix))
		}
		nsMap[entry.Namespace] = list
	}
	matchCount = 0
}

func getMatchCount() uint64 {
	return matchCount
}

func (e *EventConfig) isNamespace() bool {
	return len(e.Delete) > 0
}

func (m *namespaceFilter) Filter(level int, key, val []byte) (exp bool,
	newVal []byte) {

	if matchNamespace(key) {
		return true, nil // remove record
	}

	// keep the record
	return false, nil
}

func (m *namespaceFilter) Name() string {
	return "NamespaceFilter"
}

func matchNamespace(key []byte) bool {

	const offNamespace = 4
	if len(nsMap) == 0 || len(key) < offNamespace+2 {
		return false
	}
	// key[offNamespace:stop] stores namespace
	stop := offNamespace + int(uint8(key[offNamespace-1]))
	if len(key) < int(stop) {
		return false
	}

	prefixList, found := nsMap[string(key[offNamespace:stop])]
	if !found {
		return false
	}

	if len(prefixList) == 0 {
		matchCount++
		return true
	}

	w := len(key[stop:])
	for i := range prefixList {
		w2 := len(prefixList[i])
		if w < w2 ||
			bytes.Compare(key[stop:stop+w2], []byte(prefixList[i])) != 0 {
			continue
		}

		matchCount++
		if matchCount%100000 == 0 {
			glog.Infof("ns_keys=%d", matchCount)
		}

		return true
	}

	return false
}
