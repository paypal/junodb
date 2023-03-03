package etcd

import (
//"fmt"
)

var chanForProxy chan int
var chanForSS chan int

// Watch for version change
func WatchForProxy() chan int {
	if chanForProxy != nil {
		return chanForProxy
	}

	chanForProxy = make(chan int, 2)
	if cli == nil {
		close(chanForProxy)
	} else {
		go cli.WatchEvents(TagVersion, chanForProxy)
	}

	return chanForProxy
}
