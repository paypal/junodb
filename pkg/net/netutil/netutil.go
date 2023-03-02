package netutil

import (
	"net"

	"juno/third_party/forked/golang/glog"
)

var (
	localIPMap       map[string]bool = make(map[string]bool)
	localIPv4Address net.IP
)

func init() {
	if addrs, err := net.InterfaceAddrs(); err == nil {

		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				if localIPv4Address == nil {
					if !ipnet.IP.IsLoopback() {
						localIPv4Address = ipnet.IP.To4()
					}
				}
				localIPMap[ipnet.IP.String()] = true
			}
		}
	} else {
		glog.Warningln(err)
	}
	if localIPv4Address == nil {
		localIPv4Address = net.ParseIP("127.0.0.1").To4()
	}
}

func IsLocalAddress(addr string) bool {
	if net.ParseIP(addr) != nil {
		return IsLocalIPAddress(addr)
	}

	if ips, err := net.LookupIP(addr); err == nil {
		for _, ip := range ips {
			if IsLocalIPAddress(ip.String()) {
				return true
			}
		}
	}
	return false
}

func IsLocalIPAddress(ipAddr string) bool {
	if _, found := localIPMap[ipAddr]; found {
		return true
	}
	return false
}

func GetLocalIPv4Address() net.IP {
	return localIPv4Address
}
