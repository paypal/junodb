package main

import (
	"flag"
	"fmt"
	"juno/test/testutil/mock"
	"juno/third_party/forked/golang/glog"
)

func main() {

	var port int

	config := mock.DefaultSSConfig
	flag.IntVar(&port, "p", 10070, "port")
	flag.IntVar(&config.MeanDelay, "delay", 0, "desiredMeanDelay") // in us
	flag.IntVar(&config.StdDevDelay, "delay_sd", 0, "desiredMeanDelay")
	flag.IntVar(&config.ValueSize, "size", 1024, "desiredMeanSize")
	flag.IntVar(&config.StdDevSize, "size_sd", 100, "desiredMeanSize")

	flag.Parse() // Scan the arguments list
	flag.Lookup("logtostderr").Value.Set("true")

	glog.InitLogging(config.LogLevel, " [ss] ")

	glog.Info("Starting juno mockss")
	glog.Infof("MeanDelay: %d, sdv: %d, size: %d, sdv: %d ",
		config.MeanDelay, config.StdDevDelay, config.ValueSize, config.StdDevSize)

	var listenAddr = fmt.Sprintf(":%d", uint16(port))
	config.SetListeners([]string{listenAddr})
	service := mock.NewMockStorageService(config)
	service.Run()
}
