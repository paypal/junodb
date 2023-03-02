package main

import (
	"juno/pkg/logging/cal"
	logger "juno/pkg/logging/cal"
	"juno/pkg/logging/cal/config"

	"github.com/BurntSushi/toml"

	//"juno/pkg/logging/cal/config"
	"flag"
	"fmt"

	"juno/third_party/forked/golang/glog"

	//"io"
	//	"log"
	//"juno/pkg/logging/cal/net/protocol"
	//"os"
	//	"runtime"
	//"runtime/pprof"
	//	"net/http"
	//	_ "net/http/pprof"
	"sync"
	"time"
)

type Data map[string]interface{}

var wg sync.WaitGroup

func StartCALLogger(numOfOperation int) {
	var dict map[string]interface{}
	defer wg.Done()
	var elapsed time.Duration
	for i := 0; i < numOfOperation; i++ {
		dict = make(map[string]interface{})
		dict["k1"] = i
		dict["k2"] = i + 1
		start := time.Now()
		logger.LogAtomicTransaction(logger.TxnTypeURL, "TXN NAME", logger.StatusSuccess, time.Since(start), dict)
		elapsed = time.Since(start)
	}
	fmt.Printf("CAL : Time took to finish %d event %v\n", numOfOperation, elapsed)
}

func StartGLogger(numOfOperation, level int) {
	var dict map[string]interface{}
	defer wg.Done()
	l := glog.Level(level)
	var elapsed time.Duration
	for i := 0; i < numOfOperation; i++ {
		dict = make(map[string]interface{})
		dict["k1"] = i
		dict["k2"] = i + 1
		start := time.Now()
		glog.V(l).Infof("Data to test........First : %v, Second : %d, Third : %d", dict, i, i+1)
		elapsed = time.Since(start)
	}
	glog.V(1).Infof("GLOG : Time took to finish %d event %v\n", numOfOperation, elapsed)
}

func startCalLogger(numGoRoutine, numOfOperation int) {

	start := time.Now()
	for i := 0; i < numGoRoutine; i++ {
		wg.Add(1)
		go StartCALLogger(numOfOperation)
	}
	wg.Wait()
	elapsed := time.Since(start)
	glog.V(1).Infof("CAL : Total Time took to finish all work : %v", elapsed)
}

func startGLogger(numGoRoutine, numOfOperation, level int) {

	start := time.Now()
	for i := 0; i < numGoRoutine; i++ {
		wg.Add(1)
		go StartGLogger(numOfOperation, level)
	}
	wg.Wait()
	elapsed := time.Since(start)
	glog.V(1).Infof("GLOG : Total Time took to finish all work : %v", elapsed)
}

func main() {
	//	d := time.Duration(2) * time.Nanosecond
	//	if d.Seconds() < float64(1) {
	//		fmt.Println(d)
	//	} else {
	//		fmt.Printf("%.2f\n", d.Seconds()*1000)
	//	}
	fmt.Println("Cal client started....")
	var numGoRoutine = flag.Int("threads", 10, "Number of go routine")
	var numOfOperation = flag.Int("op", 10000, "Number of operation performed by each go routine")
	var logType = flag.Int("type", 1, "Cal logger or GLOG")
	var logLevel = flag.Int("level", 2, "GLOG Level")
	var configFile = flag.String("config", "/Users/ksomani/gitClone/infracalgo/src/calconfig.toml", "configfile")
	//var cpuprofile = flag.String("cpuprofile", "cpu.prof", "write cpu profile `file`")
	//var memprofile = flag.String("memprofile", "mem.prof", "write memory profile to `file`")
	flag.Parse() // Scan the arguments list
	flag.Lookup("logtostderr").Value.Set("true")
	config.CalConfig = &config.Config{}
	if _, err := toml.DecodeFile(*configFile, config.CalConfig); err != nil {
		fmt.Println(err)
		return
	}
	cal.InitWithConfig(config.CalConfig)

	//	go func() {
	//		http.ListenAndServe("localhost:6060", nil)
	//	}()
	//	if *cpuprofile != "" {
	//		f, err := os.Create(*cpuprofile)
	//		if err != nil {
	//			log.Fatal("could not create CPU profile: ", err)
	//		}
	//		if err := pprof.StartCPUProfile(f); err != nil {
	//			log.Fatal("could not start CPU profile: ", err)
	//		}
	//		defer pprof.StopCPUProfile()
	//	}
	//config.FileName = string(*configFile)
	if *logType == 1 {
		//startCalLogger(*numGoRoutine, *numOfOperation)
		startCalLogger(*numGoRoutine, *numOfOperation)
	} else {
		startGLogger(*numGoRoutine, *numOfOperation, *logLevel)
	}

	//	if *memprofile != "" {
	//		f, err := os.Create(*memprofile)
	//		if err != nil {
	//			log.Fatal("could not create memory profile: ", err)
	//		}
	//		runtime.GC() // get up-to-date statistics
	//		if err := pprof.WriteHeapProfile(f); err != nil {
	//			log.Fatal("could not write memory profile: ", err)
	//		}
	//		f.Close()
	//	}
	//time.Sleep(100000000000)
}
