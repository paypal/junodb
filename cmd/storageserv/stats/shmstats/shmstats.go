package shmstats

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unsafe"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/pkg/stats"
)

const (
	kTimeFormat = "15:04:05 01/02/2006"
)

var (
	shmStats ShmStatsManager
)

type (
	ReqProcStats struct {
		NumRequests       uint64
		RequestsPerSecond uint32
		AvgReqProcTime    uint32

		NumReads       uint64 //use fixed size array?
		NumDeletes     uint64
		NumCommits     uint64
		NumAborts      uint64
		NumRepairs     uint64
		NumMarkDeletes uint64
		ProcCpuUsage   float32
		MachCpuUsage   float32
	}
	StorageStats struct {
		Free                uint64 // in Megabytes
		Used                uint64 // in Megabytes
		NumKeys             uint64
		MaxDBLevel          uint32
		CompSecByInterval   uint32
		CompCountByInterval uint32
		PendingCompKBytes   uint64
		DelayedWriteRate    uint64
	}
	WorkerStats struct {
		Pid              uint32
		Port             uint16
		MonPort          uint16
		StartTimestampNs int64
		ZoneId           uint32
		MachineIndex     uint32
		LastExitCode     int32
		NumStarts        uint16
		ReqProcStats
		StorageStats
		InboundConnStats
	}
	ServerStats struct {
		StartTimestampNs int64
		NumShards        uint32
		NumZones         uint32
		NumWorkers       uint32
		Pid              uint32
	}
	InboundConnStats struct {
		NumConnections  uint32
		MaxNumConntions uint32
	}
	workerStatsManagerT struct {
		stats *WorkerStats
	}
	ShmStatsManager struct { //TODO add a header
		stats.SharedStats
		serverStats *ServerStats
		workers     []workerStatsManagerT
		current     *workerStatsManagerT
	}
)

func shmName(pid int) string {
	return fmt.Sprintf("/juno.storageserv.%d", pid)
}

func numBytesToPadTo8(size int) int {
	return (8 - size%8) % 8
}

func sizeOfServerStats() int {
	sz := int(unsafe.Sizeof(ServerStats{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func sizeOfWorkerStats() int {
	sz := int(unsafe.Sizeof(WorkerStats{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func (s *ServerStats) PrettyPrint(w io.Writer) {
	fmt.Fprintf(w, "Storageserv Statistics\n\tStartTime\t: %s\n\tNumShards\t: %d\n\tNumZones\t: %d\n\tNumWorkers\t: %d\n\tPID\t: %d\n",
		time.Unix(0, s.StartTimestampNs).Format(kTimeFormat),
		s.NumShards,
		s.NumZones,
		s.NumWorkers,
		s.Pid)
}

func (s *workerStatsManagerT) size() int {
	sz := sizeOfWorkerStats()
	//
	return sz
}

func (s *workerStatsManagerT) PrettyPrint(w io.Writer) {
	if st := s.stats; st != nil {
		fmt.Fprintf(w, "\tStartTime\t: %s\n", time.Unix(0, st.StartTimestampNs).Format(kTimeFormat))
		fmt.Fprintf(w, "\tPID\t: %d\n", st.Pid)
		fmt.Fprintf(w, "\tPort\t: %d\n", st.Port)
		fmt.Fprintf(w, "\tMonPort\t: %d\n", st.MonPort)
		fmt.Fprintf(w, "\tNumStarts\t: %d\n", st.NumStarts)
		fmt.Fprintf(w, "\tLastExitCode\t: %d\n", st.LastExitCode)
		fmt.Fprintf(w, "\tNumRequests\t: %d\n", st.NumRequests)
		fmt.Fprintf(w, "\tRequestsPerSecond\t: %d\n", st.RequestsPerSecond)
		fmt.Fprintf(w, "\tAvgReqProcTime\t: %d\n", st.AvgReqProcTime)

		fmt.Fprintf(w, "\tNumReads\t: %d\n", st.NumReads)
		fmt.Fprintf(w, "\tNumDeletes\t: %d\n", st.NumDeletes)
		fmt.Fprintf(w, "\tNumCommits\t: %d\n", st.NumCommits)
		fmt.Fprintf(w, "\tNumAborts\t: %d\n", st.NumAborts)
		fmt.Fprintf(w, "\tNumRepairs\t: %d\n", st.NumRepairs)
		fmt.Fprintf(w, "\tNumMarkDeletes\t: %d\n", st.NumMarkDeletes)
		fmt.Fprintf(w, "\tStorageFree\t: %d\n", st.Free)
		fmt.Fprintf(w, "\tStorageUsed\t: %d\n", st.Used)
		fmt.Fprintf(w, "\tNumConnections\t: %d\n", st.NumConnections)
		fmt.Fprintf(w, "\tMaxNumConnections\t: %d\n", st.MaxNumConntions)
	}
}

func (s *workerStatsManagerT) mapShmData(b []byte) error {
	if len(b) < s.size() {
		return fmt.Errorf("invalid buffer")
	}
	s.stats = (*WorkerStats)(unsafe.Pointer(&b[0]))
	return nil
}

func (m *workerStatsManagerT) GetWorkerStats() (stats WorkerStats) {
	if m.stats != nil {
		stats = *m.stats
	}
	return
}

func (m *workerStatsManagerT) GetWorkerStatsPtr() *WorkerStats {
	return m.stats
}

func (m *workerStatsManagerT) SetReqProcStats(src *ReqProcStats) {
	if m.stats != nil && src != nil {
		m.stats.ReqProcStats = *src
	}
}

func (m *workerStatsManagerT) SetStorageStats(src *StorageStats) {
	if m.stats != nil && src != nil {
		m.stats.StorageStats = *src
	}
}

func (m *workerStatsManagerT) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.stats)
}

func (s *ShmStatsManager) initMembers(numWorker int) {
	s.workers = make([]workerStatsManagerT, numWorker)
}

func (s *ShmStatsManager) calculateSize() (size int) {
	size = sizeOfServerStats()

	for i, _ := range s.workers {
		size += s.workers[i].size()
	}
	size += numBytesToPadTo8(size)
	return
}

func (s *ShmStatsManager) createAndMapShm(name string, numWorker int) (err error) {
	glog.Debugf("create %s\n", name)

	s.initMembers(numWorker)

	size := s.calculateSize()

	if err = s.Create(name, size); err != nil {
		return
	}

	if err = s.mapShmData(); err != nil {
		return
	}
	return
}

func (s *ShmStatsManager) openForReadWriteAndMapShm(name string) (err error) {
	if err = s.Open(name); err != nil {
		return
	}
	if err = s.mapShmData(); err != nil {
		return
	}
	return
}

func (s *ShmStatsManager) openForReadAndMapShm(name string) (err error) {
	if err = s.OpenForRead(name); err != nil {
		return
	}
	if err = s.mapShmData(); err != nil {
		return
	}
	return
}

func (s *ShmStatsManager) mapShmData() error {
	b := s.GetMappedData()
	off := sizeOfServerStats()

	if len(b) < off {
		return fmt.Errorf("mapped data too short")
	}
	s.serverStats = (*ServerStats)(unsafe.Pointer(&b[0]))

	if len(s.workers) == 0 {
		s.workers = make([]workerStatsManagerT, s.serverStats.NumWorkers)
	}
	for i, _ := range s.workers {
		if err := s.workers[i].mapShmData(b[off:]); err != nil {
			return err
		}
		off += s.workers[i].size()
	}
	return nil
}

func (s *ShmStatsManager) InitForManager(numChildren int) (err error) {
	pid := os.Getpid()

	if err = s.createAndMapShm(shmName(pid), numChildren); err != nil {
		return
	}
	cfg := config.ServerConfig()

	s.serverStats.NumShards = cfg.ClusterInfo.NumShards
	s.serverStats.StartTimestampNs = time.Now().UnixNano()
	s.serverStats.NumZones = cfg.ClusterInfo.NumZones
	s.serverStats.NumWorkers = uint32(numChildren)
	s.serverStats.Pid = uint32(pid)
	return
}

func (s *ShmStatsManager) InitForWorker(isChild bool, workerId int, zoneId uint32, machineIndex uint32) (err error) {
	cfg := config.ServerConfig()
	if cfg == nil {
		err = fmt.Errorf("nil config")
		return
	}
	if len(cfg.Listener) == 0 {
		err = fmt.Errorf("listener not specified")
		return
	}
	if isChild {
		if err = s.openForReadWriteAndMapShm(shmName(os.Getppid())); err != nil {
			return
		}
		if workerId >= int(s.serverStats.NumWorkers) {
			err = fmt.Errorf("invalid workerId: %d  (numChildren: %d)", workerId, s.serverStats.NumWorkers)
			return
		}
		s.current = &s.workers[workerId]
	} else {
		if err = s.createAndMapShm(shmName(os.Getpid()), 1); err != nil {
			return
		}
		s.current = &s.workers[0]
		s.serverStats.StartTimestampNs = time.Now().UnixNano()
		s.serverStats.NumShards = cfg.ClusterInfo.NumShards
		s.serverStats.NumZones = cfg.ClusterInfo.NumZones
		s.serverStats.NumWorkers = 1
		s.serverStats.Pid = uint32(os.Getpid())
		var port int
		if _, portstr, e := net.SplitHostPort(cfg.HttpMonAddr); e == nil {
			if port, err = strconv.Atoi(portstr); err != nil {
				return
			}
		} else {
			err = e
			return
		}
		s.current.stats.MonPort = uint16(port)
	}

	s.current.stats.Pid = uint32(os.Getpid())
	s.current.stats.StartTimestampNs = time.Now().UnixNano()
	s.current.stats.ZoneId = zoneId
	s.current.stats.MachineIndex = machineIndex
	s.current.stats.NumStarts++
	var port int
	if _, portstr, e := net.SplitHostPort(cfg.Listener[0].Addr); e == nil {
		if port, err = strconv.Atoi(portstr); err != nil {
			return
		}
	} else {
		err = e
		return
	}
	s.current.stats.Port = uint16(port)

	return
}

func (s *ShmStatsManager) writeStatsInJson(w io.Writer, workerId string, indent bool) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	if b, err := json.Marshal(&s.serverStats); err == nil {
		fmt.Fprintf(&buf, `"Server":%s`, string(b))
	} else {
		fmt.Fprintln(w, err)
		return
	}
	if strings.ToLower(workerId) == "all" {
		buf.WriteString(`,"Worker":[`)
		for i, _ := range s.workers {
			if i != 0 {
				buf.WriteByte(',')
			}
			if b, err := json.Marshal(&s.workers[i]); err == nil {
				fmt.Fprintf(&buf, string(b))
			} else {
				fmt.Fprintln(w, err)
				return
			}
		}
		buf.WriteByte(']')
	} else {
		if id, err := strconv.Atoi(workerId); err == nil {
			fmt.Fprintf(&buf, `,"Worker_%d":[`, id)
			if id < len(s.workers) {
				if b, err := json.Marshal(&s.workers[id]); err == nil {
					fmt.Fprintf(&buf, string(b))
				} else {
					fmt.Fprintln(w, err)
					return
				}
			}
			buf.WriteByte(']')
		}
	}
	buf.WriteByte('}')
	if indent {
		var b bytes.Buffer
		json.Indent(&b, buf.Bytes(), "", "  ")
		w.Write(b.Bytes())
	} else {
		w.Write(buf.Bytes())
	}
}

func (s *ShmStatsManager) PrettyPrint(iw io.Writer, workerId string) {
	w := tabwriter.NewWriter(iw, 0, 8, 2, ' ', 0)
	s.serverStats.PrettyPrint(w)

	if workerId == "*" || workerId == "all" {
		for i, _ := range s.workers {
			fmt.Fprintf(w, "Worker %d\n", i)
			s.workers[i].PrettyPrint(w)
		}
	} else {
		if id, err := strconv.Atoi(workerId); err == nil {
			if id < len(s.workers) {
				fmt.Fprintf(w, "Worker_%d\n", id)
				s.workers[id].PrettyPrint(w)
			}
		}
	}
	w.Flush()
}

func (s *ShmStatsManager) SetMonPorts(addrs []string) {
	//change ownership
	sz := len(addrs)
	if sz == int(s.serverStats.NumWorkers) {
		for i := 0; i < sz; i++ {
			if _, portstr, err := net.SplitHostPort(addrs[i]); err == nil {
				if port, err := strconv.Atoi(portstr); err == nil {
					s.workers[i].stats.MonPort = uint16(port)
				}
			}
		}
	}
}

func InitForManager(numChildren int) (err error) {
	return shmStats.InitForManager(numChildren)
}

func InitForMonitor() (err error) {
	return shmStats.openForReadAndMapShm(shmName(os.Getppid()))
}

func InitForWorker(isChild bool, workerId int, zoneId uint32, machineIndex uint32) error {
	return shmStats.InitForWorker(isChild, workerId, zoneId, machineIndex)
}

func InitForRead(pid int) (err error) {
	return shmStats.openForReadAndMapShm(shmName(pid))
}

func Finalize() {
	shmStats.Finalize()
}

func GetServerStats() (stats ServerStats) {
	if shmStats.serverStats != nil {
		stats = *shmStats.serverStats
		if shmStats.current != nil {
			stats.StartTimestampNs = shmStats.current.stats.StartTimestampNs
			stats.Pid = shmStats.current.stats.Pid
		}
	}
	return
}

func GetCurrentWorkerStatsManager() *workerStatsManagerT {
	return shmStats.current
}

func GetWorkerStatsManager(workerId int) *workerStatsManagerT {
	if workerId < len(shmStats.workers) {
		return &shmStats.workers[workerId]
	}
	return nil
}

func WriteStatsInJson(w io.Writer, workerId string, indent bool) {
	shmStats.writeStatsInJson(w, workerId, indent)
}

func PrettyPrint(w io.Writer, workerId string) {
	shmStats.PrettyPrint(w, workerId)
}

func SetMonPorts(addrs []string) {
	shmStats.SetMonPorts(addrs)
}
