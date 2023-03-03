package shmstats

import (
	"bytes"
	"encoding/json"
	"fmt"
	goio "io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"text/tabwriter"
	"time"
	"unsafe"

	//	"github.com/BurntSushi/toml"
	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/pkg/io"
	"juno/pkg/stats"
	"juno/pkg/util"
)

const (
	kTimeFormat                   = "15:04:05 01/02/2006"
	kNumReservedAppNamespaceStats = 200
)

var (
	shmStats ShmStatsManager
)

type (
	ReqProcStats struct {
		NumRequests          uint64
		NumReads             uint32 // outstanding read
		NumWrites            uint32 // outstanding write
		RequestsPerSecond    uint32 //tps
		AvgReqProcTime       uint32 //apt
		ReqProcErrsPerSecond uint32 //eps
		NumBadShards         uint16
		NumWarnShards        uint16
		NumAlertShards       uint16
		ProcCpuUsage         float32
		MachCpuUsage         float32
	}
	WorkerStats struct {
		Pid                 uint32
		MonPort             uint32
		NumAppNsStats       uint16
		NumStarts           uint16
		StartTimestampNs    int64
		LastExitCode        int32
		TotalNumConnections uint32 //total number of active connections accepted by all the listeners
		ReqProcStats
		CalDropCount uint64
	}
	ListenerStats struct {
		Port uint16
		Type uint16
	}
	ServerStats struct {
		NumShards             uint32
		NumZones              uint32
		NumWorkers            uint32
		Pid                   uint32
		NumListeners          uint16
		MonPort               uint16
		NumRepTargets         uint16
		NumReservedAppNsStats uint16
		StartTimestampNs      int64
	}
	ReplicationTargetStats struct {
		LenAddr  uint8
		LenName  uint8
		Port     uint16
		Type     uint16
		CapQueue uint16
		Addr     [256]byte
		Name     [12]byte
	}
	serverStatsManagerT struct {
		stats       *ServerStats
		listeners   []*ListenerStats
		replicators []*ReplicationTargetStats
	}
	InboundConnStats struct {
		NumConnections  uint32
		MaxNumConntions uint32
	}
	ReplicatorStats struct {
		NumConnections uint16
		SzQueue        uint16
		MaxSzQueue     uint16
		NumDrops       uint64
		NumErrors      uint64
	}
	StatsByAppNamespace struct {
		stats.AppNamespaceStats
		szAppNsKey uint16
		appNsKey   [512]byte
	}
	workerStatsManagerT struct {
		stats     *WorkerStats
		connStats []*InboundConnStats
		repStats  []*ReplicatorStats
		statsByNs []*StatsByAppNamespace
	}
	ShmStatsManager struct {
		stats.SharedStats
		server          serverStatsManagerT
		workers         []workerStatsManagerT
		current         *workerStatsManagerT
		currentWorkerId int
	}
)

func shmName(pid int) string {
	return fmt.Sprintf("/juno.proxy.%d", pid)
}

func numBytesToPadTo8(size int) int {
	return (8 - size%8) % 8
}

func sizeOfServerStats() int {
	sz := int(unsafe.Sizeof(ServerStats{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func sizeOfListenerStats() int {
	szLt := int(unsafe.Sizeof(ListenerStats{}))
	szLt += numBytesToPadTo8(szLt)
	return szLt
}

func sizeOfReplicationTargetStats() int {
	sz := int(unsafe.Sizeof(ReplicationTargetStats{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func sizeOfWorkerStats() int {
	sz := int(unsafe.Sizeof(WorkerStats{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func sizeOfInboundConnStats() int {
	sz := int(unsafe.Sizeof(InboundConnStats{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func sizeOfReplicatorStats() int {
	sz := int(unsafe.Sizeof(ReplicatorStats{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func sizeOfStatsByAppNamespace() int {
	sz := int(unsafe.Sizeof(StatsByAppNamespace{}))
	sz += numBytesToPadTo8(sz)
	return sz
}

func (s *ReqProcStats) PrettyPrint(w goio.Writer) {
	fmt.Fprintf(w, "\tNumRequests\t: %d\n", s.NumRequests)
	fmt.Fprintf(w, "\tNumReads\t: %d\n", s.NumReads)
	fmt.Fprintf(w, "\tNumWrites\t: %d\n", s.NumWrites)
	fmt.Fprintf(w, "\tRequestsPerSecond\t: %d\n", s.RequestsPerSecond)
	fmt.Fprintf(w, "\tAvgReqProcTime\t: %s\n", time.Duration(s.AvgReqProcTime*1000).String())
	fmt.Fprintf(w, "\tReqProcErrsPerSecond\t: %d\n", s.ReqProcErrsPerSecond)
	fmt.Fprintf(w, "\tNumBadShards\t: %d\n", s.NumBadShards)
	fmt.Fprintf(w, "\tNumWarnShards\t: %d\n", s.NumWarnShards)
	fmt.Fprintf(w, "\tNumAlertShards\t: %d\n", s.NumAlertShards)
}

func (s *WorkerStats) PrettyPrint(w goio.Writer) {
	fmt.Fprintf(w, "\tStartTime\t: %s\n\tProcessId\t: %d\n\tMonPort\t: %d\n\tNumStarts\t: %d\n\tLastExitCode\t: %d\n",
		time.Unix(0, s.StartTimestampNs).Format(kTimeFormat),
		s.Pid,
		s.MonPort, s.NumStarts, s.LastExitCode)
	s.ReqProcStats.PrettyPrint(w)
}

func (s *ListenerStats) GetListenAddress() (addr string) {
	if s.Type != 0 {
		addr = fmt.Sprintf("ssl:%d", s.Port)
	} else {
		addr = fmt.Sprintf(":%d", s.Port)
	}
	return
}

func (s *ReplicationTargetStats) GetListenAddress() (addr string) {

	if s.Type != 0 {
		addr = fmt.Sprintf("%s:%s:ssl:%d", string(s.Name[:s.LenName]), string(s.Addr[:s.LenAddr]), s.Port)
	} else {
		addr = fmt.Sprintf("%s:%s:%d", string(s.Name[:s.LenName]), string(s.Addr[:s.LenAddr]), s.Port)
	}
	return
}

func (m *serverStatsManagerT) initMembers(numListeners int, numReplicationTargets int) {
	m.listeners = make([]*ListenerStats, numListeners)
	m.replicators = make([]*ReplicationTargetStats, numReplicationTargets)
}

func (m *serverStatsManagerT) size() int {
	return sizeOfServerStats() + sizeOfListenerStats()*len(m.listeners) + sizeOfReplicationTargetStats()*len(m.replicators)
}

func (m *serverStatsManagerT) mapShmData(b []byte, byShmCreator bool) error {
	off := sizeOfServerStats()
	if len(b) < off {
		return fmt.Errorf("mapped data too short")
	}
	m.stats = (*ServerStats)(unsafe.Pointer(&b[0]))

	if byShmCreator == false {
		m.listeners = make([]*ListenerStats, m.stats.NumListeners)
		m.replicators = make([]*ReplicationTargetStats, m.stats.NumRepTargets)
	}

	szLt := sizeOfListenerStats()

	for i, _ := range m.listeners {
		m.listeners[i] = (*ListenerStats)(unsafe.Pointer(&b[off]))
		off += szLt
	}
	szRep := sizeOfReplicationTargetStats()

	for i, _ := range m.replicators {
		m.replicators[i] = (*ReplicationTargetStats)(unsafe.Pointer(&b[off]))
		off += szRep
	}
	return nil
}

func (m *serverStatsManagerT) PrettyPrint(w goio.Writer) {
	if s := m.stats; s != nil {
		fmt.Fprintf(w, "Proxy Statistics\n\tStartTime\t: %s\n\tNumShards\t: %d\n\tNumZones\t: %d\n\tNumWorkers\t: %d\n\tPID\t: %d\n",
			time.Unix(0, s.StartTimestampNs).Format(kTimeFormat),
			s.NumShards,
			s.NumZones,
			s.NumWorkers,
			s.Pid)
		if s.NumListeners != 0 {
			var addrs []string
			for _, l := range m.listeners {
				addrs = append(addrs, l.GetListenAddress())
			}
			fmt.Fprintf(w, "\tListenOn\t: [%s]\n", strings.Join(addrs, ","))
		}
		if s.NumRepTargets != 0 {
			var addrs []string
			for _, r := range m.replicators {
				addrs = append(addrs, r.GetListenAddress())
			}
			fmt.Fprintf(w, "\tReplicateTo\t: [%s]\n", strings.Join(addrs, ","))
		}
	}
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

func (m *workerStatsManagerT) GetReqProcStats() (stats ReqProcStats) {
	if m.stats != nil {
		stats = m.stats.ReqProcStats
	}
	return
}

func (m *workerStatsManagerT) SetCalDropCount(cnt uint64) {
	if m.stats != nil {
		m.stats.CalDropCount = cnt
	}
}

func (m *workerStatsManagerT) SetReqProcStats(stats *ReqProcStats) {
	if stats != nil && m.stats != nil {
		m.stats.ReqProcStats = *stats
	}
}

func (m *workerStatsManagerT) GetAppNsStatsMap() (stmap map[string]map[string]stats.AppNamespaceStats) {
	n := int(m.stats.NumAppNsStats)
	if n == 0 {
		return nil
	}
	stmap = make(map[string]map[string]stats.AppNamespaceStats)
	for i := 0; i < n; i++ {
		stat := m.statsByNs[i]
		str := strings.Split(string(stat.appNsKey[:stat.szAppNsKey]), ".")
		if len(str) == 2 {
			if _, ok := stmap[str[1]]; !ok {
				stmap[str[1]] = make(map[string]stats.AppNamespaceStats)
			}
			stmap[str[1]][str[0]] = stat.AppNamespaceStats
		}
	}
	return
}

func (m *workerStatsManagerT) SetAppNsStats(index uint32, appNsKey []byte, stat *stats.AppNamespaceStats) {
	if stat != nil && m.stats != nil {
		if index < kNumReservedAppNamespaceStats {
			shst := m.statsByNs[index]

			if index >= uint32(m.stats.NumAppNsStats) {
				m.stats.NumAppNsStats = uint16(index + 1)
			}
			if shst.szAppNsKey == 0 {
				shst.szAppNsKey = uint16(len(appNsKey))
				copy(shst.appNsKey[:], appNsKey)
			} else {
				//TODO check app the same?
			}
			m.statsByNs[index].AppNamespaceStats = *stat
		}
	}
}

func (m *workerStatsManagerT) GetReplicatorStats() (stats []ReplicatorStats) {
	if m.stats != nil {
		sz := len(m.repStats)
		stats = make([]ReplicatorStats, sz)
		for i := 0; i < sz; i++ {
			stats[i] = *m.repStats[i]
		}
	}
	return
}

func (m *workerStatsManagerT) GetReplicatorStatsPtr(targetId int) *ReplicatorStats {
	if m.stats == nil || targetId >= len(m.repStats) {
		return nil
	}
	return m.repStats[targetId]
}

func (m *workerStatsManagerT) GetReplicatorDropCounter(targetId int) *util.AtomicShareCounter {
	if m.stats == nil || targetId >= len(m.repStats) {
		return nil
	}
	return util.NewAtomicShareCounter(&m.repStats[targetId].NumDrops)
}

func (m *workerStatsManagerT) GetReplicatorErrorCounter(targetId int) *util.AtomicShareCounter {
	if m.stats == nil || targetId >= len(m.repStats) {
		return nil
	}
	return util.NewAtomicShareCounter(&m.repStats[targetId].NumErrors)
}

// Note we don't need to set NumDrops & NumErrors in SetReplicatorStats
// as they are incremented directly by the replicators.
func (m *workerStatsManagerT) SetReplicatorStats(targetId int, numConns uint16, queueLen uint16) {
	sz := len(m.repStats)
	if targetId < sz {
		if m.repStats[targetId] != nil {
			if st := m.repStats[targetId]; st != nil {
				st.NumConnections = numConns
				st.SzQueue = queueLen
				if queueLen > st.MaxSzQueue {
					st.MaxSzQueue = queueLen
				}
			}
		}
	}
}

func (m *workerStatsManagerT) GetInboundConnStats() (stats []InboundConnStats) {
	if m.stats != nil {
		sz := len(m.connStats)
		stats = make([]InboundConnStats, sz)
		for i := 0; i < sz; i++ {
			stats[i] = *m.connStats[i]
		}
	}
	return
}

func (m *workerStatsManagerT) GetInboundConnStatsPtr() []*InboundConnStats {
	return m.connStats
}

func (m *workerStatsManagerT) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	if s := m.stats; s != nil {
		fmt.Fprintf(&buf, `"StartTime":%d,"PID":%d,"MonPort":%d,`, s.StartTimestampNs, s.Pid, s.MonPort)
		fmt.Fprintf(&buf, `"NumStarts":%d,"LastExitCode":%d,`, s.NumStarts, s.LastExitCode)
		fmt.Fprintf(&buf, `"NumRequests":%d,"NumReads":%d,"NumWrites":%d,`,
			s.NumRequests, s.NumReads, s.NumWrites)
		fmt.Fprintf(&buf, `"RequestsPerSecond":%d,"AvgReqProcTime":%d,"ReqProcErrsPerSecond":%d,`,
			s.RequestsPerSecond, s.AvgReqProcTime, s.ReqProcErrsPerSecond)
		fmt.Fprintf(&buf, `"NumBadShards":%d,"NumWarnShards":%d,"NumAlertShards":%d`,
			s.NumBadShards, s.NumWarnShards, s.NumAlertShards)
	}
	buf.WriteString(`,"ConnStats":[`)
	for i, _ := range m.connStats {
		if i != 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"NumActiveConns":%d,"MaxNumConns":%d}`, m.connStats[i].NumConnections, m.connStats[i].MaxNumConntions)
	}
	buf.WriteByte(']')
	buf.WriteString(`,"RepStats":[`)
	for i, _ := range m.repStats {
		if i != 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"QueueSize":%d,"MaxQueueSize":%d}`, m.repStats[i].SzQueue, m.repStats[i].MaxSzQueue)
	}
	buf.WriteByte(']')
	buf.WriteString(`,"AppNsStats":[`)

	n := int(m.stats.NumAppNsStats)
	if n != 0 {
		for i := 0; i < n; i++ {
			d := m.statsByNs[i]
			if i != 0 {
				fmt.Fprint(&buf, ",")
			}
			fmt.Fprintf(&buf, `{"AppNs":"%s","MaxPayload":%d,"AvgPayload":%d,"MaxTTL":%d,"AvgTTL":%d}`,
				string(d.appNsKey[:d.szAppNsKey]), d.MaxPayloadLen, d.AvgPayloadLen, d.MaxTimeToLive, d.AvgTimeToLive)
		}
	}
	buf.WriteByte(']')
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (m *workerStatsManagerT) SetInboundConnStats(listeners []io.IListener) {
	sz := len(m.connStats)
	if sz != 0 && len(listeners) == sz {
		var total uint32
		for i := 0; i < sz; i++ {
			num := listeners[i].GetNumActiveConnections()
			m.connStats[i].NumConnections = num
			if m.connStats[i].MaxNumConntions < num {
				m.connStats[i].MaxNumConntions = num
			}
			total += num
		}
		//child worker will read TotalNumConnections as well
		atomic.StoreUint32(&m.stats.TotalNumConnections, total)
	}
}

func (m *workerStatsManagerT) initMembers(numListeners int, numRepTargets int) {
	m.connStats = make([]*InboundConnStats, numListeners)
	m.repStats = make([]*ReplicatorStats, numRepTargets)
	m.statsByNs = make([]*StatsByAppNamespace, kNumReservedAppNamespaceStats)
}

func (m *workerStatsManagerT) size() int {
	szStats := sizeOfWorkerStats()
	szLt := sizeOfInboundConnStats()

	numLsnr := len(m.connStats)
	szRep := sizeOfReplicatorStats()

	numRepTargets := len(m.repStats)

	szStatsByNs := sizeOfStatsByAppNamespace()

	sz := szStats + numLsnr*szLt + numRepTargets*szRep + szStatsByNs*kNumReservedAppNamespaceStats

	return sz
}
func (s *serverStatsManagerT) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	if stats := s.stats; stats != nil {
		fmt.Fprintf(&buf, `"StartTime":%d,"PID":%d,"NumListeners":%d,"NumShards":%d,"NumZones":%d,"NumWorkers":%d,"MonPort":%d,"NumRepTgts":%d`,
			stats.StartTimestampNs, stats.Pid, stats.NumListeners, stats.NumShards, stats.NumZones, stats.NumWorkers, stats.MonPort, stats.NumRepTargets)
		buf.WriteString(`,"Listener":[`)
		for i, _ := range s.listeners {
			if i != 0 {
				buf.WriteByte(',')
			}
			fmt.Fprintf(&buf, `"%s"`, s.listeners[i].GetListenAddress())
		}
		buf.WriteByte(']')
		buf.WriteString(`,"Replicator":[`)
		for i, _ := range s.replicators {
			if i != 0 {
				buf.WriteByte(',')
			}
			fmt.Fprintf(&buf, `"%s"`, s.listeners[i].GetListenAddress())
		}
		buf.WriteByte(']')
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (m *workerStatsManagerT) mapShmData(b []byte) error {
	m.stats = (*WorkerStats)(unsafe.Pointer(&b[0]))

	szStats := sizeOfWorkerStats()
	off := szStats
	szLt := sizeOfInboundConnStats()

	for i, _ := range m.connStats {
		m.connStats[i] = (*InboundConnStats)(unsafe.Pointer(&b[off]))
		off += szLt
	}

	szRep := sizeOfReplicatorStats()

	for i, _ := range m.repStats {
		m.repStats[i] = (*ReplicatorStats)(unsafe.Pointer(&b[off]))
		off += szRep
	}
	szNsStats := sizeOfStatsByAppNamespace()
	for i, _ := range m.statsByNs {
		m.statsByNs[i] = (*StatsByAppNamespace)(unsafe.Pointer(&b[off]))
		off += szNsStats
	}
	return nil
}

func (s *ShmStatsManager) initMembers(numListeners int, numRepTargets int, numWorkers int) {
	s.server.initMembers(numListeners, numRepTargets)
	s.workers = make([]workerStatsManagerT, numWorkers)
	for i := 0; i < numWorkers; i++ {
		s.workers[i].initMembers(numListeners, numRepTargets)
	}
}

func (s *ShmStatsManager) calculateSize() int {
	sz := s.server.size()
	for i, _ := range s.workers {
		sz += s.workers[i].size()
	}
	return sz
}

func (s *ShmStatsManager) mapShmData(byCreator bool) error {
	b := s.GetMappedData()
	if err := s.server.mapShmData(b, byCreator); err != nil {
		return err
	}
	off := s.server.size()

	srvStats := s.server.stats
	if srvStats == nil {
		return fmt.Errorf("shm invalid")
	}
	if byCreator == false {
		s.workers = make([]workerStatsManagerT, int(srvStats.NumWorkers))
		for i, _ := range s.workers {
			s.workers[i].initMembers(int(srvStats.NumListeners), int(srvStats.NumRepTargets))
		}
	} else {
		//setting them later?
		srvStats.NumWorkers = uint32(len(s.workers))
		srvStats.NumListeners = uint16(len(s.server.listeners))
		srvStats.NumReservedAppNsStats = kNumReservedAppNamespaceStats
		srvStats.NumRepTargets = uint16(len(s.server.replicators))
	}
	for i, _ := range s.workers {
		if err := s.workers[i].mapShmData(b[off:]); err != nil {
			return err
		}
		off += s.workers[i].size()
	}
	return nil
}

func (s *ShmStatsManager) createAndMapShm(name string, numListeners int, numRepTargets int, numWorker int) (err error) {
	glog.Debugf("create %s\n", name)

	s.initMembers(numListeners, numRepTargets, numWorker)

	size := s.calculateSize()

	if err = s.Create(name, size); err != nil {
		return
	}

	if err = s.mapShmData(true); err != nil {
		return
	}
	return
}

func (s *ShmStatsManager) openForReadWriteAndMapShm(name string) (err error) {
	if err = s.Open(name); err != nil {
		return
	}
	if err = s.mapShmData(false); err != nil {
		return
	}
	return
}

func (s *ShmStatsManager) openForReadAndMapShm(name string) (err error) {
	if err = s.OpenForRead(name); err != nil {
		return
	}
	if err = s.mapShmData(false); err != nil {
		return
	}
	return
}

func (s *ShmStatsManager) setServerStats(pid int) (err error) {
	cfg := &config.Conf
	numListeners := len(cfg.Listener)

	for i := 0; i < numListeners; i++ {
		lsnr := s.server.listeners[i]
		if _, port, err := net.SplitHostPort(cfg.Listener[i].Addr); err == nil {
			if p, err := strconv.Atoi(port); err == nil {
				lsnr.Port = uint16(p)
			} else {
				return err
			}
		} else {
			return err
		}
		if cfg.Listener[i].SSLEnabled {
			lsnr.Type = 1
		}
	}
	numRep := len(cfg.Replication.Targets)
	for i := 0; i < numRep; i++ {
		r := cfg.Replication.Targets[i]
		repTgt := s.server.replicators[i]

		if host, port, err := net.SplitHostPort(r.Addr); err == nil {
			szHost := len(host)
			repTgt.LenAddr = uint8(szHost)
			copy(repTgt.Addr[:szHost], []byte(host))
			if p, err := strconv.Atoi(port); err == nil {
				repTgt.Port = uint16(p)
			} else {
				return err
			}
			szName := len(r.Name)
			///check length
			repTgt.LenName = uint8(szName)
			copy(repTgt.Name[:szName], []byte(r.Name))
			if r.SSLEnabled {
				repTgt.Type = 1
			}
			repTgt.CapQueue = uint16(cfg.Replication.GetIoConfig(&r).ReqChanBufSize)
		} else {
			return err
		}
	}
	s.server.stats.StartTimestampNs = time.Now().UnixNano()
	s.server.stats.NumShards = cfg.ClusterInfo.NumShards
	s.server.stats.NumZones = cfg.ClusterInfo.NumZones
	s.server.stats.NumWorkers = uint32(cfg.NumChildren)
	s.server.stats.Pid = uint32(pid)
	s.server.stats.NumListeners = uint16(numListeners)
	s.server.stats.NumReservedAppNsStats = kNumReservedAppNamespaceStats
	s.server.stats.NumRepTargets = uint16(len(cfg.Replication.Targets))
	return
}

func (s *ShmStatsManager) InitForManager() (err error) {
	pid := os.Getpid()
	cfg := &config.Conf

	if err = s.createAndMapShm(shmName(pid), len(cfg.Listener), len(cfg.Replication.Targets), cfg.NumChildren); err != nil {
		return
	}
	err = s.setServerStats(pid)

	return
}

func (s *ShmStatsManager) InitForWorker(isChild bool, workerId int) (err error) {
	if isChild {
		if err = s.openForReadWriteAndMapShm(shmName(os.Getppid())); err != nil {
			return
		}
		numWorkers := int(s.server.stats.NumWorkers)
		if workerId >= numWorkers {
			err = fmt.Errorf("invalid workerId: %d  (numChildren: %d)", workerId, numWorkers)
			return
		}
		s.current = &s.workers[workerId]
		s.currentWorkerId = workerId
	} else {
		cfg := &config.Conf
		if err = s.createAndMapShm(shmName(os.Getpid()), len(cfg.Listener), len(cfg.Replication.Targets), 1); err != nil {
			return
		}
		if err = s.setServerStats(os.Getpid()); err != nil {
			return
		}
		s.server.stats.NumWorkers = 1
		s.current = &s.workers[0]
		s.currentWorkerId = 0
	}
	s.current.stats.Pid = uint32(os.Getpid())
	s.current.stats.StartTimestampNs = time.Now().UnixNano()
	s.current.stats.NumStarts++
	return
}

func (s *ShmStatsManager) writeStatsInJson(w goio.Writer, workerId string, indent bool) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	if b, err := json.Marshal(&s.server); err == nil {
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
			if id < len(s.workers) {
				fmt.Fprintf(&buf, `,"Worker_%d":[`, id)
				if b, err := json.Marshal(&s.workers[id]); err == nil {
					fmt.Fprintf(&buf, string(b))
				} else {
					fmt.Fprintln(w, err)
					return
				}
				buf.WriteByte(']')
			}
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

func (s *ShmStatsManager) PrettyPrint(iw goio.Writer, workerId string) {
	w := tabwriter.NewWriter(iw, 0, 8, 2, ' ', 0)
	s.server.PrettyPrint(w)
	lsnrs := s.server.listeners
	nLsnr := len(lsnrs)
	nRepTgt := int(s.server.stats.NumRepTargets)
	aggStats := s.GetAggregatedReqProcStats()
	aggStats.PrettyPrint(w)
	conns := s.GetAggregatedInboundConnStats()
	if nLsnr == len(conns) {
		for j := 0; j < nLsnr; j++ {
			fmt.Fprintf(w, "\tNumActiveConns[%s]\t: %d\n", lsnrs[j].GetListenAddress(), conns[j].NumConnections)
			fmt.Fprintf(w, "\tMaxNumConns[%s]\t: %d\n", lsnrs[j].GetListenAddress(), conns[j].MaxNumConntions)
		}
	}
	writeWorkerStats := func(worker *workerStatsManagerT) {
		worker.stats.PrettyPrint(w)
		conns := worker.connStats
		tgts := worker.repStats
		if nLsnr == len(conns) {
			for j := 0; j < nLsnr; j++ {
				fmt.Fprintf(w, "\tNumActiveConns[%s]\t: %d\n", lsnrs[j].GetListenAddress(), conns[j].NumConnections)
				fmt.Fprintf(w, "\tMaxNumConns[%s]\t: %d\n", lsnrs[j].GetListenAddress(), conns[j].MaxNumConntions)
			}
		}
		for j := 0; j < nRepTgt; j++ {
			fmt.Fprintf(w, "\tQueueSizeRepTarget_%d\t: %d\n", j, tgts[j].SzQueue)
			fmt.Fprintf(w, "\tMaxQueueSizeRepTarget_%d\t: %d\n", j, tgts[j].MaxSzQueue)
		}
		for j := 0; j < int(worker.stats.NumAppNsStats); j++ {
			d := worker.statsByNs[j]

			fmt.Fprintf(w, "\tAppnameNamespace\t: %s\n", string(d.appNsKey[:d.szAppNsKey]))
			fmt.Fprintf(w, "\t  MaxPayloadLength\t: %d\n", d.MaxPayloadLen)
			fmt.Fprintf(w, "\t  AveragePayloadLength\t: %d\n", d.AvgPayloadLen)
			fmt.Fprintf(w, "\t  MaxTTL\t: %d\n", d.MaxTimeToLive)
			fmt.Fprintf(w, "\t  AverageTTL\t: %d\n", d.AvgTimeToLive)
		}
	}
	if strings.ToLower(workerId) == "all" {

		for i, _ := range s.workers {
			fmt.Fprintf(w, "Worker %d\n", i)
			writeWorkerStats(&s.workers[i])
		}
	} else {
		if id, err := strconv.Atoi(workerId); err == nil {
			if id < len(s.workers) {
				fmt.Fprintf(w, "Worker_%d\n", id)
				writeWorkerStats(&s.workers[id])
			}
		}
	}

	w.Flush()
}

func (m *ShmStatsManager) GetAggregatedReqProcStats() (s ReqProcStats) {
	if numWorkers := len(m.workers); numWorkers != 0 {
		var totalProcTime uint32
		for i := 0; i < numWorkers; i++ {
			var ws ReqProcStats
			ws = m.workers[i].stats.ReqProcStats
			s.NumRequests += ws.NumRequests
			s.RequestsPerSecond += ws.RequestsPerSecond
			totalProcTime += ws.RequestsPerSecond * ws.AvgReqProcTime
			s.ReqProcErrsPerSecond += ws.ReqProcErrsPerSecond
		}
		s.AvgReqProcTime = uint32(float32(totalProcTime) / float32(s.RequestsPerSecond))
	}
	return
}

func (m *ShmStatsManager) GetAggregatedInboundConnStats() (stats []InboundConnStats) {
	if numWorkers := len(m.workers); numWorkers != 0 {
		sz := len(m.workers[0].connStats)
		stats = make([]InboundConnStats, sz)
		for i := 0; i < numWorkers; i++ {
			for j := 0; j < sz; j++ {
				sl := shmStats.workers[i].connStats[j]
				stats[j].NumConnections += sl.NumConnections
				stats[j].MaxNumConntions += sl.MaxNumConntions
			}
		}
	}
	return
}

func (m *ShmStatsManager) Finalize() {
	m.current = nil
	m.SharedStats.Finalize()
}

func SetHttpPort(addr string) {
	if _, portstr, err := net.SplitHostPort(addr); err == nil {
		if port, err := strconv.Atoi(portstr); err == nil {
			shmStats.current.stats.MonPort = uint32(port)
		}
	}
}

func InitForManager() (err error) {
	return shmStats.InitForManager()
}

func InitForWorker(isChild bool, workerId int) (err error) {
	return shmStats.InitForWorker(isChild, workerId)
}

func Finalize() {
	shmStats.Finalize()
}

func InitForMonitor() (err error) {
	return shmStats.openForReadAndMapShm(shmName(os.Getppid()))
}

func InitForRead(pid int) (err error) {
	return shmStats.openForReadAndMapShm(shmName(pid))
}

func WriteStatsInJson(w goio.Writer, workerId string, indent bool) {
	shmStats.writeStatsInJson(w, workerId, indent)
}

func PrettyPrint(w goio.Writer, workerId string) {
	shmStats.PrettyPrint(w, workerId)
}

func GetServerStats() (stats ServerStats) {
	if shmStats.server.stats != nil {
		stats = *shmStats.server.stats
		if shmStats.current != nil {
			stats.StartTimestampNs = shmStats.current.stats.StartTimestampNs
			stats.Pid = shmStats.current.stats.Pid
		}
	}
	return
}

func GetAggregatedReqProcStats() (s ReqProcStats) {
	return shmStats.GetAggregatedReqProcStats()
}

func GetAggregatedInboundConnStats() (stats []InboundConnStats) {
	return shmStats.GetAggregatedInboundConnStats()
}

func GetListenerStats() (stats []ListenerStats) {
	if sz := len(shmStats.server.listeners); sz != 0 {
		stats = make([]ListenerStats, sz)
		for i := 0; i < sz; i++ {
			stats[i] = *shmStats.server.listeners[i]
		}
	}
	return
}

//TODO make sure shmStats has been initialized....
func GetCurrentWorkerStatsManager() *workerStatsManagerT {
	return shmStats.current
}

func GetWorkerStatsManager(workerId int) *workerStatsManagerT {
	if workerId < len(shmStats.workers) {
		return &shmStats.workers[workerId]
	}
	return nil
}

func GetReplicationTargetStats() []*ReplicationTargetStats {
	return shmStats.server.replicators
}

func CurrentWorkerHasTheLeastInboundConnections() bool {
	numConns := atomic.LoadUint32(&shmStats.current.stats.TotalNumConnections)
	if numConns == 0 {
		return true
	}
	numWorkers := int(shmStats.server.stats.NumWorkers)
	currentWorkerId := shmStats.currentWorkerId

	minNumConns := numConns

	for i := 0; i < numWorkers; i++ {
		if i != currentWorkerId {
			if minNumConns > shmStats.workers[i].stats.TotalNumConnections {
				minNumConns = shmStats.workers[i].stats.TotalNumConnections
			}
		}
	}
	if numConns <= minNumConns {
		return true
	}
	return false
}
