package standalone

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/weedge/pkg/driver"
)

type SrvInfo struct {
	srv *RespCmdService
}

func NewSrvInfo(srv *RespCmdService) (srvInfo *SrvInfo) {
	srvInfo = new(SrvInfo)
	srvInfo.srv = srv

	driver.RegisterDumpHandler("server", srvInfo.DumpServer)
	driver.RegisterDumpHandler("memory", srvInfo.DumpMemory)
	driver.RegisterDumpHandler("gcstats", srvInfo.DumpGCStats)
	driver.RegisterDumpHandler("keyspace", srvInfo.DumpKeySpace)
	//driver.RegisterDumpHandler("keyspace", srvInfo.DumpKeySpaceNoStats)
	// todo @weedge
	//driver.RegisterDumpHandler("storage", srvInfo.DumpStorageStats)

	return
}

func (m *SrvInfo) DumpBytes(name driver.DumpSrvInfoName) []byte {
	buf := &bytes.Buffer{}

	if len(name) == 0 {
		m.dumpAll(buf)
		return buf.Bytes()
	}

	buf.Write(name.RespDumpName())
	dumpHandler, ok := driver.RegisteredDumpHandlers[driver.DumpSrvInfoName(name.ToLow())]
	if ok {
		dumpHandler(buf)
	}

	return buf.Bytes()
}

func (m *SrvInfo) dumpAll(w io.Writer) {
	for i, name := range driver.RegisteredDumpHandlerNames {
		w.Write(name.RespDumpName())
		driver.RegisteredDumpHandlers[name](w)
		if i != len(driver.RegisteredDumpHandlers)-1 {
			w.Write(Delims)
		}
	}
}

func (m *SrvInfo) DumpServer(w io.Writer) {
	m.DumpPairs(w,
		driver.InfoPair{Key: "os", Value: runtime.GOOS},
		driver.InfoPair{Key: "process_id", Value: os.Getpid()},
		driver.InfoPair{Key: "addr", Value: m.srv.opts.Addr},
		driver.InfoPair{Key: "goroutine_num", Value: runtime.NumGoroutine()},
		driver.InfoPair{Key: "cgo_call_num", Value: runtime.NumCgoCall()},
		driver.InfoPair{Key: "resp_client_num", Value: m.srv.RespCmdConnectNum()},
	)
}

func (m *SrvInfo) DumpGCStats(w io.Writer) {
	count := 5
	var st debug.GCStats
	st.Pause = make([]time.Duration, count)
	debug.ReadGCStats(&st)

	h := make([]string, 0, count)

	for i := 0; i < count && i < len(st.Pause); i++ {
		h = append(h, st.Pause[i].String())
	}

	m.DumpPairs(w,
		driver.InfoPair{Key: "gc_last_time", Value: st.LastGC.Format("2006/01/02 15:04:05.000")},
		driver.InfoPair{Key: "gc_num", Value: st.NumGC},
		driver.InfoPair{Key: "gc_pause_total", Value: st.PauseTotal.String()},
		driver.InfoPair{Key: "gc_pause_history", Value: strings.Join(h, ",")},
	)
}

func getMemoryHuman(m uint64) string {
	if m > GB {
		return fmt.Sprintf("%0.3fG", float64(m)/float64(GB))
	} else if m > MB {
		return fmt.Sprintf("%0.3fM", float64(m)/float64(MB))
	} else if m > KB {
		return fmt.Sprintf("%0.3fK", float64(m)/float64(KB))
	} else {
		return fmt.Sprintf("%d", m)
	}
}

func (m *SrvInfo) DumpMemory(w io.Writer) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	m.DumpPairs(w,
		driver.InfoPair{Key: "mem_alloc", Value: getMemoryHuman(mem.Alloc)},
		driver.InfoPair{Key: "mem_sys", Value: getMemoryHuman(mem.Sys)},
		driver.InfoPair{Key: "mem_looksups", Value: getMemoryHuman(mem.Lookups)},
		driver.InfoPair{Key: "mem_mallocs", Value: getMemoryHuman(mem.Mallocs)},
		driver.InfoPair{Key: "mem_frees", Value: getMemoryHuman(mem.Frees)},
		driver.InfoPair{Key: "mem_total", Value: getMemoryHuman(mem.TotalAlloc)},
		driver.InfoPair{Key: "mem_heap_alloc", Value: getMemoryHuman(mem.HeapAlloc)},
		driver.InfoPair{Key: "mem_heap_sys", Value: getMemoryHuman(mem.HeapSys)},
		driver.InfoPair{Key: "mem_head_idle", Value: getMemoryHuman(mem.HeapIdle)},
		driver.InfoPair{Key: "mem_head_inuse", Value: getMemoryHuman(mem.HeapInuse)},
		driver.InfoPair{Key: "mem_head_released", Value: getMemoryHuman(mem.HeapReleased)},
		driver.InfoPair{Key: "mem_head_objects", Value: mem.HeapObjects},
	)
}

func (m *SrvInfo) DumpStorageStats(w io.Writer) {

}

// # Keyspace
// db0:keys=1,expires=0,avg_ttl=0
func (m *SrvInfo) DumpKeySpace(w io.Writer) {
	data := m.srv.store.StatsInfo("keyspace")
	if items, ok := data["keyspace"]; ok {
		m.DumpPairs(w, items...)
	}
}

func (m *SrvInfo) DumpKeySpaceNoStats(w io.Writer) {
	data := m.srv.store.StatsInfo("existkeydb")
	if items, ok := data["existkeydb"]; ok {
		m.DumpPairs(w, items...)
	}
}

func (m *SrvInfo) DumpPairs(w io.Writer, pairs ...driver.InfoPair) {
	for _, pair := range pairs {
		w.Write(pair.RespDumpInfo())
	}
}
