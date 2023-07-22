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

	"github.com/weedge/pkg/utils"
)

type DumpSrvInfoName string

func (name DumpSrvInfoName) RespDumpName() []byte {
	nameStr := fmt.Sprintf("# %s\r\n", name.FirstToUp())
	return utils.String2Bytes(nameStr)
}

func (name DumpSrvInfoName) ToLow() []byte {
	return utils.String2Bytes(strings.ToLower(string(name)))
}

func (name DumpSrvInfoName) FirstToUp() []byte {
	str := strings.ToUpper(string(name)[:1]) + string(name)[1:]
	return utils.String2Bytes(str)
}

type InfoPair struct {
	Key   string
	Value interface{}
}

func (pair InfoPair) RespDumpInfo() []byte {
	pairInfo := fmt.Sprintf("%s:%v\r\n", pair.Key, pair.Value)
	return utils.String2Bytes(pairInfo)
}

type DumpHandler func(w io.Writer)

var RegisteredDumpHandlers = map[DumpSrvInfoName]DumpHandler{}
var RegisteredDumpHandlerNames = []DumpSrvInfoName{}

func RegisterDumpHandler(name DumpSrvInfoName, handler DumpHandler) {
	if _, ok := RegisteredDumpHandlers[name]; !ok {
		RegisteredDumpHandlerNames = append(RegisteredDumpHandlerNames, name)
	}
	RegisteredDumpHandlers[name] = handler
}

type ISrvInfo interface {
	DumpBytes(name DumpSrvInfoName) []byte
}

type SrvInfo struct {
	srv *RespCmdService
}

func NewSrvInfo(srv *RespCmdService) (srvInfo *SrvInfo) {
	srvInfo = new(SrvInfo)
	srvInfo.srv = srv

	RegisterDumpHandler("server", srvInfo.DumpServer)
	RegisterDumpHandler("memory", srvInfo.DumpMemory)
	RegisterDumpHandler("gcstats", srvInfo.DumpGCStats)
	// todo @weedge
	//RegisterDumpHandler("storage", srvInfo.DumpStorageStats)

	return
}

func (m *SrvInfo) DumpBytes(name DumpSrvInfoName) []byte {
	buf := &bytes.Buffer{}

	if len(name) == 0 {
		m.dumpAll(buf)
		return buf.Bytes()
	}

	buf.Write(name.RespDumpName())
	dumpHandler, ok := RegisteredDumpHandlers[DumpSrvInfoName(name.ToLow())]
	if ok {
		dumpHandler(buf)
	}

	return buf.Bytes()
}

func (m *SrvInfo) dumpAll(w io.Writer) {
	for i, name := range RegisteredDumpHandlerNames {
		w.Write(name.RespDumpName())
		RegisteredDumpHandlers[name](w)
		if i != len(RegisteredDumpHandlers)-1 {
			w.Write(Delims)
		}
	}
}

func (m *SrvInfo) DumpServer(w io.Writer) {
	m.DumpPairs(w,
		InfoPair{"os", runtime.GOOS},
		InfoPair{"process_id", os.Getpid()},
		InfoPair{"addr", m.srv.opts.Addr},
		InfoPair{"goroutine_num", runtime.NumGoroutine()},
		InfoPair{"cgo_call_num", runtime.NumCgoCall()},
		InfoPair{"resp_client_num", m.srv.RespCmdConnectNum()},
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
		InfoPair{"gc_last_time", st.LastGC.Format("2006/01/02 15:04:05.000")},
		InfoPair{"gc_num", st.NumGC},
		InfoPair{"gc_pause_total", st.PauseTotal.String()},
		InfoPair{"gc_pause_history", strings.Join(h, ",")},
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
		InfoPair{"mem_alloc", getMemoryHuman(mem.Alloc)},
		InfoPair{"mem_sys", getMemoryHuman(mem.Sys)},
		InfoPair{"mem_looksups", getMemoryHuman(mem.Lookups)},
		InfoPair{"mem_mallocs", getMemoryHuman(mem.Mallocs)},
		InfoPair{"mem_frees", getMemoryHuman(mem.Frees)},
		InfoPair{"mem_total", getMemoryHuman(mem.TotalAlloc)},
		InfoPair{"mem_heap_alloc", getMemoryHuman(mem.HeapAlloc)},
		InfoPair{"mem_heap_sys", getMemoryHuman(mem.HeapSys)},
		InfoPair{"mem_head_idle", getMemoryHuman(mem.HeapIdle)},
		InfoPair{"mem_head_inuse", getMemoryHuman(mem.HeapInuse)},
		InfoPair{"mem_head_released", getMemoryHuman(mem.HeapReleased)},
		InfoPair{"mem_head_objects", mem.HeapObjects},
	)
}

func (m *SrvInfo) DumpStorageStats(w io.Writer) {

}

func (m *SrvInfo) DumpPairs(w io.Writer, pairs ...InfoPair) {
	for _, pair := range pairs {
		w.Write(pair.RespDumpInfo())
	}
}
