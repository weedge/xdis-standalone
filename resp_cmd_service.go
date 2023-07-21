package standalone

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/xdis-standalone/config"
)

type OnAccept func(conn redcon.Conn) bool
type OnClosed func(conn redcon.Conn, err error)

type RespCmdService struct {
	opts *config.RespCmdServiceOptions
	// redcon server handler
	mux *redcon.ServeMux
	// redcon server
	redconSrv *redcon.Server
	onAccept  OnAccept
	onClosed  OnClosed
	handles   map[string]driver.CmdHandle

	// storager
	store driver.IStorager

	// mutex lock for respConnMap add/delete
	rcm sync.Mutex
	// resp cmd connects map
	respConnMap map[driver.IRespConn]struct{}

	// pub/sub
	pubSub redcon.PubSub
}

func New(opts *config.RespCmdServiceOptions) (srv *RespCmdService) {
	if opts == nil {
		return
	}
	srv = &RespCmdService{
		opts:        opts,
		mux:         redcon.NewServeMux(),
		respConnMap: map[driver.IRespConn]struct{}{},
	}
	srv.onAccept = srv.OnAccept
	srv.onClosed = srv.OnClosed
	srv.handles = driver.RegisteredCmdHandles

	return
}

func (s *RespCmdService) SetStorager(store driver.IStorager) {
	s.store = store
}

func (s *RespCmdService) Name() driver.RespServiceName {
	return config.RegisterRespSrvModeName
}

func (s *RespCmdService) Close() (err error) {
	s.CloseAllRespCmdConnect()

	if s.redconSrv != nil {
		if err = s.redconSrv.Close(); err != nil {
			klog.Errorf("close redcon service err: %s", err.Error())
		}
		s.redconSrv = nil
	}

	if err == nil {
		klog.Infof("close resp cmd service ok")
	}
	return
}

func (s *RespCmdService) SetRegisteredCmdHandles(handles map[string]driver.CmdHandle) {
	s.handles = handles
}

func (s *RespCmdService) RegisterRespCmdConnHandle() {
	s.mux.HandleFunc("quit", func(conn redcon.Conn, cmd redcon.Command) {
		// closed by srv
		err := conn.Close()
		if err != nil {
			klog.Errorf("resp cmd quit connect close err: %s", err.Error())
		}
	})

	// Publish to all pub/sub subscribers and return the number of
	// messages that were sent.
	s.mux.HandleFunc("publish", func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		count := s.pubSub.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
		conn.WriteInt(count)
	})

	// Subscribe to a pub/sub channel. The `Psubscribe` and
	// `Subscribe` operations will detach the connection from the
	// event handler and manage all network I/O for this connection
	// in the background.
	subHandler := func(conn redcon.Conn, cmd redcon.Command) {
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		command := strings.ToLower(string(cmd.Args[0]))
		for i := 1; i < len(cmd.Args); i++ {
			if command == "psubscribe" {
				s.pubSub.Psubscribe(conn, string(cmd.Args[i]))
			} else {
				s.pubSub.Subscribe(conn, string(cmd.Args[i]))
			}
		}
	}
	s.mux.HandleFunc("subscribe", subHandler)
	s.mux.HandleFunc("psubscribe", subHandler)

	for cmdOp := range s.handles {
		s.mux.HandleFunc(cmdOp, func(conn redcon.Conn, cmd redcon.Command) {
			cmdOp := utils.SliceByteToString(cmd.Args[0])
			params := [][]byte{}
			if len(cmd.Args) > 0 {
				params = cmd.Args[1:]
			}

			respConn, ok := conn.Context().(driver.IRespConn)
			if !ok {
				klog.Errorf("resp cmd connect init err")
				return
			}
			ctx := context.WithValue(context.Background(), RespCmdCtxKey, conn.Context())
			res, err := respConn.DoCmd(ctx, cmdOp, params)
			klog.Debugf("resp cmd %s params %v res: %+v to %s err: %v", cmdOp, params, res, conn.RemoteAddr(), err)
			// nothing to do, has Write to connFd in DoCmd
			if err == ErrNoops {
				return
			}
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if _, ok := res.(int64); ok {
				conn.WriteInt64(res.(int64))
				return
			}
			conn.WriteAny(res)
		})
	}
}

func (s *RespCmdService) InitRespConn(ctx context.Context, dbIdx int) driver.IRespConn {
	if dbIdx < 0 {
		dbIdx = 0
	}

	conn := &RespCmdConn{RespConnBase: &driver.RespConnBase{}, srv: s, isAuthed: false}
	db, err := s.store.Select(ctx, dbIdx)
	if err != nil {
		return nil
	}
	conn.SetDb(db)

	return conn
}

func (s *RespCmdService) SetOnAccept(onAccept OnAccept) {
	s.onAccept = onAccept
}

func (s *RespCmdService) SetOnClosed(onClosed OnClosed) {
	s.onClosed = onClosed
}

func (s *RespCmdService) OnAccept(conn redcon.Conn) bool {
	klog.Infof("accept: %s", conn.RemoteAddr())

	// todo: get net.Conn request info set to context Value for trace
	// add resp cmd conn
	respConn := s.InitRespConn(context.Background(), 0)
	respCmdConn := respConn.(*RespCmdConn)
	respCmdConn.SetRedConn(conn)
	s.AddRespCmdConn(respCmdConn)

	// set ctx
	conn.SetContext(respConn)
	return true
}

func (s *RespCmdService) OnClosed(conn redcon.Conn, err error) {
	logF := klog.Infof
	if err != nil {
		logF = klog.Errorf
	}
	logF("closed by %s, err: %v", conn.RemoteAddr(), err)

	// del resp cmd conn
	respConn, ok := conn.Context().(driver.IRespConn)
	if !ok {
		klog.Errorf("resp cmd connect client init err")
		return
	}
	respCmdConn := respConn.(*RespCmdConn)
	s.DelRespCmdConn(respCmdConn)
}

func (s *RespCmdService) Start(ctx context.Context) (err error) {
	//RESP cmd tcp server
	s.redconSrv = redcon.NewServer(s.opts.Addr, s.mux.ServeRESP,
		// use this function to accept (return true) or deny the connection (return false).
		s.onAccept,
		// this is called when the connection has been closed by remote client
		s.onClosed,
	)

	if s.opts.ConnKeepaliveInterval > 0 {
		s.redconSrv.SetIdleClose(time.Duration(s.opts.ConnKeepaliveInterval))
	}

	s.RegisterRespCmdConnHandle()

	listenErrSignal := make(chan error)
	go func() {
		err := s.redconSrv.ListenServeAndSignal(listenErrSignal)
		if err != nil {
			klog.Fatal(err)
		}
	}()
	err = <-listenErrSignal
	if err != nil {
		klog.Errorf("resp cmd server listen err:%s", err.Error())
		return
	}
	klog.Infof("resp cmd server listening on address=%s", s.opts.Addr)
	return
}

func (s *RespCmdService) AddRespCmdConn(c driver.IRespConn) {
	s.rcm.Lock()
	s.respConnMap[c] = struct{}{}
	s.rcm.Unlock()
}

func (s *RespCmdService) DelRespCmdConn(c driver.IRespConn) {
	s.rcm.Lock()
	delete(s.respConnMap, c)
	s.rcm.Unlock()
}

func (s *RespCmdService) CloseAllRespCmdConnect() {
	s.rcm.Lock()
	for c := range s.respConnMap {
		if err := c.Close(); err != nil {
			klog.Errorf("close conn %s err %s", c.Name(), err.Error())
		} else {
			klog.Debugf("close conn %s ok", c.Name())
		}
	}
	s.rcm.Unlock()
}

func (s *RespCmdService) RespCmdConnectNum() int {
	s.rcm.Lock()
	n := len(s.respConnMap)
	s.rcm.Unlock()
	return n
}
