package standalone

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/xdis-standalone/config"
)

type RespCmdService struct {
	opts *config.RespCmdServiceOptions
	// redcon server handler
	mux *redcon.ServeMux
	// redcon server
	redconSrv *redcon.Server
	// storager
	store driver.IStorager

	// mutex lock for respConnMap add/delete
	rcm sync.Mutex
	// resp cmd connects map
	respConnMap map[*RespCmdConn]struct{}
}

func New(opts *config.RespCmdServiceOptions) (srv *RespCmdService) {
	if opts == nil {
		return
	}
	srv = &RespCmdService{
		opts:        opts,
		mux:         redcon.NewServeMux(),
		respConnMap: map[*RespCmdConn]struct{}{},
	}

	return
}

func (s *RespCmdService) SetStorager(store driver.IStorager) {
	s.store = store
}

func (s *RespCmdService) Name() driver.RespServiceName {
	return config.RegisterRespSrvModeName
}

func (s *RespCmdService) Close() (err error) {
	if s.redconSrv != nil {
		if err = s.redconSrv.Close(); err != nil {
			klog.Errorf("close redcon service err: %s", err.Error())
		}
		s.redconSrv = nil
	}

	if err == nil {
		klog.Infof("close resp cmd service ok")
	}

	s.closeAllRespCmdConnect()
	return
}

func (s *RespCmdService) registerRespCmdConnHandle() {
	for cmdOp := range driver.RegisteredCmdHandles {
		s.mux.HandleFunc(cmdOp, func(conn redcon.Conn, cmd redcon.Command) {
			cmdOp := utils.SliceByteToString(cmd.Args[0])
			params := [][]byte{}
			if len(cmd.Args) > 0 {
				params = cmd.Args[1:]
			}

			switch cmdOp {
			case "quit":
				// closed by srv
				err := conn.Close()
				if err != nil {
					klog.Errorf("resp cmd quit connect close err: %s", err.Error())
				}
				return
			}

			respConn, ok := conn.Context().(driver.IRespConn)
			if !ok {
				klog.Errorf("resp cmd connect init err")
				return
			}
			ctx := context.WithValue(context.Background(), RespCmdCtxKey, conn.Context())
			res, err := respConn.DoCmd(ctx, cmdOp, params)
			klog.Debugf("resp cmd %s params %v res: %+v err: %v", cmdOp, params, res, err)
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

func (s *RespCmdService) Start(ctx context.Context) (err error) {
	//RESP cmd tcp server
	s.redconSrv = redcon.NewServer(s.opts.Addr, s.mux.ServeRESP,
		// use this function to accept (return true) or deny the connection (return false).
		func(conn redcon.Conn) bool {
			klog.Infof("accept: %s", conn.RemoteAddr())

			// add resp cmd conn
			respConn := s.InitRespConn(ctx, 0)
			respCmdConn := respConn.(*RespCmdConn)
			respCmdConn.SetRedConn(conn)
			s.addRespCmdConn(respCmdConn)

			// set ctx
			conn.SetContext(respConn)
			return true
		},
		// this is called when the connection has been closed by remote client
		func(conn redcon.Conn, err error) {
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
			s.delRespCmdConn(respCmdConn)
		},
	)

	if s.opts.ConnKeepaliveInterval > 0 {
		s.redconSrv.SetIdleClose(time.Duration(s.opts.ConnKeepaliveInterval))
	}

	s.registerRespCmdConnHandle()

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

func (s *RespCmdService) addRespCmdConn(c *RespCmdConn) {
	s.rcm.Lock()
	s.respConnMap[c] = struct{}{}
	s.rcm.Unlock()
}

func (s *RespCmdService) delRespCmdConn(c *RespCmdConn) {
	s.rcm.Lock()
	delete(s.respConnMap, c)
	s.rcm.Unlock()
}

func (s *RespCmdService) closeAllRespCmdConnect() {
	s.rcm.Lock()
	for c := range s.respConnMap {
		c.Close()
	}
	s.rcm.Unlock()
}

func (s *RespCmdService) RespCmdConnectNum() int {
	s.rcm.Lock()
	n := len(s.respConnMap)
	s.rcm.Unlock()
	return n
}
