package standalone

import (
	"context"
	"errors"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
)

type RespCmdConn struct {
	*driver.RespConnBase

	srv      *RespCmdService
	isAuthed bool

	redcon.Conn
}

func (c *RespCmdConn) SetRedConn(redConn redcon.Conn) {
	c.Conn = redConn
}

func (c *RespCmdConn) GetRemoteAddr() string {
	return c.Conn.RemoteAddr()
}

func (c *RespCmdConn) Close() error {
	err := c.Conn.Close()
	return err
}

func (c *RespCmdConn) DoCmd(ctx context.Context, cmd string, cmdParams [][]byte) (res interface{}, err error) {
	cmd = strings.ToLower(strings.TrimSpace(cmd))
	f, ok := c.srv.handles[cmd]
	if !ok {
		err = errors.New("ERR unknown command '" + cmd + "'")
		return
	}

	respConn, ok := ctx.Value(RespCmdCtxKey).(driver.IRespConn)
	if !ok {
		err = errors.New("respCmdCtxKey not IRespConn")
		return
	}

	res, err = f(ctx, respConn, cmdParams)
	if err != nil {
		return
	}

	return
}
