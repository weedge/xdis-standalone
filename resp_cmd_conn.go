package standalone

import (
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
