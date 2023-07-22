package standalone

import (
	"strings"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/tidwall/redcon"
)

// QuitCmd connect is closed by srv
func (s *RespCmdService) QuitCmd(conn redcon.Conn, cmd redcon.Command) {
	err := conn.Close()
	if err != nil {
		conn.WriteError(err.Error())
		klog.Errorf("resp cmd quit connect close err: %s", err.Error())
		return
	}

	conn.WriteString("OK")
}

// InfoCmd srv info to dump
func (s *RespCmdService) InfoCmd(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) > 2 {
		conn.WriteError(ErrCmdParams.Error())
		return
	}

	section := DumpSrvInfoName("")
	if len(cmd.Args) == 2 {
		section = DumpSrvInfoName(cmd.Args[1])
	}
	blukInfo := s.info.DumpBytes(section)
	conn.WriteBulk(blukInfo)
}

// PublishCmd pub to all pub/sub subscribers and return the number of
// messages that were sent.
func (s *RespCmdService) PublishCmd(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	count := s.pubSub.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
	conn.WriteInt(count)
}

// Subscribe sub from a pub/sub channel. The `Psubscribe` and
// `Subscribe` operations will detach the connection from the
// event handler and manage all network I/O for this connection
// in the background.
func (s *RespCmdService) SubscribeCmd(conn redcon.Conn, cmd redcon.Command) {
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
