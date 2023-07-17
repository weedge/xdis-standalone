package standalone

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-standalone/config"
)

func init() {
	// not use storage
	driver.RegisterCmd(driver.CmdTypeSrv, "client", client)
	driver.RegisterCmd(driver.CmdTypeSrv, "echo", echo)
	driver.RegisterCmd(driver.CmdTypeSrv, "hello", hello)
	driver.RegisterCmd(driver.CmdTypeSrv, "ping", ping)

	// need use storage
	driver.RegisterCmd(driver.CmdTypeSrv, "select", selectCmd)
	driver.RegisterCmd(driver.CmdTypeSrv, "flushdb", flushdb)
	driver.RegisterCmd(driver.CmdTypeSrv, "flushall", flushall)
}

func authUser(ctx context.Context, c driver.IRespConn, pwd string) (err error) {
	return
}

func client(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 1 {
		return nil, ErrCmdParams
	}
	op := strings.ToLower(utils.Bytes2String(cmdParams[0]))
	switch op {
	case "getname":
		res = c.Name()
	default:
		//todo
	}

	return
}

// just hello cmd, no resp protocol change
func hello(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) > 6 {
		op := cmdParams[len(cmdParams)-1]
		return nil, fmt.Errorf("%s in %s option '%s'", ErrSyntax.Error(), "HELLO", op)
	}

	/*
		data := map[string]any{
			"server": "redis",
			"proto":  redcon.SimpleInt(2),
			"mode":   c.srv.opts.RespCmdSrvOpts.Mode,
		}
	*/
	data := []any{
		"server", "redis",
		"proto", redcon.SimpleInt(2),
		"mode", config.RegisterRespSrvModeName,
	}
	res = data
	if len(cmdParams) == 0 {
		return
	}

	protocalVer, err := strconv.ParseInt(utils.Bytes2String(cmdParams[0]), 10, 64)
	if err != nil {
		return nil, ErrProtocalVer
	}
	if protocalVer < 2 || protocalVer > 3 {
		return nil, ErrUnsupportVer
	}

	for nextArg := 1; nextArg < len(cmdParams); nextArg++ {
		moreArgs := len(cmdParams) - nextArg - 1
		op := strings.ToLower(utils.Bytes2String(cmdParams[nextArg]))
		if op == "auth" && moreArgs > 0 && moreArgs%2 == 0 {
			nextArg++
			if strings.ToLower(utils.Bytes2String(cmdParams[nextArg])) != "default" {
				return nil, ErrInvalidPwd
			}
			nextArg++
			pwd := utils.Bytes2String(cmdParams[nextArg])
			if err = authUser(ctx, c, pwd); err != nil {
				return nil, err
			}
			//println("auth", nextArg)
		} else if op == "setname" && moreArgs > 0 {
			nextArg++
			c.SetConnName(utils.Bytes2String(cmdParams[nextArg]))
			//println("setname", nextArg)
		} else {
			//println("other", moreArgs, nextArg, op)
			if moreArgs == 3 {
				op = "setname"
			}
			return nil, fmt.Errorf("%s in %s option '%s'", ErrSyntax.Error(), "HELLO", op)
		}
	}

	return
}

func ping(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) > 1 {
		return nil, ErrCmdParams
	}
	if len(cmdParams) == 1 {
		res = cmdParams[0]
		return
	}
	res = PONG
	return
}

func echo(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		return nil, ErrCmdParams
	}

	res = cmdParams[0]
	return
}

func selectCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		return nil, ErrCmdParams
	}

	index, err := strconv.Atoi(utils.Bytes2String(cmdParams[0]))
	if err != nil {
		return
	}

	db, err := c.(RespCmdConn).srv.store.Select(ctx, index)
	if err != nil {
		return
	}
	c.SetDb(db)

	res = OK
	return
}

func flushdb(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	_, err = c.Db().FlushDB(ctx)
	if err != nil {
		return
	}

	res = OK
	return
}

func flushall(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	err = c.(RespCmdConn).srv.store.FlushAll(ctx)
	if err != nil {
		return
	}

	res = OK
	return
}
