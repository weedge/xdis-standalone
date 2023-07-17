package standalone

import (
	"context"
	"strconv"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
)

func init() {
	driver.RegisterCmd(driver.CmdTypeString, "append", appendCmd)
	driver.RegisterCmd(driver.CmdTypeString, "decr", decr)
	driver.RegisterCmd(driver.CmdTypeString, "decrby", decrby)
	driver.RegisterCmd(driver.CmdTypeString, "get", get)
	driver.RegisterCmd(driver.CmdTypeString, "getrange", getrange)
	driver.RegisterCmd(driver.CmdTypeString, "getset", getset)
	driver.RegisterCmd(driver.CmdTypeString, "incr", incr)
	driver.RegisterCmd(driver.CmdTypeString, "incrby", incrby)
	driver.RegisterCmd(driver.CmdTypeString, "mget", mget)
	driver.RegisterCmd(driver.CmdTypeString, "mset", mset)
	driver.RegisterCmd(driver.CmdTypeString, "set", set)
	driver.RegisterCmd(driver.CmdTypeString, "setnx", setnx)
	driver.RegisterCmd(driver.CmdTypeString, "setex", setex)
	driver.RegisterCmd(driver.CmdTypeString, "setrange", setrange)
	driver.RegisterCmd(driver.CmdTypeString, "strlen", strlen)

	// new
	driver.RegisterCmd(driver.CmdTypeString, "setnxex", setnxex)
	driver.RegisterCmd(driver.CmdTypeString, "setxxex", setxxex)

	// just for string type key
	driver.RegisterCmd(driver.CmdTypeString, "del", del)
	driver.RegisterCmd(driver.CmdTypeString, "exists", exists)
	driver.RegisterCmd(driver.CmdTypeString, "expire", expire)
	driver.RegisterCmd(driver.CmdTypeString, "expireat", expireat)
	driver.RegisterCmd(driver.CmdTypeString, "persist", persist)
	driver.RegisterCmd(driver.CmdTypeString, "ttl", ttl)
}

func get(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	v, err := c.Db().DBString().GetSlice(ctx, cmdParams[0])
	if err != nil {
		return
	}
	if v == nil {
		return
	}

	res = v.Data()
	v.Free()
	return
}

func set(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	if err = c.Db().DBString().Set(ctx, cmdParams[0], cmdParams[1]); err != nil {
		return
	}

	return OK, nil
}

func appendCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().Append(ctx, cmdParams[0], cmdParams[1])
	return
}

func decr(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().Decr(ctx, cmdParams[0])
	return
}

func decrby(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}
	delta, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().DecrBy(ctx, cmdParams[0], delta)
	return
}

func del(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().Del(ctx, cmdParams...)
	return
}

func exists(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().Exists(ctx, cmdParams[0])
	return
}

func getrange(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	start, err := strconv.Atoi(string(cmdParams[1]))
	if err != nil {
		err = ErrValue
		return
	}

	end, err := strconv.Atoi(string(cmdParams[2]))
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().GetRange(ctx, cmdParams[0], start, end)
	return
}

func getset(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().GetSet(ctx, cmdParams[0], cmdParams[1])
	return
}

func incr(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().Incr(ctx, cmdParams[0])
	return
}

func incrby(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	delta, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().IncrBy(ctx, cmdParams[0], delta)
	return
}

func mget(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	return c.Db().DBString().MGet(ctx, cmdParams...)
}

func mset(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 || len(cmdParams)%2 != 0 {
		err = ErrCmdParams
		return
	}

	kvs := make([]driver.KVPair, len(cmdParams)/2)
	for i := 0; i < len(kvs); i++ {
		kvs[i].Key = cmdParams[2*i]
		kvs[i].Value = cmdParams[2*i+1]
	}

	err = c.Db().DBString().MSet(ctx, kvs...)
	if err != nil {
		return
	}
	res = OK

	return
}

func setnx(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().SetNX(ctx, cmdParams[0], cmdParams[1])
	return
}

func setex(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	sec, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	err = c.Db().DBString().SetEX(ctx, cmdParams[0], sec, cmdParams[2])
	if err != nil {
		return
	}

	res = OK
	return
}

func setnxex(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	sec, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().SetNXEX(ctx, cmdParams[0], sec, cmdParams[2])
	return
}

func setxxex(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	sec, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().SetXXEX(ctx, cmdParams[0], sec, cmdParams[2])
	return
}

func setrange(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	offset, err := strconv.Atoi(string(cmdParams[1]))
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().SetRange(ctx, cmdParams[0], offset, cmdParams[2])
	return
}

func strlen(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	return c.Db().DBString().StrLen(ctx, cmdParams[0])
}

func expire(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	duration, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().Expire(ctx, cmdParams[0], duration)
	return
}

func expireat(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	when, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBString().ExpireAt(ctx, cmdParams[0], when)
	return
}

func ttl(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().TTL(ctx, cmdParams[0])
	return
}

func persist(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBString().Persist(ctx, cmdParams[0])
	return
}
