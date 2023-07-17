package standalone

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
)

func init() {
	driver.RegisterCmd(driver.CmdTypeList, "blpop", blpop)
	driver.RegisterCmd(driver.CmdTypeList, "brpop", brpop)
	driver.RegisterCmd(driver.CmdTypeList, "lindex", lindex)
	driver.RegisterCmd(driver.CmdTypeList, "llen", llen)
	driver.RegisterCmd(driver.CmdTypeList, "lpop", lpop)
	driver.RegisterCmd(driver.CmdTypeList, "lrange", lrange)
	driver.RegisterCmd(driver.CmdTypeList, "lset", lset)
	driver.RegisterCmd(driver.CmdTypeList, "lpush", lpush)
	driver.RegisterCmd(driver.CmdTypeList, "rpop", rpop)
	driver.RegisterCmd(driver.CmdTypeList, "rpush", rpush)
	driver.RegisterCmd(driver.CmdTypeList, "brpoplpush", brpoplpush)
	driver.RegisterCmd(driver.CmdTypeList, "rpoplpush", rpoplpush)

	//del for list
	driver.RegisterCmd(driver.CmdTypeList, "lmclear", lmclear)
	//exists for list
	driver.RegisterCmd(driver.CmdTypeList, "lkeyexists", lkeyexists)
	//expire for list
	driver.RegisterCmd(driver.CmdTypeList, "lexpire", lexpire)
	//expireat for list
	driver.RegisterCmd(driver.CmdTypeList, "lexpireat", lexpireat)
	//ttl for list
	driver.RegisterCmd(driver.CmdTypeList, "lttl", lttl)
	//persist for list
	driver.RegisterCmd(driver.CmdTypeList, "lpersist", lpersist)
}

func lmclear(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().Del(ctx, cmdParams...)
	return
}

func blpop(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	t, err := strconv.ParseFloat(utils.Bytes2String(cmdParams[len(cmdParams)-1]), 64)
	if err != nil {
		return
	}
	timeout := time.Duration(t * float64(time.Second))

	res, err = c.Db().DBList().BLPop(ctx, cmdParams[:len(cmdParams)-1], timeout)
	return
}

func brpop(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	t, err := strconv.ParseFloat(utils.Bytes2String(cmdParams[len(cmdParams)-1]), 64)
	if err != nil {
		return
	}
	timeout := time.Duration(t * float64(time.Second))

	res, err = c.Db().DBList().BRPop(ctx, cmdParams[:len(cmdParams)-1], timeout)
	return
}

func lindex(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	i, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBList().LIndex(ctx, cmdParams[0], int32(i))
	return
}

func llen(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().LLen(ctx, cmdParams[0])
	return
}

func lpop(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().LPop(ctx, cmdParams[0])
	return
}

func lrange(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	start, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}
	end, err := utils.StrInt64(cmdParams[2], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBList().LRange(ctx, cmdParams[0], int32(start), int32(end))
	return
}

func lset(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}
	i, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	if err = c.Db().DBList().LSet(ctx, cmdParams[0], int32(i), cmdParams[2]); err != nil {
		return
	}

	res = OK
	return
}

func lpush(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().LPush(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func rpop(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().RPop(ctx, cmdParams[0])
	return
}

func rpush(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().RPush(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func brpoplpush(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	source, dest := cmdParams[0], cmdParams[1]
	t, err := strconv.ParseFloat(utils.Bytes2String(cmdParams[len(cmdParams)-1]), 64)
	if err != nil {
		return
	}
	timeout := time.Duration(t * float64(time.Second))

	ttl := int64(-1)
	// source dest equal, same list, get ttl
	if bytes.Equal(source, dest) {
		ttl, err = c.Db().DBList().TTL(ctx, source)
		if err != nil {
			return
		}
	}

	kvdata, err := c.Db().DBList().BRPop(ctx, [][]byte{source}, timeout)
	if err != nil {
		return
	}
	if kvdata == nil {
		return
	}
	if len(kvdata) < 2 {
		return
	}

	vdata, ok := kvdata[1].([]byte)
	if !ok {
		err = ErrValue
		return
	}

	// lpush err rpush back
	if _, err = c.Db().DBList().LPush(ctx, dest, vdata); err != nil {
		c.Db().DBList().RPush(ctx, source, vdata)
		return
	}

	// reset tll
	if ttl != -1 {
		c.Db().DBList().Expire(ctx, source, ttl)
	}

	res = vdata
	return
}

func rpoplpush(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	source, dest := cmdParams[0], cmdParams[1]
	ttl := int64(-1)
	// source dest equal, same list, get ttl
	if bytes.Equal(source, dest) {
		ttl, err = c.Db().DBList().TTL(ctx, source)
		if err != nil {
			return
		}
	}

	data, err := c.Db().DBList().RPop(ctx, source)
	if err != nil {
		return
	}
	if data == nil {
		return
	}

	// lpush err rpush back
	if _, err = c.Db().DBList().LPush(ctx, dest, data); err != nil {
		c.Db().DBList().RPush(ctx, source, data)
		return
	}

	// reset tll
	if ttl != -1 {
		c.Db().DBList().Expire(ctx, source, ttl)
	}

	res = data
	return
}

func lkeyexists(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().Exists(ctx, cmdParams[0])
	return
}

func lexpire(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	d, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBList().Expire(ctx, cmdParams[0], d)
	return
}

func lexpireat(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	d, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBList().ExpireAt(ctx, cmdParams[0], d)
	return
}

func lttl(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().TTL(ctx, cmdParams[0])
	return
}

func lpersist(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBList().Persist(ctx, cmdParams[0])
	return
}
