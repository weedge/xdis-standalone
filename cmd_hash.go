package standalone

import (
	"context"

	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
)

func init() {
	driver.RegisterCmd(driver.CmdTypeHash, "hexists", hexists)
	driver.RegisterCmd(driver.CmdTypeHash, "hget", hget)
	driver.RegisterCmd(driver.CmdTypeHash, "hgetall", hgetall)
	driver.RegisterCmd(driver.CmdTypeHash, "hincrby", hincrby)
	driver.RegisterCmd(driver.CmdTypeHash, "hkeys", hkeys)
	driver.RegisterCmd(driver.CmdTypeHash, "hlen", hlen)
	driver.RegisterCmd(driver.CmdTypeHash, "hmget", hmget)
	driver.RegisterCmd(driver.CmdTypeHash, "hmset", hmset)
	driver.RegisterCmd(driver.CmdTypeHash, "hset", hset)
	driver.RegisterCmd(driver.CmdTypeHash, "hvals", hvals)

	//del for hash
	driver.RegisterCmd(driver.CmdTypeHash, "hmclear", hmclear)
	//exists for hash
	driver.RegisterCmd(driver.CmdTypeHash, "hkeyexists", hkeyexists)
	//expire for hash
	driver.RegisterCmd(driver.CmdTypeHash, "hexpire", hexpire)
	//expireat for hash
	driver.RegisterCmd(driver.CmdTypeHash, "hexpireat", hexpireat)
	//ttl for hash
	driver.RegisterCmd(driver.CmdTypeHash, "httl", httl)
	//persist for hash
	driver.RegisterCmd(driver.CmdTypeHash, "hpersist", hpersist)
}

func hexists(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	v, err := c.Db().DBHash().HGet(ctx, cmdParams[0], cmdParams[1])
	if err != nil {
		return
	}
	if v == nil {
		res = redcon.SimpleInt(0)
		return
	}

	res = redcon.SimpleInt(1)
	return
}

func hget(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	v, err := c.Db().DBHash().HGet(ctx, cmdParams[0], cmdParams[1])
	if len(v) == 0 {
		return nil, nil
	}
	res = v
	return
}

func hgetall(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	data, err := c.Db().DBHash().HGetAll(ctx, cmdParams[0])
	if err != nil {
		return
	}

	tmp := [][]byte{}
	for _, item := range data {
		tmp = append(tmp, item.Field)
		tmp = append(tmp, item.Value)
	}
	res = tmp

	return
}

func hincrby(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}
	delta, err := utils.StrInt64(cmdParams[2], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBHash().HIncrBy(ctx, cmdParams[0], cmdParams[1], delta)
	return
}

func hkeys(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().HKeys(ctx, cmdParams[0])
	return
}

func hlen(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().HLen(ctx, cmdParams[0])
	return
}

func hmget(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().HMget(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func hmset(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 3 || len(cmdParams[1:])%2 != 0 {
		err = ErrCmdParams
		return
	}

	args := cmdParams[1:]
	kvs := make([]driver.FVPair, len(args)/2)
	for i := 0; i < len(kvs); i++ {
		kvs[i].Field = args[2*i]
		kvs[i].Value = args[2*i+1]
	}

	if err = c.Db().DBHash().HMset(ctx, cmdParams[0], kvs...); err != nil {
		return
	}

	res = OK
	return
}

func hset(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 3 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().HSet(ctx, cmdParams[0], cmdParams[1], cmdParams[2])
	return
}

func hvals(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().HValues(ctx, cmdParams[0])
	return
}

func hmclear(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().Del(ctx, cmdParams...)
	return
}

func hkeyexists(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().Exists(ctx, cmdParams[0])
	return
}

func hexpire(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	d, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBHash().Expire(ctx, cmdParams[0], d)
	return
}

func hexpireat(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	d, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBHash().ExpireAt(ctx, cmdParams[0], d)
	return
}

func httl(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().TTL(ctx, cmdParams[0])
	return
}

func hpersist(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBHash().Persist(ctx, cmdParams[0])
	return
}
