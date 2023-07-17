package standalone

import (
	"context"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
)

func init() {
	driver.RegisterCmd(driver.CmdTypeSet, "sadd", sadd)
	driver.RegisterCmd(driver.CmdTypeSet, "scard", scard)
	driver.RegisterCmd(driver.CmdTypeSet, "sdiff", sdiff)
	driver.RegisterCmd(driver.CmdTypeSet, "sdiffstore", sdiffstore)
	driver.RegisterCmd(driver.CmdTypeSet, "sinter", sinter)
	driver.RegisterCmd(driver.CmdTypeSet, "sinterstore", sinterstore)
	driver.RegisterCmd(driver.CmdTypeSet, "sismember", sismember)
	driver.RegisterCmd(driver.CmdTypeSet, "smembers", smembers)
	driver.RegisterCmd(driver.CmdTypeSet, "srem", srem)
	driver.RegisterCmd(driver.CmdTypeSet, "sunion", sunion)
	driver.RegisterCmd(driver.CmdTypeSet, "sunionstore", sunionstore)

	// del
	driver.RegisterCmd(driver.CmdTypeSet, "smclear", smclear)
	// expire
	driver.RegisterCmd(driver.CmdTypeSet, "sexpire", sexpire)
	// expireat
	driver.RegisterCmd(driver.CmdTypeSet, "sexpireat", sexpireat)
	// ttl
	driver.RegisterCmd(driver.CmdTypeSet, "sttl", sttl)
	// persist
	driver.RegisterCmd(driver.CmdTypeSet, "spersist", spersist)
	// exists
	driver.RegisterCmd(driver.CmdTypeSet, "skeyexists", skeyexists)
}

func sadd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SAdd(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func scard(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SCard(ctx, cmdParams[0])
	return
}

func sdiff(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SDiff(ctx, cmdParams...)
	return
}

func sdiffstore(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SDiffStore(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func sinter(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SInter(ctx, cmdParams...)
	return
}

func sinterstore(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SInterStore(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func sismember(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SIsMember(ctx, cmdParams[0], cmdParams[1])
	return
}

func smembers(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SMembers(ctx, cmdParams[0])
	return
}

func srem(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SRem(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func sunion(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SUnion(ctx, cmdParams...)
	return
}

func sunionstore(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 2 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().SUnionStore(ctx, cmdParams[0], cmdParams[1:]...)
	return
}

func smclear(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().Del(ctx, cmdParams...)
	return
}

func skeyexists(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().Exists(ctx, cmdParams[0])
	return
}

func sexpire(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	d, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBSet().Expire(ctx, cmdParams[0], d)
	return
}

func sexpireat(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 {
		err = ErrCmdParams
		return
	}

	d, err := utils.StrInt64(cmdParams[1], nil)
	if err != nil {
		err = ErrValue
		return
	}

	res, err = c.Db().DBSet().ExpireAt(ctx, cmdParams[0], d)
	return
}

func sttl(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().TTL(ctx, cmdParams[0])
	return
}

func spersist(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		err = ErrCmdParams
		return
	}

	res, err = c.Db().DBSet().Persist(ctx, cmdParams[0])
	return
}
