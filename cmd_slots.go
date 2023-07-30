package standalone

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
)

// cmd more detail reference:
// https://github.com/CodisLabs/codis/blob/master/extern/redis-3.2.11/src/server.c#L302-L325
// https://github.com/CodisLabs/codis/blob/master/doc/redis_change_zh.md

func init() {
	driver.RegisterCmd(driver.CmdTypeSlot, "slotshashkey", slotsHashKeyCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotsinfo", slotsInfoCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotsdel", slotsDelCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotscheck", slotsCheckCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotsrestore", slotsRestoreCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotsmgrtone", slotsMgrtOneCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotsmgrtslot", slotsMgrtSlotCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotsmgrttagone", slotsMgrtTagOneCmd)
	driver.RegisterCmd(driver.CmdTypeSlot, "slotsmgrttagslot", slotsMgrtTagSlotCmd)
}

// SLOTSHASHKEY key [key...]
func slotsHashKeyCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		return nil, ErrCmdParams
	}

	data := make([]redcon.SimpleInt, 0, len(cmdParams))
	slots, err := c.Db().(driver.IDBSlots).DBSlot().SlotsHashKey(ctx, cmdParams...)
	if err != nil {
		return
	}
	for _, slot := range slots {
		data = append(data, redcon.SimpleInt(slot))
	}
	res = data

	return
}

// SLOTSINFO start offset [WITHSIZE]
func slotsInfoCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) > 3 {
		return nil, ErrCmdParams
	}

	var start, count uint64 = 0, 0
	var withSize bool = false
	switch len(cmdParams) {
	case 3:
		switch strings.ToLower(utils.SliceByteToString(cmdParams[2])) {
		case "withsize":
			withSize = true
		}
		fallthrough
	case 2:
		count, err = strconv.ParseUint(utils.SliceByteToString(cmdParams[1]), 10, 64)
		if err != nil {
			return nil, ErrCmdParams
		}
		fallthrough
	case 1:
		start, err = strconv.ParseUint(utils.SliceByteToString(cmdParams[0]), 10, 64)
		if err != nil {
			return nil, ErrCmdParams
		}
	}

	slotsInfo, err := c.Db().(driver.IDBSlots).DBSlot().SlotsInfo(ctx, start, count, withSize)
	if err != nil {
		return nil, err
	}

	data := make([]any, len(slotsInfo))
	for i := 0; i < len(slotsInfo); i++ {
		data[i] = []any{
			redcon.SimpleInt(slotsInfo[i].Num),
			redcon.SimpleInt(slotsInfo[i].Size),
		}
	}
	res = data

	return
}

func parseMgrtArgs(cmdParams [][]byte) (addr string, timeout time.Duration, err error) {
	if len(cmdParams) != 4 {
		err = ErrCmdParams
		return
	}

	host := string(cmdParams[0])
	port, err := strconv.ParseInt(utils.SliceByteToString(cmdParams[1]), 10, 64)
	if err != nil {
		err = ErrCmdParams
		return
	}
	addr = fmt.Sprintf("%s:%d", host, port)

	ttlms, err := strconv.ParseInt(utils.SliceByteToString(cmdParams[2]), 10, 64)
	if err != nil {
		err = ErrCmdParams
		return
	}
	timeout = time.Duration(ttlms) * time.Millisecond
	if timeout == 0 {
		timeout = time.Second
	}

	return
}

// SLOTSMGRTONE host port timeout key
func slotsMgrtOneCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	addr, timeout, err := parseMgrtArgs(cmdParams)
	if err != nil {
		return nil, err
	}

	key := cmdParams[3]
	migrateCn, err := c.Db().(driver.IDBSlots).DBSlot().MigrateOneKey(ctx, addr, timeout, key)
	if err != nil {
		return nil, err
	}
	res = redcon.SimpleInt(migrateCn)

	return
}

// SLOTSMGRTSLOT host port timeout slot
func slotsMgrtSlotCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	addr, timeout, err := parseMgrtArgs(cmdParams)
	if err != nil {
		return nil, err
	}

	slot, err := strconv.ParseInt(utils.SliceByteToString(cmdParams[3]), 10, 64)
	if err != nil {
		return 0, ErrCmdParams
	}

	migrateCn, err := c.Db().(driver.IDBSlots).DBSlot().MigrateSlotOneKey(ctx, addr, timeout, uint64(slot))
	if err != nil {
		return 0, err
	}
	res = redcon.SimpleInt(migrateCn)

	return
}

// SLOTSMGRTTAGONE host port timeout key
func slotsMgrtTagOneCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	addr, timeout, err := parseMgrtArgs(cmdParams)
	if err != nil {
		return nil, err
	}

	key := cmdParams[3]
	migrateCn, err := c.Db().(driver.IDBSlots).DBSlot().MigrateKeyWithSameTag(ctx, addr, timeout, key)
	if err != nil {
		return nil, err
	}
	res = redcon.SimpleInt(migrateCn)

	return
}

// SLOTSMGRTTAGSLOT host port timeout slot
func slotsMgrtTagSlotCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	addr, timeout, err := parseMgrtArgs(cmdParams)
	if err != nil {
		return nil, err
	}

	slot, err := strconv.ParseInt(utils.SliceByteToString(cmdParams[3]), 10, 64)
	if err != nil {
		return 0, ErrCmdParams
	}

	migrateCn, err := c.Db().(driver.IDBSlots).DBSlot().MigrateSlotKeyWithSameTag(ctx, addr, timeout, uint64(slot))
	if err != nil {
		return 0, err
	}
	res = redcon.SimpleInt(migrateCn)

	return
}

func TTLmsToExpireAt(ttlms int64) (int64, bool) {
	if ttlms < 0 {
		return 0, false
	}
	expireat := time.Now().UnixMilli() + ttlms

	return expireat, true
}

// SLOTSRESTORE key ttlms value [key ttlms value ...]
func slotsRestoreCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 || len(cmdParams)%3 != 0 {
		return nil, ErrCmdParams
	}

	objs := make([]*driver.SlotsRestoreObj, len(cmdParams)/3)
	for i := 0; i < len(objs); i++ {
		key := cmdParams[i*3]

		ttlms, err := strconv.ParseInt(utils.SliceByteToString(cmdParams[i*3+1]), 10, 64)
		if err != nil {
			return nil, ErrCmdParams
		}

		value := cmdParams[i*3+2]
		objs[i] = &driver.SlotsRestoreObj{
			Key:   key,
			Val:   value,
			TTLms: ttlms,
		}
	}

	err = c.Db().(driver.IDBSlots).DBSlot().SlotsRestore(ctx, objs...)
	if err != nil {
		return nil, err
	}
	res = OK

	return
}

// SLOTSCHECK
// for debug/test, don't use in product
func slotsCheckCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	err = c.Db().(driver.IDBSlots).DBSlot().SlotsCheck(ctx)
	if err != nil {
		return nil, err
	}
	res = OK

	return
}

// SLOTSDEL slot ...
func slotsDelCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) < 1 {
		return nil, ErrCmdParams
	}
	slots := make([]uint64, len(cmdParams))
	for i := 0; i < len(cmdParams); i++ {
		slot, err := strconv.ParseInt(utils.SliceByteToString(cmdParams[i]), 10, 64)
		if err != nil {
			return nil, ErrCmdParams
		}
		slots[i] = uint64(slot)
	}

	slotsInfo, err := c.Db().(driver.IDBSlots).DBSlot().SlotsDel(ctx, slots...)
	if err != nil {
		return nil, err
	}
	data := make([]any, len(slotsInfo))
	for i := 0; i < len(slotsInfo); i++ {
		data[i] = []any{
			redcon.SimpleInt(slotsInfo[i].Num),
			redcon.SimpleInt(slotsInfo[i].Size),
		}
	}
	res = data

	return
}
