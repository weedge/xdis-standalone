package standalone

import (
	"testing"

	"github.com/weedge/pkg/driver"
)

func TestRespCmdSrv_Implements(t *testing.T) {
	var i interface{} = &RespCmdService{}
	if _, ok := i.(driver.IRespService); !ok {
		t.Fatalf("does not implement driver.IRespService")
	}
}

func TestRespCmdSrvConn_Implements(t *testing.T) {
	var i interface{} = &RespCmdConn{}
	if _, ok := i.(driver.IRespConn); !ok {
		t.Fatalf("does not implement driver.IRespConn")
	}
}
