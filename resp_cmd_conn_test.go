package standalone

import (
	"testing"

	"github.com/weedge/pkg/driver"
)

func TestImpIRespConn(t *testing.T) {
	var i interface{} = &RespCmdConn{}
	if _, ok := i.(driver.IRespConn); !ok {
		t.Fatalf("does not implement driver.IRespConn")
	}
}
