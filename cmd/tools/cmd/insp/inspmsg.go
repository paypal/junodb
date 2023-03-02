package insp

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"

	"juno/pkg/cmd"
	"juno/pkg/proto"
)

type cmdInspMsgT struct {
	cmd.Command
	msg []byte
}

func (c *cmdInspMsgT) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.SetSynopsis("<hex-string>")
}

func (c *cmdInspMsgT) Parse(args []string) (err error) {
	if err = c.FlagSet.Parse(args); err != nil {
		return
	}
	n := c.NArg()
	if n < 1 {
		err = fmt.Errorf("missing hex msg")
		return
	}
	if c.msg, err = hex.DecodeString(c.Arg(0)); err != nil {
		return
	}
	return
}

func (c *cmdInspMsgT) Exec() {
	c.Validate()

	buf := bytes.NewBuffer(c.msg)

	var raw proto.RawMessage
	if _, err := raw.Read(buf); err != nil {
		fmt.Println(err)
		return
	}
	raw.HexDump()
	var opMsg proto.OperationalMessage
	opMsg.Decode(&raw)
	opMsg.PrettyPrint(os.Stdout)
}

func init() {
	c := &cmdInspMsgT{}
	c.Init("inspect", "check juno binary message, ...")

	cmd.Register(c)
}
