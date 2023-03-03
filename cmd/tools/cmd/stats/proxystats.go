package stats

import (
	"fmt"
	"os"

	"juno/cmd/proxy/stats/shmstats"
	"juno/pkg/cmd"
)

var _ cmd.ICommand = (*CmdProxyStats)(nil)

type CmdProxyStats struct {
	cmd.Command
	optPid      int
	optWorkerId string
}

func (c *CmdProxyStats) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.IntOption(&c.optPid, "p|pid", -1, "specify the pid of the proxy")
	c.StringOption(&c.optWorkerId, "w|worker-id", "", "specify worker id. print stats for all workers, if \"all\"")
}

func (c *CmdProxyStats) Parse(args []string) (err error) {
	if err = c.Option.Parse(args); err != nil {
		return
	}
	if c.optPid == -1 {
		err = fmt.Errorf("specify a valid proxy pid")
		return
	}
	return
}

func (c *CmdProxyStats) Exec() {
	if err := shmstats.InitForRead(c.optPid); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	shmstats.PrettyPrint(os.Stdout, c.optWorkerId)
	shmstats.Finalize()
}
