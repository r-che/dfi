package dbms

import (
	"context"

	"github.com/r-che/log"
)

// Fields and methods common to all clients
type CommonClient struct {
	Ctx			context.Context
	stop		context.CancelFunc

	// Values configured on startup
	Cfg			*DBConfig	// database configuration (auth, connection, etc...)
	CliHost		string		// host where client runs

	// Runtime configured values
	ReadOnly	bool

	TermLongVal int		// should be incremented when need to terminate long-term operation
}

func NewCommonClient(dbCfg *DBConfig, cliHost string) *CommonClient {
	cc := &CommonClient{
		Cfg:		dbCfg,
		CliHost:	cliHost,
	}

	// Separate context for DB client
	cc.Ctx, cc.stop = context.WithCancel(context.Background())

	return cc
}

func (cc *CommonClient) SetReadOnly(ro bool) {
	log.W("(%sCli:SetReadOnly) Set database read-only flag to: %v", Backend, ro)
	cc.ReadOnly = true
}

func (cc *CommonClient) TermLong() {
	log.W("(%sCli:TermLong) Terminating long operations...", Backend)
	cc.TermLongVal++
}

func (cc *CommonClient) Stop() {
	cc.stop()
}
