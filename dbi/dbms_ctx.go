package dbi

import (
	"context"

	"github.com/r-che/dfi/types"

	"github.com/r-che/log"
)

type DBMSCtx interface {
	Update(string,*types.FSObject) error
	Delete(string) error
	Commit() (int64, int64, error)
	Stop()

	logDbg(string, ...any)
	logInf(string, ...any)
	logWrn(string, ...any)
	logErr(string, ...any)
	logFat(string, ...any)
}

type DBMSContext struct {
	ctx context.Context
	stop context.CancelFunc

	logID		string

	updated		int64
	deleted		int64
}

func (dc *DBMSContext) logSetup(name, suff string) {
	// Setup log ID
	if suff == "" {
		dc.logID = "(" + name + ") "
	} else {
		dc.logID = "(" + name + ":" + suff + ") "
	}
}

func (dc *DBMSContext) Stop() {
	dc.stop()
}

func (dc *DBMSContext) logDbg(format string, v ...any) {
	log.D(dc.logID + format, v...)
}

func (dc *DBMSContext) logInf(format string, v ...any) {
	log.I(dc.logID + format, v...)
}

func (dc *DBMSContext) logWrn(format string, v ...any) {
	log.W(dc.logID + format, v...)
}

func (dc *DBMSContext) logErr(format string, v ...any) {
	log.E(dc.logID + format, v...)
}

func (dc *DBMSContext) logFat(format string, v ...any) {
	log.F(dc.logID + format, v...)
}
