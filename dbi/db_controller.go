package dbi

import (
	"context"
	"sync"
	"fmt"

	"github.com/r-che/dfi/types"
)

type DBController struct {
	hostname	string

	ctx			context.Context
	wg			*sync.WaitGroup
	dbctx		DBMSCtx
}

func initController(ctx context.Context, ctrlName, hostname string, dbctx DBMSCtx) (*DBController, error) {
	// Create new controller instance
	ctrl := &DBController{
		ctx:		ctx,
		hostname:	hostname,
		wg:			ctx.Value(types.CtxWGDBC).(*sync.WaitGroup),
		dbctx:		dbctx,
	}

	ctrl.dbctx.logInf("Database controller created")

	return ctrl, nil
}

func (dbc *DBController) update(dbOps []*DBOperation) (int64, int64, error) {

	for _, op := range dbOps {
		// Make key
		key := ObjPrefix + dbc.hostname + ":" + op.ObjectInfo.FPath
		dbc.dbctx.logDbg("%v => %s\n", op.Op, key)

		switch op.Op {
			case Update:
				// Add/update data in DB
				if err := dbc.dbctx.Update(key, op.ObjectInfo); err != nil {
					return 0, 0, fmt.Errorf("update for %q returned error: %v", key, err)
				}
			case Delete:
				// Delete data from DB
				if err := dbc.dbctx.Delete(key); err != nil {
					return 0, 0, fmt.Errorf("delete for %q returned error: %v", key, err)
				}
		}
	}

	// Commit operations
	return dbc.dbctx.Commit()
}


func (dbc *DBController) Run(dbChan <-chan []*DBOperation) {
	dbc.dbctx.logInf("Database controller started ")

	for {
		select {
			// Wait for set of values from watchers
			case dbOps := <-dbChan:
				// Process database operations
				dbc.dbctx.logInf("Processing %d operations ...", len(dbOps))

				updated, deleted, err := dbc.update(dbOps)
				if err != nil {
					dbc.dbctx.logErr("Update operations failed: %v", err)
				}

				dbc.dbctx.logInf("%d records updated, %d records deleted", updated, deleted)

			// Wait for finish signal from context
			case <-dbc.ctx.Done():
				// Cancel DB client
				dbc.dbctx.Stop()

				dbc.dbctx.logInf("Database controller finished")
				// Signal that this goroutine is finished
				dbc.wg.Done()

				// Exit from controller goroutine
				return
		}
	}
}
