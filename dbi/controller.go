package dbi

import (
	"context"
	"sync"

	"github.com/r-che/log"
	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
)

type DBController struct {
	// Internal fields
	ctx context.Context

	dbChan		dbms.DBChan

	dbCli		dbms.ClientController

	wg			*sync.WaitGroup
	cancel		context.CancelFunc
	termLongVal int		// should be incremented when need to terminate long-term operation
}

func NewController(dbCfg *dbms.DBConfig) (*DBController, error) {
	// Initiate database client
	dbCli, err := NewClientController(dbCfg)
	if err != nil {
		return nil, err
	}

	// Context to stop database controller
	ctx, cancel := context.WithCancel(context.Background())

	return &DBController{
		ctx:		ctx,
		dbChan:		make(dbms.DBChan),
		dbCli:		dbCli,
		wg:			&sync.WaitGroup{},
		cancel:		cancel,
	}, nil
}

// TermLong terminates long-term operations on database
func (dbc *DBController) TermLong() {
	dbc.termLongVal++
	dbc.dbCli.TermLong()
}

func (dbc *DBController) Stop() {
	log.D("Stopping database controller...")

	// Cancel on context to stop related DB activities
	dbc.cancel()

	// Wait for finishing
	dbc.wg.Wait()
}

func (dbc *DBController) Run() {
	log.I("(DBC) Database controller started ")

	// Increment WaitGroup BEFORE start separate goroutine
	dbc.wg.Add(1)

	// Start DB events loop
	go func() {
		for {
			select {
				// Wait for set of values from watchers
				case dbOps := <-dbc.dbChan:
					// Process database operations
					rv := dbc.update(dbOps)
					if !rv.OK() {
						log.E("(DBC) Update operations returned %d errors: {ERROR: %s}",
							len(rv.Errs()), rv.ErrsJoin("}, {ERROR: "))
					}

					log.I("(DBC) Completed %d operations", rv.Changed())
				// Wait for finish signal from context
				case <-dbc.ctx.Done():
					// Stop DB client
					dbc.dbCli.Stop()

					// Call waitgroup from context
					dbc.wg.Done()

					log.I("(DBC) Database controller finished")

					// Exit from database controler loop
					return
			}
		}
	}()
}

func (dbc *DBController) Channel() dbms.DBChan {
	return dbc.dbChan
}

func (dbc *DBController) SetReadOnly(v bool) {
	dbc.dbCli.SetReadOnly(v)
}

func (dbc *DBController) update(dbOps []*dbms.DBOperation) *types.CmdRV {
	// Summary return value
	rv := types.NewCmdRV()

	// Keep current termLong value to have ability to compare during long-term updates
	initTermLong := dbc.termLongVal

	// Counter for objects that have to be deleted
	toDelN := int64(0)
    for _, op := range dbOps {
		// If value of the termLong was updated - need to stop long-term update
		if dbc.termLongVal != initTermLong {
			rv.AddErr("terminated")
			// Break loop to commit
			break
		}

        switch op.Op {
		// Object need to be updated
		case dbms.Update:
			// Add/update data in DB
			if err := dbc.dbCli.UpdateObj(op.ObjectInfo); err != nil {
				rv.AddErr(err)
			}

		// Object need to be deleted
		case dbms.Delete:
			// Delete data from DB
			if err := dbc.dbCli.DeleteObj(op.ObjectInfo); err != nil {
				rv.AddErr(err)
			} else {
				// Increase number of objects for deletion
				toDelN++
			}
        }
    }

    // Commit operations
    updated, deleted, err := dbc.dbCli.Commit()
	if err != nil {
		return rv.AddErr(err)
	}

	// Check for not frequent, but probably situation
	if deleted != toDelN {
		// Print explanation message
		log.W("(DBC) %d objects were expected to be removed, but removed %d. This may be because" +
			" when some objects were added and deleted before they were added to DB",
			toDelN, deleted)
	}

	log.I("(DBC) %d records updated, %d records deleted", updated, deleted)

	// Set number of changed objects
	rv.AddChanged(updated + deleted)

	// Done
	return rv
}
