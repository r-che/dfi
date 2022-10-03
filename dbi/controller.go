package dbi

import (
	"fmt"
	"context"
	"sync"

	"github.com/r-che/log"
	"github.com/r-che/dfi/types/dbms"
)

type DBController struct {
	// Internal fields
	ctx context.Context

	dbChan		dbms.DBChan

	dbCli		dbms.Client

	wg			*sync.WaitGroup
	cancel		context.CancelFunc
	stopLongVal int		// should be incremented when need to terminate long-term operation
}

func NewController(dbCfg *dbms.DBConfig) (*DBController, error) {
	// Initiate database client
	dbCli, err := NewClient(dbCfg)
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

// StopLong stops long-term operations on database
func (dbc *DBController) StopLong() {
	dbc.stopLongVal++
	dbc.dbCli.StopLong()
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
					if err := dbc.update(dbOps); err != nil {
						log.E("(DBC) Update operations failed: %v", err)
					} else {
						log.I("(DBC) Done %d operations", len(dbOps))
					}
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

func (dbc *DBController) update(dbOps []*dbms.DBOperation) error {
	// Keep current stopLong value to have ability to compare during long-term updates
	initStopLong := dbc.stopLongVal

    for _, op := range dbOps {
		// If value of the stopLong was updated - need to stop long-term update
		if dbc.stopLongVal != initStopLong {
			return fmt.Errorf("terminated")
		}

        switch op.Op {
		case dbms.Update:
			// Add/update data in DB
			if err := dbc.dbCli.UpdateObj(op.ObjectInfo); err != nil {
				return err
			}
		case dbms.Delete:
			// Delete data from DB
			if err := dbc.dbCli.DeleteObj(op.ObjectInfo); err != nil {
				return err
			}
        }
    }

    // Commit operations
    updated, deleted, err := dbc.dbCli.Commit()
	if err != nil {
		return err
	}

	log.I("(DBC) %d records updated, %d records deleted", updated, deleted)

	// OK
	return nil
}
