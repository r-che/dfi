package dbi

import (
	"context"
	"sync"

	"github.com/r-che/dfi/types"

	"github.com/r-che/log"
)

type DBController struct {
	// Internal fields
	ctx context.Context

	dbChan		DBChan

	dbCli		DBClient
}

func NewController(ctx context.Context, dbCfg *DBConfig, dbChan DBChan) (*DBController, error) {
	// Initiate database client
	dbCli, err := newDBClient(dbCfg)
	if err != nil {
		return nil, err
	}

	return &DBController{
		ctx:		ctx,
		dbChan:		dbChan,
		dbCli:		dbCli,
	}, nil
}

func (dbc *DBController) Run() {
	log.I("(DBC) Database controller started ")

	// Start DB events loop
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
				dbc.ctx.Value(types.CtxWGDBC).(*sync.WaitGroup).Done()

				log.I("(DBC) Database controller finished")

				// Exit from database controler loop
				return
		}
	}

}

func (dbc *DBController) update(dbOps []*DBOperation) error {

    for _, op := range dbOps {
        switch op.Op {
            case Update:
                // Add/update data in DB
                if err := dbc.dbCli.UpdateObj(op.ObjectInfo); err != nil {
                    return err
                }
            case Delete:
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
