//go:build dbi_redis
package dbi

import (
    "context"
    "fmt"
	"sync"

	"github.com/r-che/dfi/types"
	"github.com/r-che/log"

    "github.com/go-redis/redis/v8"
)

const (
	ObjsMetaIdx	=	"obj-meta-idx"	// Objects metadata index for RediSearch
)


func InitController(ctx context.Context, dbc *DBConfig, dbChan <-chan []*DBOperation) (DBContrFunc, error) {
	// Initialize Redis client
    rc := redis.NewClient(&redis.Options{
        Addr:     dbc.HostPort,
        Password: dbc.Password,
        DB:       0,  // TODO Need to select DB from additional DB configuration
    })

	log.I("Redis database controller created")

	return func() {
		// Get waitgroup from context
		wg := ctx.Value(types.CtxWGDBC).(*sync.WaitGroup)

		log.I("Redis database controller started ")

		for {
			select {
				// Wait for set of values from watchers
				case dbOps := <-dbChan:
					// Process database operations
					if err := updateRedis(ctx, rc, dbOps); err != nil {
						log.E("Redis update failed: %v", err)
					}
				// Wait for finish signal from context
				case <-ctx.Done():
					wg.Done()
					log.I("Redis database controller finished")

					// Exit from controller go
					return
			}
		}
	}, nil
}

func updateRedis(ctx context.Context, rc *redis.Client, dbOps []*DBOperation) error {
	// TODO
	fmt.Println("dbOps:", dbOps)

	// OK
	return nil
}
