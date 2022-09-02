//go:build dbi_redis
package dbi

import (
    "context"
	"strconv"
	"sync"

	"github.com/r-che/dfi/types"
	"github.com/r-che/log"

    "github.com/go-redis/redis/v8"
)

const (
	ObjPrefix	=	"obj:"
)

func InitController(ctx context.Context, hostname string, dbc *DBConfig, dbChan <-chan []*DBOperation) (DBContrFunc, error) {
	// Initialize Redis client
    rc := redis.NewClient(&redis.Options{
        Addr:     dbc.HostPort,
        Password: dbc.Password,
        DB:       0,  // TODO Need to select DB from additional DB configuration
    })

	log.I("(Redis) Database controller created")

	return func() {
		// Get waitgroup from context
		wg := ctx.Value(types.CtxWGDBC).(*sync.WaitGroup)

		log.I("(Redis) Database controller started ")

		for {
			select {
				// Wait for set of values from watchers
				case dbOps := <-dbChan:
					// Process database operations
					if err := updateRedis(ctx, hostname, rc, dbOps); err != nil {
						log.E("(Redis) Update operations failed: %v", err)
					}
				// Wait for finish signal from context
				case <-ctx.Done():
					wg.Done()
					log.I("(Redis) Database controller finished")

					// Exit from controller go
					return
			}
		}
	}, nil
}

func updateRedis(ctx context.Context, hostname string, rc *redis.Client, dbOps []*DBOperation) error {
	// Update data in Redis
	log.I("(Redis) Updating %d keys ...", len(dbOps))

	for _, op := range dbOps {
		// Make key
		key := ObjPrefix + hostname + ":" + op.ObjectInfo.FPath
		log.D("(Redis) %v => %s\n", op.Op, key)

		res := rc.HSet(ctx, key, prepareValues(op.ObjectInfo))
		if err := res.Err(); err != nil {
			return err
		}
	}

	log.I("(Redis) Update finished")

	// OK
	return nil
}

func prepareValues(fso *types.FSObject) []string {
	// Output slice with values prepared to send to Redis
	values := make([]string, 0, types.FSObjectFieldsNum + 1)	// + 1 - id field

	// Append mandatory fields
	values = append(values, FieldID, makeID(fso))
	values = append(values, FieldName, fso.Name)
	values = append(values, FieldFPath, fso.FPath)
	values = append(values, FieldType, fso.Type)
	values = append(values, FieldSize, strconv.FormatInt(fso.Size, 10))

	// Append optional fields
	if fso.RPath != "" {
		values = append(values, FieldRPath, fso.RPath)
	}
	if fso.Checksum != "" {
		values = append(values, FieldChecksum, fso.Checksum)
	}

	return values
}
