//go:build dbi_redis
package dbi

import (
    "context"
	"fmt"
	"strconv"
	"sync"

	"github.com/r-che/dfi/types"

    "github.com/go-redis/redis/v8"
)

const (
	redisLogID	=	"RedisCtrl"
	ObjPrefix	=	"obj:"
)

func InitController(ctx context.Context, hostname string, dbc *DBConfig, dbChan <-chan []*DBOperation) (DBContrFunc, error) {
	// Set Redis controller log identifier
	var logIDSuff string
	if v := ctx.Value(types.CtxLogIdSuff); v != nil {
		logIDSuff = v.(string)
	}
	setLogID(redisLogID, logIDSuff)

	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(dbc.DBID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cannot convert database identifier value to unsigned integer: %v", err)
	}
	// Initialize Redis client
    rc := redis.NewClient(&redis.Options{
        Addr:     dbc.HostPort,
        Password: dbc.Password,
        DB:       int(dbid),
    })
	// Separate context for redis client
	ctxR, rCliStop := context.WithCancel(context.Background())

	logInf("Database controller created")

	return func() {
		// Get waitgroup from context
		wg := ctx.Value(types.CtxWGDBC).(*sync.WaitGroup)

		logInf("Database controller started ")

		for {
			select {
				// Wait for set of values from watchers
				case dbOps := <-dbChan:
					// Process database operations
					if err := updateRedis(ctxR, hostname, rc, dbOps); err != nil {
						logErr("Update operations failed: %v", err)
					}
				// Wait for finish signal from context
				case <-ctx.Done():
					// Cancel Redis client
					rCliStop()

					logInf("Database controller finished")
					// Signal that this goroutine is finished
					wg.Done()

					// Exit from controller goroutine
					return
			}
		}
	}, nil
}

func updateRedis(ctx context.Context, hostname string, rc *redis.Client, dbOps []*DBOperation) error {
	// Update data in Redis
	logInf("Processing %d keys ...", len(dbOps))

	// List of keys to delete
	delKeys := []string{}

	updated, deleted := 0, int64(0)
	defer func() {
		logInf("%d keys updated, %d keys deleted", updated, deleted)
	}()

	for _, op := range dbOps {
		// Make key
		key := ObjPrefix + hostname + ":" + op.ObjectInfo.FPath
		logDbg("%v => %s\n", op.Op, key)

		switch op.Op {
			case Update:
				res := rc.HSet(ctx, key, prepareValues(op.ObjectInfo))
				if err := res.Err(); err != nil {
					return fmt.Errorf("HSET of key %q returned error: %v", key, err)
				}
				updated++
			case Delete:
				// Add key to delete list
				delKeys = append(delKeys, key)
		}
	}

	// Check for keys to delete
	if nDel := len(delKeys); nDel != 0 {
		logDbg("Need to delete %d keys", nDel)

		res := rc.Del(ctx, delKeys...)
		if err := res.Err(); err != nil {
			return fmt.Errorf("DEL operation failed: %v", err)
		}

		deleted = res.Val()

		logDbg("Done deletion operation")
	}

	// OK
	return nil
}

func prepareValues(fso *types.FSObject) []string {
	// Output slice with values prepared to send to Redis
	values := make([]string, 0, types.FSObjectFieldsNum + 1)	// + 1 - id field

	values = append(values, FieldID, makeID(fso))
	values = append(values, FieldName, fso.Name)
	values = append(values, FieldFPath, fso.FPath)
	values = append(values, FieldRPath, fso.RPath)
	values = append(values, FieldType, fso.Type)
	values = append(values, FieldSize, strconv.FormatInt(fso.Size, 10))
	values = append(values, FieldChecksum, fso.Checksum)

	return values
}
