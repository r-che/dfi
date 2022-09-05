//go:build dbi_redis
package dbi

import (
    "context"
	"fmt"
	"strconv"
	//"sync"

	"github.com/r-che/dfi/types"

    "github.com/go-redis/redis/v8"
)

const (
	redisLogID	=	"RedisCtrl"
	ObjPrefix	=	"obj:"
)

type RedisCtx struct {
	DBMSContext

	cli *redis.Client

	toDelete	[]string
}

func NewController(ctx context.Context, hostname string, dbc *DBConfig, dbChan <-chan []*DBOperation) (*DBController, error) {
	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(dbc.DBID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cannot convert database identifier value to unsigned integer: %v", err)
	}

	// Initialize Redis client
    rc := RedisCtx {
		cli: redis.NewClient(&redis.Options{
			Addr:     dbc.HostPort,
			Password: dbc.Password,
			DB:       int(dbid),
		}),
	}
	rc.logSetup(redisLogID, "")

	// Separate context for redis client
	rc.ctx, rc.stop = context.WithCancel(context.Background())

	return initController(ctx, redisLogID, hostname, &rc)
}

func (rc *RedisCtx) Update(key string, fso *types.FSObject) error {
	res := rc.cli.HSet(rc.ctx, key, prepareValues(fso))
	if err := res.Err(); err != nil {
		return fmt.Errorf("HSET of key %q returned error: %v", key, err)
	}

	rc.updated++

	// OK
	return nil
}

func (rc *RedisCtx) Delete(key string) error {
	// Append key to delete
	rc.toDelete = append(rc.toDelete, key)

	// OK
	return nil
}

func (rc *RedisCtx) Commit() (int64, int64, error) {
	// Check for keys to delete
	if nDel := len(rc.toDelete); nDel != 0 {
		rc.logDbg("Need to delete %d keys", nDel)

		res := rc.cli.Del(rc.ctx, rc.toDelete...)
		if err := res.Err(); err != nil {
			return rc.updated, res.Val(), fmt.Errorf("DEL operation failed: %v", err)
		}

		rc.deleted = res.Val()

		rc.logDbg("Done deletion operation")
	}

	ru, rd := rc.updated, rc.deleted

	// Reset counters
	rc.updated = 0
	rc.deleted = 0
	// Reset list to delete
	rc.toDelete = nil

	return ru, rd, nil
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
