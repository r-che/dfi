//go:build dbi_redis
package dbi

import (
	"fmt"
	"context"
	"strconv"

	"github.com/r-che/dfi/types"

	"github.com/r-che/log"

	"github.com/go-redis/redis/v8"
)

const RedisObjPrefix	=	"obj:"

type RedisClient struct {
	// Pre-configured members
	ctx		context.Context
	stop	context.CancelFunc
	cliHost	string

	c		*redis.Client

	// Dynamic members
	toDelete	[]string
	updated		int64
	deleted		int64
}

func newDBClient(dbCfg *DBConfig) (DBClient, error) {
	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(dbCfg.DBID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("RedisCtx: cannot convert database identifier value to unsigned integer: %v", err)
	}

	// Initialize Redis client
	rc := &RedisClient{
		c: redis.NewClient(&redis.Options{
			Addr:		dbCfg.HostPort,
			Password:	dbCfg.Password,
			DB:			int(dbid),
		}),
		cliHost:	dbCfg.CliHost,
	}

	// Separate context for redis client
	rc.ctx, rc.stop = context.WithCancel(context.Background())

	return rc, nil
}

func (rc *RedisClient) Update(fso *types.FSObject) error {
	// Make a key
	key := RedisObjPrefix + rc.cliHost + ":" + fso.FPath

	log.D("(DBC) HSET => %s\n", key)

	res := rc.c.HSet(rc.ctx, key, prepareHSetValues(fso))
	if err := res.Err(); err != nil {
		return fmt.Errorf("HSET of key %q returned error: %v", key, err)
	}

	rc.updated++

	// OK
	return nil
}

func (rc *RedisClient) Delete(fso *types.FSObject) error {
	// Make a key
	key := RedisObjPrefix + rc.cliHost + ":" + fso.FPath

	log.D("(DBC) DEL (pending) => %s\n", key)

	// Append key to delete
	rc.toDelete = append(rc.toDelete, key)

	// OK
	return nil
}

func (rc *RedisClient) Commit() (int64, int64, error) {
	// Reset state on return
	defer func() {
		// Reset counters
		rc.updated = 0
		rc.deleted = 0
		// Reset list to delete
		rc.toDelete = nil
	}()

	// Check for keys to delete
	if nDel := len(rc.toDelete); nDel != 0 {
		log.D("(RedisCli) Need to delete %d keys", nDel)

		res := rc.c.Del(rc.ctx, rc.toDelete...)
		if err := res.Err(); err != nil {
			return rc.updated, res.Val(), fmt.Errorf("DEL operation failed: %v", err)
		}

		rc.deleted = res.Val()

		log.D("(RedisCli) Done deletion operation")
	}

	// XXX Use intermediate variables to avoid resetting return values by deferred function
	ru, rd := rc.updated, rc.deleted

	return ru, rd, nil
}

func (rc *RedisClient) Stop() {
	rc.stop()
}

// Auxiliary functions

func prepareHSetValues(fso *types.FSObject) []string {
	// Output slice with values prepared to send to Redis
	values := make([]string, 0, types.FSObjectFieldsNum + 1)	// + 1 - id field

	values = append(values,
		FieldID, makeID(fso),
		FieldName, fso.Name,
		FieldFPath, fso.FPath,
		FieldRPath, fso.RPath,
		FieldType, fso.Type,
		FieldSize, strconv.FormatInt(fso.Size, 10),
		FieldChecksum, fso.Checksum,
	)

	return values
}
