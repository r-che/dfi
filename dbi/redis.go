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

const (
	RedisMaxScanKeys	=	1024 * 10
	RedisObjPrefix		=	"obj:"
)

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
		return nil, fmt.Errorf("(RedisCli) cannot convert database identifier value to unsigned integer: %v", err)
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

func (rc *RedisClient) UpdateObj(fso *types.FSObject) error {
	// Make a key
	key := RedisObjPrefix + rc.cliHost + ":" + fso.FPath

	log.D("(RedisCli) HSET => %s\n", key)

	res := rc.c.HSet(rc.ctx, key, prepareHSetValues(fso))
	if err := res.Err(); err != nil {
		return fmt.Errorf("HSET of key %q returned error: %v", key, err)
	}

	rc.updated++

	// OK
	return nil
}

func (rc *RedisClient) DeleteObj(fso *types.FSObject) error {
	return rc.Delete(fso.FPath)
}

func (rc *RedisClient) Delete(path string) error {
	// Make a key
	key := RedisObjPrefix + rc.cliHost + ":" + path

	log.D("(RedisCli) DEL (pending) => %s\n", key)

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

func (rc *RedisClient) LoadHostPaths() ([]string, error) {
	// Make prefix of objects keys
	pref := RedisObjPrefix + rc.cliHost + ":*"

	// Output list of keys of objects belong to the host
	hostKeys := []string{}
	// Calculate path offset to append paths to the output list
	pathOffset:= len(pref) - 1

	// Scan() intermediate  variables
	var cursor uint64
	var sKeys []string
	var err error

	log.D("(RedisCli) Scanning DB for keys with prefix %q, using %d as COUNT value for SCAN operation", pref, RedisMaxScanKeys)
	// Scan keys space prefixed by pref
	for {
		// Scan for RedisMaxScanKeys items (max)
		sKeys, cursor, err = rc.c.Scan(rc.ctx, cursor, pref, RedisMaxScanKeys).Result()
		if err != nil {
			return nil, err
		}

		// Append scanned keys to the resulted list as set of paths without prefix
		for _, k := range sKeys {
			hostKeys = append(hostKeys, k[pathOffset:])
		}

		// Is the end of keys space reached
		if cursor == 0 {
			// Return resulted data
			log.D("(RedisCli) Scan for keys prefixed by %q finished, %d keys obtained", pref, len(hostKeys))
			return hostKeys, nil
		}
	}
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
