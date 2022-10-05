//go:build dbi_redis
package redis

import (
	"fmt"
	"context"
	"strconv"
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/dbi/common"

	"github.com/r-che/log"

	"github.com/go-redis/redis/v8"
)

const (
	RedisMaxScanKeys	=	1024 * 10

	// Redis namespace prefixes
	RedisObjPrefix		=	"obj:"
	RedisAIIPrefix		=	"aii:"
	RedisAIIDMetaPefix	=	"aii-meta:"
	RedisAIIDSetPrefix	=	RedisAIIDMetaPefix + "set-"

	// Private configuration fields
	userField	=	"user"
	uassField	=	"password"

	// Error value of redis.Get* function when requested data is not found
	RedisNotFound	=	redis.Nil
)

type RedisClient struct {
	// Pre-configured members
	ctx			context.Context
	stop		context.CancelFunc
	cliHost		string
	// Provided configuration
	cfg			*dbms.DBConfig

	c		*redis.Client

	// Dynamic members
	toDelete	[]string
	updated		int64
	deleted		int64
	stopLongVal int		// should be incremented when need to terminate long-term operation
}

func NewClient(dbCfg *dbms.DBConfig) (dbms.Client, error) {
	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(dbCfg.ID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli) cannot convert database identifier value to unsigned integer: %v", err)
	}

	// Read username/password from private data if set
	user, passw, err := userPasswd(dbCfg.PrivCfg)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli) failed to load username/password from private configuration: %v", err)
	}

	// Initialize Redis client
	rc := &RedisClient{
		cfg: dbCfg,
		c: redis.NewClient(&redis.Options{
			Addr:		dbCfg.HostPort,
			Username:	user,
			Password:	passw,
			DB:			int(dbid),
		}),
		cliHost:	dbCfg.CliHost,
	}

	// Separate context for redis client
	rc.ctx, rc.stop = context.WithCancel(context.Background())

	return rc, nil
}

func userPasswd(pcf map[string]any) (string, string, error) {
	// Check for empty configuration
	if pcf == nil {
		// OK, just return nothing
		return "", "", nil
	}


	loadField := func(field string) (string, error) {
		v, ok := pcf[field]
		if !ok {
			return "", fmt.Errorf("(RedisCli) private configuration does not contain %q field", field)
		}
		if user, ok := v.(string); ok {
			return user, nil
		}
		return "", fmt.Errorf(`(RedisCli) invalid type of %q field in private configuration, got %T, wanted string`,
			field, v)
	}

	// Extract username/password values
	user, err := loadField(userField)
	if err != nil {
		return "", "", err
	}

	passwd, err := loadField(uassField)
	if err != nil {
		return "", "", err
	}

	return user, passwd, nil
}

func (rc *RedisClient) UpdateObj(fso *types.FSObject) error {
	// Make a key
	key := RedisObjPrefix + rc.cliHost + ":" + fso.FPath

	log.D("(RedisCli) HSET => %s\n", key)

	res := rc.c.HSet(rc.ctx, key, prepareHSetValues(rc.cliHost, fso))
	if err := res.Err(); err != nil {
		return fmt.Errorf("HSET of key %q returned error: %v", key, err)
	}

	rc.updated++

	// OK
	return nil
}

func (rc *RedisClient) DeleteObj(fso *types.FSObject) error {
	// Make a key
	key := RedisObjPrefix + rc.cliHost + ":" + fso.FPath

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

func (rc *RedisClient) StopLong() {
	rc.stopLongVal++
}

func (rc *RedisClient) Stop() {
	rc.stop()
}

func (rc *RedisClient) LoadHostPaths(filter dbms.FilterFunc) ([]string, error) {
	// Make prefix of objects keys
	pref := RedisObjPrefix + rc.cliHost + ":*"

	// Output list of keys of objects belong to the host
	hostKeys := []string{}
	// Calculate path offset to append paths to the output list
	pathOffset:= len(pref) - 1

	// Keep current stopLong value to have ability to compare during long-term operations
	initStopLong := rc.stopLongVal

	// Scan() intermediate  variables
	var cursor uint64
	var sKeys []string
	var err error

	log.D("(RedisCli) Scanning DB for keys with prefix %q, using %d as COUNT value for SCAN operation", pref, RedisMaxScanKeys)
	// Scan keys space prefixed by pref
	for i := 0; ; i++ {
		// If value of the stopLong was updated - need to stop long-term operation
		if rc.stopLongVal != initStopLong {
			return nil, fmt.Errorf("terminated")
		}

		// Scan for RedisMaxScanKeys items (max)
		sKeys, cursor, err = rc.c.Scan(rc.ctx, cursor, pref, RedisMaxScanKeys).Result()
		if err != nil {
			return nil, err
		}

		// Append scanned keys to the resulted list as set of paths without prefix
		for _, k := range sKeys {
			// Append only filtered values
			if path := k[pathOffset:]; filter(path) {
				hostKeys = append(hostKeys, path)
			}
		}

		// Is the end of keys space reached
		if cursor == 0 {
			// Return resulted data
			log.D("(RedisCli) Scan for keys prefixed by %q finished, scans number %d, %d keys filtered", pref, i, len(hostKeys))
			return hostKeys, nil
		}
	}
}

// Auxiliary functions

func prepareHSetValues(host string, fso *types.FSObject) []string {
	// Output slice with values prepared to send to Redis
	values := make([]string, 0, types.FSObjectFieldsNum + 2)	// + 2 - id field + host field

	/*
	 * Prepare FPath value
	 */
	// XXX Convert of found path value to lowercase because RediSearch
	// XXX does not fully support case insensitivity for non-English locales
	fpathPrepared := strings.ToLower(fso.FPath)
	// Replace underscores by spaces to improve RediSearch full-text search results
	// due to default tokenizator does not use underscores as separator[1]
	// [1]https://redis.io/docs/stack/search/reference/escaping/
	fpathPrepared = strings.ReplaceAll(fpathPrepared, "_", " ")
	// Do the same for the name field
	namePrepared := strings.ReplaceAll(strings.ToLower(fso.Name), "_", " ")

	values = append(values,
		dbms.FieldID, common.MakeID(host, fso),
		dbms.FieldHost, host,
		dbms.FieldName, namePrepared,
		dbms.FieldFPath, fpathPrepared,
		dbms.FieldRPath, fso.RPath,
		dbms.FieldType, fso.Type,
		dbms.FieldSize, strconv.FormatInt(fso.Size, 10),
		dbms.FieldMTime, strconv.FormatInt(fso.MTime, 10),
		dbms.FieldChecksum, fso.Checksum,
	)

	return values
}
