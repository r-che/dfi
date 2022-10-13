//go:build dbi_redis
package redis

import (
	"fmt"
	"context"
	"strconv"
	"strings"
	"errors"

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
	readOnly	bool
	// Provided configuration
	cfg			*dbms.DBConfig

	c		*redis.Client

	// Dynamic members
	toDelete	[]string
	updated		int64
	deleted		int64
	termLongVal int		// should be incremented when need to terminate long-term operation
}

func NewClient(dbCfg *dbms.DBConfig) (*RedisClient, error) {
	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(dbCfg.ID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:NewClient) cannot convert database identifier value to unsigned integer: %w", err)
	}

	// Read username/password from private data if set
	user, passw, err := userPasswd(dbCfg.PrivCfg)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:NewClient) failed to load username/password from private configuration: %w", err)
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
			return "", fmt.Errorf("(RedisCli:userPasswd) private configuration does not contain %q field", field)
		}
		if user, ok := v.(string); ok {
			return user, nil
		}
		return "", fmt.Errorf(`(RedisCli:userPasswd) invalid type of %q field in private configuration, got %T, wanted string`,
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


	if rc.readOnly {
		// Read-only datbase mode, do nothing
		log.W("(RedisCli:UpdateObj) R/O mode IS SET, will not be performed: HSET => %s\n", key)
	} else {
		log.D("(RedisCli:UpdateObj) HSET => %s\n", key)
		// Do real update
		res := rc.c.HSet(rc.ctx, key, prepareHSetValues(rc.cliHost, fso))
		if err := res.Err(); err != nil {
			return fmt.Errorf("(RedisCli:UpdateObj) HSET of key %q returned error: %w", key, err)
		}
	}

	rc.updated++

	// OK
	return nil
}

func (rc *RedisClient) DeleteObj(fso *types.FSObject) error {
	// Make a key
	key := RedisObjPrefix + rc.cliHost + ":" + fso.FPath

	if rc.readOnly {
		log.W("(RedisCli:DeleteObj) R/O mode IS SET, will not be performed: DEL => %s\n", key)
	} else {
		log.D("(RedisCli:DeleteObj) DEL (pending) => %s\n", key)
	}

	// XXX Append key to delete regardless of R/O mode because it will be skipped in the Commit() operation
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
		log.D("(RedisCli:Commit) Need to delete %d keys", nDel)

		if rc.readOnly {
			// Read-only datbase mode, count numbers of keys that would have been deleted on normal mode
			wd := []string{}	// would be deleted
			nd := []string{}	// will not be deleted

			for _, key := range rc.toDelete {
				res := rc.c.HGet(rc.ctx, key, dbms.FieldID)
				err := res.Err()
				if err == nil {
					// Ok, key will be deleted
					wd = append(wd, key)
					// Try to check the next key
					continue
				}

				//
				// Cannot delete this key, inspect why
				//

				// Check kind of error
				if errors.Is(err, RedisNotFound) {
					log.E("(RedisCli:Commit) HGET (used instead of DEL on R/O mode) for key %q failed: key is not found", key)
				} else {
					// Unknown error
					log.E("(RedisCli:Commit) HGET (used instead of DEL on R/O mode) for key %q failed: %v", key, err)
				}

				// Anyway - key will NOT be deleted
				nd = append(nd, key)
			}

			// Update deleted counter by number of selected keys that would be deleted
			rc.deleted = int64(len(wd))

			// Check for keys that would be deleted
			if len(wd) != 0 {
				// Print warning message about these keys
				log.W("(RedisCli:Commit) R/O mode - DO NOT delete %d keys because  %v", len(wd), wd)
			}

			// Check for keys that would not be deleted
			if len(nd) != 0 {
				// Print warning
				log.W("(RedisCli:Commit) R/O mode - DEL could NOT delete %d keys because not exist or other errors: %v", len(nd), nd)
			}

		} else {
			res := rc.c.Del(rc.ctx, rc.toDelete...)
			if err := res.Err(); err != nil {
				return rc.updated, res.Val(), fmt.Errorf("(RedisCli:Commit) DEL operation failed: %w", err)
			}

			rc.deleted = res.Val()
		}


		log.D("(RedisCli:Commit) Done deletion operation")
	}

	// XXX Use intermediate variables to avoid resetting return values by deferred function
	ru, rd := rc.updated, rc.deleted

	return ru, rd, nil
}

func (rc *RedisClient) SetReadOnly(ro bool) {
	log.W("(RedisClient:SetReadOnly) Set database read-only flag to: %v", ro)
	rc.readOnly = true
}

func (rc *RedisClient) TermLong() {
	rc.termLongVal++
}

func (rc *RedisClient) Stop() {
	rc.stop()
}

func (rc *RedisClient) LoadHostPaths(match dbms.MatchStrFunc) ([]string, error) {
	// Make prefix of objects keys
	pref := RedisObjPrefix + rc.cliHost + ":*"

	// Output list of keys of paths belong to the host
	hostPaths := []string{}
	// Calculate path offset to append paths to the output list
	pathOffset:= len(pref) - 1

	// Keep current termLong value to have ability to compare during long-term operations
	initTermLong := rc.termLongVal

	// Scan() intermediate  variables
	var cursor uint64
	var sKeys []string
	var err error

	log.D("(RedisCli:LoadHostPaths) Scanning DB for keys with prefix %q, using %d as COUNT value for SCAN operation", pref, RedisMaxScanKeys)
	// Scan keys space prefixed by pref
	for i := 0; ; i++ {
		// If value of the termLong was updated - need to terminate long-term operation
		if rc.termLongVal != initTermLong {
			return nil, fmt.Errorf("(RedisCli:LoadHostPaths) terminated")
		}

		// Scan for RedisMaxScanKeys items (max)
		sKeys, cursor, err = rc.c.Scan(rc.ctx, cursor, pref, RedisMaxScanKeys).Result()
		if err != nil {
			return nil, err
		}

		// Append scanned keys to the resulted list as set of paths without prefix
		for _, k := range sKeys {
			// Append only matched values
			if path := k[pathOffset:]; match(path) {
				hostPaths = append(hostPaths, path)
			}
		}

		// Is the end of keys space reached
		if cursor == 0 {
			// Return resulted data
			log.D("(RedisCli:LoadHostPaths) Scan for keys prefixed by %q finished, scans number %d, %d keys matched", pref, i, len(hostPaths))
			return hostPaths, nil
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
