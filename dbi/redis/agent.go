//go:build dbi_redis
package redis

import (
	"fmt"
	"errors"
	"strings"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/common/tools"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
)

func (rc *RedisClient) UpdateObj(fso *types.FSObject) error {
	// Make a key
	key := RedisObjPrefix + rc.Cfg.CliHost + ":" + fso.FPath

	if rc.ReadOnly {
		// Read-only database mode, do nothing
		log.W("(RedisCli:UpdateObj) R/O mode IS SET, will not be performed: HSET => %s\n", key)
	} else {
		log.D("(RedisCli:UpdateObj) HSET => %s\n", key)
		// Do real update
		res := rc.c.HSet(rc.Ctx, key, prepareHSetValues(rc.Cfg.CliHost, fso))
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
	key := RedisObjPrefix + rc.Cfg.CliHost + ":" + fso.FPath

	if rc.ReadOnly {
		log.W("(RedisCli:DeleteObj) R/O mode IS SET, will not be performed: DEL => %s\n", key)
	} else {
		log.D("(RedisCli:DeleteObj) DEL (pending) => %s\n", key)
	}

	// XXX Append key to delete regardless of R/O mode because it will be skipped in the Commit() operation
	rc.toDelete = append(rc.toDelete, key)

	// OK
	return nil
}

func (rc *RedisClient) DeleteFPathPref(fso *types.FSObject) (int64, error) {
	// Make prefix of objects keys
	pref := RedisObjPrefix + rc.Cfg.CliHost + ":" + fso.FPath + "*"

	// Keys to delete using prefix
	toDel := []string{}

	// Load keys
	err := rc.loadKeysByPrefix(pref,
	// Append found key to the list of keys to delete
	func(value any) error {
		key, ok := value.(string)
		// Check for invalid type of key
		if !ok {
			// That should never happen
			panic(fmt.Sprintf("(RedisCli:DeleteFPathPref:appender) non-string key: %#v", value))
		}

		toDel = append(toDel, key)

		// OK
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("(RedisCli:DeleteFPathPref) cannot load keys to delete with prefix %q: %w", pref, err)
	}

	log.D("(RedisCli:DeleteFPathPref) %d matched to delete with prefix %q %s", len(toDel), pref,
		tools.Tern(rc.ReadOnly, "R/O mode IS SET, will not be performed", "(pending)"))

	// Replace/append deletion queue
	rc.toDelete = tools.Tern(len(rc.toDelete) == 0,
		toDel,
		append(rc.toDelete, toDel...))

	// OK
	return int64(len(toDel)), nil
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

	var err error

	// Check for keys to delete
	if nDel := len(rc.toDelete); nDel != 0 {
		log.D("(RedisCli:Commit) Need to delete %d keys", nDel)

		if rc.ReadOnly {
			// Read-only database mode, count numbers of keys that would have been deleted on normal mode
			wd := []string{}	// would be deleted
			nd := []string{}	// will not be deleted

			for _, key := range rc.toDelete {
				res := rc.c.HGet(rc.Ctx, key, dbms.FieldID)
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
				log.W("(RedisCli:Commit) %d key(s) should be deleted but would NOT because R/O mode: %v",
						len(wd), strings.Join(wd, ", "))
			}

			// Check for keys that would not be deleted
			if len(nd) != 0 {
				// Print warning
				log.W("(RedisCli:Commit) R/O mode - DEL could NOT delete %d keys because not exist or other errors: %v",
					len(nd), strings.Join(nd, ", "))
			}

		} else {
			// Delete all keys from rc.toDelete slice
			res := rc.c.Del(rc.Ctx, rc.toDelete...)

			// Check for deletion error
			// XXX Need to use external err variable here to pass error to the function return values
			if err = res.Err(); err != nil {
				// Save error value
				err = fmt.Errorf("(RedisCli:Commit) DEL operation failed: %w", err)
			}

			// Update deleted value
			rc.deleted = res.Val()
		}

		log.D("(RedisCli:Commit) Done deletion operation with results: %v",
			tools.Tern[any](err == nil, "no errors", err))
	}

	// XXX Use intermediate variables to avoid resetting return values by deferred function
	ru, rd := rc.updated, rc.deleted

	return ru, rd, err
}

func (rc *RedisClient) LoadHostPaths(match dbms.MatchStrFunc) ([]string, error) {
	// Make prefix of objects keys
	pref := RedisObjPrefix + rc.Cfg.CliHost + ":*"

	// Output list of keys of paths belong to the host
	hostPaths := []string{}
	// Calculate path offset to append paths to the output list
	pathOffset:= len(pref) - 1

	// Load keys
	err := rc.loadKeysByPrefix(pref,
	// Append found key to the list of found paths
	func(value any) error {
		key, ok := value.(string)
		// Check for invalid type of key
		if !ok {
			// That should never happen
			panic(fmt.Sprintf("(RedisCli:LoadHostPaths:appender) non-string key: %#v", value))
		}

		// Append only matched values
		if path := key[pathOffset:]; match(path) {
			hostPaths = append(hostPaths, path)
		}

		// OK
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:LoadHostPaths) cannot load host paths: %w", err)
	}

	log.D("(RedisCli:LoadHostPaths) %d paths matched the filter", len(hostPaths))

	return hostPaths, nil
}
