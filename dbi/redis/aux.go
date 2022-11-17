//go:build dbi_redis
package redis

import (
	"fmt"
	"strings"
	"strconv"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	"github.com/r-che/dfi/dbi/common"

	"github.com/r-che/log"
)

func (rc *Client) loadKeysByPrefix(prefix string, appendFunc func(any) error) error {
	// Keep current termLong value to have ability to compare during long-term operations
	initTermLong := rc.TermLongVal

	// Scan() intermediate  variables
	var cursor uint64
	var sKeys []string
	var err error

	log.D("(RedisCli:loadKeysByPrefix) Scanning DB for keys with prefix %q, using %d as COUNT value for SCAN operation", prefix, RedisMaxScanKeys)
	// Scan keys space prefixed with pref
	for i := 0; ; i++ {
		// If value of the termLong was updated - need to terminate long-term operation
		if rc.TermLongVal != initTermLong {
			return fmt.Errorf("(RedisCli:loadKeysByPrefix) terminated")
		}

		// Scan for RedisMaxScanKeys items (max)
		sKeys, cursor, err = rc.c.Scan(rc.Ctx, cursor, prefix, RedisMaxScanKeys).Result()
		if err != nil {
			return fmt.Errorf("(RedisCli:loadKeysByPrefix) SCAN for prefix %q (cursor: %d) failed: %w", prefix, cursor, err)
		}

		// Append scanned keys to the resulted list as set of paths without prefix
		for _, k := range sKeys {
			// Append prefix field
			if err := appendFunc(k); err != nil {
				log.E("(RedisCli:loadKeysByPrefix) cannot append key %q - %v", k, err)
			}
		}

		// Is the end of keys space reached
		if cursor == 0 {
			// Return resulted data
			log.D("(RedisCli:loadKeysByPrefix) Scan for keys prefixed with %q finished, scans number %d", prefix, i)
			// OK
			return nil
		}
	}
}

func prepareHSetValues(host string, fso *types.FSObject) []string {
	// Output slice with values prepared to send to Redis
	values := make([]string, 0, types.FSObjectFieldsNum + 1 /* id field */ + 1 /* host field */)

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
