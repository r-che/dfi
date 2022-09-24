//go:build dbi_redis
package dbi

import (
	"fmt"
	"strings"

	"github.com/r-che/log"
	"github.com/gomodule/redigo/redis"
    rsh "github.com/RediSearch/redisearch-go/redisearch"
)

const metaRschIdx = "obj-meta-idx"
const objsPerQuery = 1000

func (rc *RedisClient) Query(searchPhrases []string, qa *QueryArgs, retFields []string) ([]QueryResult, error) {
	// TODO
	// Common order:
	// * Add to query all additional arguments from query args
	// * For each search phrase:
	//   * Use SCAN MATCH
	//   * Try to use it as part of object name (using RediSearch)
	// * Build final query using or/not modifiers


	// Do standard SCAN search

	// TODO
	if _, err := rc.scanSearch(searchPhrases, qa, retFields); err != nil {
		log.E("(RedisCli) SCAN search failed: %v", err)
	}

	// Do RediSearch

	// Get RediSearch client
	rsc, err := rc.rschInit()
	if err != nil {
		// Log error but do not abort execution
		log.E("(RedisCli) Cannot initialize RediSearch client: %v", err)
	} else {
		qr := rshSearch(rsc, qa, searchPhrases, retFields)
		for _, obj := range qr {
			fmt.Println(obj)	// TODO
			_=obj
		}
	}

	return nil, nil
}

func (rc *RedisClient) scanSearch(searchPhrases []string, qa *QueryArgs, retFields []string) ([]QueryResult, error) {
	// Check for empty search phrases
	if len(searchPhrases) == 0 {
		// Nothing to search using SCAN
		return nil, nil
	}

	// 1. Prepare matches list for all search phrases
	matches := make([]string, 0, len(searchPhrases) * len(qa.hosts))
	for _, sp := range searchPhrases {
		sp = prepareScanPhrase(sp)

		if len(qa.hosts) == 0 {
			// All hosts match
			matches = append(matches, RedisObjPrefix + sp)
		} else {
			// Prepare search phrases for each host separately
			for _, host := range qa.hosts {
				matches = append(matches, RedisObjPrefix + host + ":" + sp)
			}
		}
	}

	// 2. Search for all matching keys
	matched := []string{}
	for _, match := range matches {
		log.D("(RedisCli) Do SCAN search for: %s", match)
		if err := rc.scanKeyMatch(match, &matched); err != nil {
			return nil, err
		}
	}
	log.D("(RedisCli) Total %d matched by SCAN operation", len(matched))

	// Check for nothing to do
	if len(matched) == 0 {
		return nil, nil
	}

	// 3. Get ID for each matched key
	ids := make([]string, 0, len(matched))
	for _, k := range matched {
		id, err := rc.c.HGet(rc.ctx, k, FieldID).Result()
		if err != nil {
			return nil, fmt.Errorf("cannot get ID for key %q: %v", k, err)
		}
		// Append extracted ID
		ids = append(ids, id)
	}
	log.D("(RedisCli) Identifiers of found objects extracted")

	// 4. TODO Run RediSearch with extracted ID and provided query arguments

	return nil, nil
}

func (rc *RedisClient) scanKeyMatch(match string, keys *[]string) error {
	// Scan() intermediate  variables
	var cursor uint64
	var sKeys []string
	var err error

	// Scan keys space prefixed by pref
	for i := 0; ; i++ {
		// Scan for RedisMaxScanKeys items (max)
		sKeys, cursor, err = rc.c.Scan(rc.ctx, cursor, match, RedisMaxScanKeys).Result()
		if err != nil {
			return err
		}

		// Append scanned keys to the resulted list as set of paths without prefix
		for _, k := range sKeys {
			*keys = append(*keys, k)
		}

		// Is the end of keys space reached
		if cursor == 0 {
			// Scan finished
			return nil
		}
	}
}

func prepareScanPhrase(sp string) string {
	if !strings.HasPrefix(sp, "*") {
		sp = "*" + sp
	}
	if !strings.HasSuffix(sp, "*") {
		sp += "*"
	}

	return sp
}

func rshSearch(cli *rsh.Client, qa *QueryArgs, searchPhrases []string, retFields []string) []*QueryResult {
	// Offset from which matched documents should be selected
	offset := 0

	// Make redisearch initial query
	q := rsh.NewQuery(rshQuerySP(searchPhrases, qa))

	if len(retFields) != 0 {
		// Set list of requested fields
		q.SetReturnFields(retFields...)
	} else {
		q.SetFlags(rsh.QueryNoContent)
	}

	log.D("(RedisCli) Prepared RediSearch query string: %v", q.Raw)

	// Output result
	qr := make([]*QueryResult, 0, 10)

	// Total selected docs
	totDocs := 0

	for {
		// Update query to set offset/limit
		q.Limit(offset, objsPerQuery)

		// Do search
		docs, total, err := cli.Search(q)
		if err != nil {
			log.E("(RedisCli) RediSearch query failed: %v", err)
			break
		}

		log.D("Scanned offset: %d .. %d, selected %d (total matched %d)", offset, offset + objsPerQuery, len(docs), total)

		// Convert scanned documents to output result
		for _, doc := range docs {
			qr = append(qr, &QueryResult{FullID: doc.Id, Fields: doc.Properties})
		}

		// Check for number of total matched documents reached total - no mo docs to scan
		if totDocs += len(docs); totDocs >= total {
			// Return results
			log.D("(RedisCli) RediSearch returned %d records", len(qr))
			break
		}

		// Update offset
		offset += objsPerQuery
	}

	return qr
}

func (rc *RedisClient) rschInit() (*rsh.Client, error) {
	// Client pointer
	var c *rsh.Client

	// Read username/password from private data if set
	user, passw, err := userPasswd(rc.cfg.PrivCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load username/password from private configuration: %v", err)
	}

	// Is password set?
	if passw != "" {
		// Need to create pool to provide authentication ability
		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", rc.cfg.HostPort,
					redis.DialUsername(user),
					redis.DialPassword(passw),
				)
			},
		}

		// Create client from pool
		c = rsh.NewClientFromPool(pool, metaRschIdx)
	} else {
		// Create a simple client
		c = rsh.NewClient(rc.cfg.HostPort, metaRschIdx)
	}

	// OK
	return c, err
}

func rshQuerySP(searchPhrases []string, qa *QueryArgs) string {
	if len(searchPhrases) == 0 {
		// Return only arguments part
		return rshArgsQuery(qa)
	}

	// Make search phrases query - try to search them in found path and real path
	var spQuery string
	if len(searchPhrases) != 0 {
		spQuery = fmt.Sprintf(`(@%[1]s:%[3]s | @%[2]s:%[3]s)`,
			FieldFPath, FieldRPath,
			strings.Join(searchPhrases, ` | `),
		)
	}

	// Make a summary query with search phrases
	return spQuery + ` ` + rshArgsQuery(qa)
}

func rshArgsQuery(qa *QueryArgs) string {
	// Arguments query chunks
	chunks := []string{}

	if qa.isMtime() {
		chunks = append(chunks, prepMtime(qa))
	}
	if qa.isSize() {
		chunks = append(chunks, prepSize(qa))
	}
	if qa.isType() {
		chunks = append(chunks, prepType(qa))
	}
	if qa.isChecksum() {
		chunks = append(chunks, prepChecksum(qa))
	}
	if qa.isHost() {
		chunks = append(chunks, prepHost(qa))
	}

	// Check that chunks is not empty
	if len(chunks) == 0 {
		return ""
	}

	// Need to build request from chunks
	var argsQuery string
	negMark := ""
	if qa.negExpr {
		negMark = "-"
	}

	if qa.orExpr {
		argsQuery = fmt.Sprintf(`%s(%s)`, negMark, strings.Join(chunks, ` | `))
	} else {
		argsQuery = fmt.Sprintf(`%s(%s)`, negMark, strings.Join(chunks, ` `))
	}

	// Done
	return argsQuery
}

func prepMtime(qa *QueryArgs) string {
	return makeSetRangeQuery(FieldMTime, qa.mtimeStart, qa.mtimeEnd, qa.mtimeSet)
}

func prepSize(qa *QueryArgs) string {
	return makeSetRangeQuery(FieldSize, qa.sizeStart, qa.sizeEnd, qa.sizeSet)
}

func prepType(qa *QueryArgs) string {
	return `@` + FieldType + `:{` +  strings.Join(qa.types, `|`) + `}`
}

func prepChecksum(qa *QueryArgs) string {
	return `@` + FieldChecksum + `:{` +  strings.Join(qa.csums, `|`) + `}`
}

func prepHost(qa *QueryArgs) string {
	escapedHosts := make([]string, 0, len(qa.hosts))

	for _, host := range qa.hosts {
		escapedHosts = append(escapedHosts, rsh.EscapeTextFileString(host))
	}

	// FT.SEARCH obj-meta-idx @host:'{deb\-devtest}' RETURN 1 host
	return `@` + FieldHost + `:{` + strings.Join(escapedHosts, `|`) + `}`
}

func makeSetRangeQuery(field string, min, max int64, set []int64) string {
	// Is set is not provided
	if len(set) == 0 {
		// Then min/max query

		// If closed interval
		if min != 0 && max != 0 {
			return fmt.Sprintf(`@%s:[%d %d]`, field, min, max)
		}

		// Half-open interval
		if min == 0 {
			return fmt.Sprintf(`@%s:[-inf %d]`, field, max)
		}

		return fmt.Sprintf(`@%s:[%d +inf]`, field, min)
	}

	// Build query from set of values
	chunks := make([]string, 0, len(set))

	for _, item := range set {
		chunks = append(chunks, fmt.Sprintf(`@%s:[%d %d]`, field, item, item))
	}

	return `(` + strings.Join(chunks, ` | `) + `)`
}
