//go:build dbi_redis
package dbi

import (
	"fmt"
	"strings"
	"strconv"

	"github.com/r-che/log"
	"github.com/gomodule/redigo/redis"
    rsh "github.com/RediSearch/redisearch-go/redisearch"
)

const (
	metaRschIdx		=	"obj-meta-idx"
	aiiRschIdx		=	"aii-idx"
	objsPerQuery	=	1000
)

func (rc *RedisClient) Query(qa *QueryArgs, retFields []string) (QueryResults, error) {
	// Get RediSearch client
	rsc, err := rc.rschInit(metaRschIdx)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli) cannot initialize RediSearch client: %v", err)
	}

	// Check for search by AII enabled
	if qa.UseAII() {
		qa.ids, err = rc.queryAII(qa)
		if err != nil {
			return nil, err
		}
	}

	// Make initial query
	q := rsh.NewQuery(rshQuerySP(qa))

	// Do search
	qr, err := rshSearch(rsc, q, retFields)
	if err != nil {
		return qr, err
	}

	// Check for deep search required
	if qa.deep {
		// Do additional standard SCAN search
		log.D("(RedisCli) Running deep search using SCAN operation...")
		n, err := rc.scanSearch(rsc, qa, retFields, &qr)
		if err != nil {
			return qr, fmt.Errorf("(RedisCli) SCAN search failed: %v", err)
		}
		log.D("(RedisCli) Total of %d records were found with a deep (SCAN) search", n)
	}

	return qr, nil
}

func (rc *RedisClient) queryAII(qa *QueryArgs) ([]string, error) {
	// Get RediSearch client to search by additional information items
	rsc, err := rc.rschInit(aiiRschIdx)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:queryAII) cannot initialize RediSearch client: %v", err)
	}

	var chunks []string

	// Check for need to use tags
	if qa.useTags {
		chunks = append(chunks, `@` + AIIFieldTags + `:{` +  strings.Join(qa.sp, `|`) + `}`)
	}

	// TODO Check for need to use description

	// Make query to search by AII fields
	q := rsh.NewQuery(strings.Join(chunks, ` | `))

	ids, err := rshSearchAII(rsc, q)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:queryAII) cannot execute query: %v", err)
	}

	log.D("(RedisCli:queryAII) AII search (tags: %t descr: %t) found identifiers: %v", qa.useTags, false /*TODO*/, ids)

	return ids, nil
}

func rshSearchAII(cli *rsh.Client, q *rsh.Query) ([]string, error) {
	// Offset from which matched documents should be selected
	offset := 0

	// Content is not needed - only keys should be returned
	q.SetFlags(rsh.QueryNoContent)

	// Output result
	ids := make([]string, 0, 32)	// 32 - should probably be enough for most cases on average

	// Total selected docs
	totDocs := 0

	// Key prefix length
	kpl := len(RedisAIIPrefix)

	for {
		// Update query to set offset/limit
		q.Limit(offset, objsPerQuery)

		// Do search
		docs, total, err := cli.Search(q)
		if err != nil {
			return ids, fmt.Errorf("(RedisCli:rshSearchAII) RediSearch returned %d records and failed: %v", len(ids), err)
		}

		log.D("(RedisCli) Scanned offset: %d .. %d, selected %d (total matched %d)", offset, offset + objsPerQuery, len(docs), total)

		// Convert scanned documents to output result
		for _, doc := range docs {
			if len(doc.Id) <= kpl {
				log.E("RedisCli:rshSearchAII) Found invalid AII with too short key %q, skip it", doc.Id)
				continue
			}
			ids = append(ids, doc.Id[len(RedisAIIPrefix):])
		}

		// Check for number of total matched documents reached total - no more docs to scan
		if totDocs += len(docs); totDocs >= total {
			break
		}

		// Update offset
		offset += objsPerQuery
	}

	// Return results
	log.D("(RedisCli:rshSearchAII) RediSearch returned %d records", len(ids))

	// OK
	return ids, nil
}

func (rc *RedisClient) GetObjects(ids, retFields []string) (QueryResults, error) {
	// Get RediSearch client
	rsc, err := rc.rschInit(metaRschIdx)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:GetObjects) cannot initialize RediSearch client: %v", err)
	}

	// Make initial query
	q := rsh.NewQuery(rshQueryIDs(ids, &QueryArgs{}))

	// Do search and return
	return rshSearch(rsc, q, retFields)
}

func (rc *RedisClient) rschInit(rschIdx string) (*rsh.Client, error) {
	// Read username/password from private data if set
	user, passw, err := userPasswd(rc.cfg.PrivCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load username/password from private configuration: %v", err)
	}

	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(rc.cfg.ID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:rschInit) cannot convert database identifier value to unsigned integer: %v", err)
	}

	// Check for DB ID is not 0
	if dbid != 0 {
		// It may cause problems
		log.W("(RedisCli:rschInit) WARNING! Redis DB ID is set to %d - it may cause incorrect results " +
			"due to RediSearch does not work on DBs with non-zero ID, see: %s",
			dbid, `https://github.com/RediSearch/RediSearch/issues/367`)
	}

	// Create pool to have ability to provide authentication and database identifier
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			if passw == "" {
				// Simple dial
				return redis.Dial("tcp", rc.cfg.HostPort, redis.DialDatabase(int(dbid)))
			}
			// Dial with authentication
			return redis.Dial("tcp", rc.cfg.HostPort,
				redis.DialDatabase(int(dbid)),
				redis.DialUsername(user),
				redis.DialPassword(passw),
			)
		},
	}

	// OK, return client from pool
	return rsh.NewClientFromPool(pool, rschIdx), nil
}

func rshSearch(cli *rsh.Client, q *rsh.Query, retFields []string) (QueryResults, error) {
	// Offset from which matched documents should be selected
	offset := 0

	if len(retFields) != 0 {
		// Set list of requested fields
		q.SetReturnFields(retFields...)
	} else {
		q.SetFlags(rsh.QueryNoContent)
	}

	// log.D("(RedisCli) Prepared RediSearch query string: %v", q.Raw)	// XXX Raw query may be too long

	// Output result
	qr := make(QueryResults, 32)	// 32 - should probably be enough for most cases on average

	// Total selected docs
	totDocs := 0

	for {
		// Update query to set offset/limit
		q.Limit(offset, objsPerQuery)

		// Do search
		docs, total, err := cli.Search(q)
		if err != nil {
			return qr, fmt.Errorf("(RedisCli) RediSearch returned %d records and failed: %v", len(qr), err)
		}

		log.D("(RedisCli) Scanned offset: %d .. %d, selected %d (total matched %d)", offset, offset + objsPerQuery, len(docs), total)

		// Convert scanned documents to output result
		for _, doc := range docs {
			// Split identifier without object prefix from host:path format to separate values
			host, path, ok := strings.Cut(doc.Id[len(RedisObjPrefix):], ":")
			if !ok {
				log.E("(RedisCli) Skip document with invalid key format: %q", doc.Id)
				continue
			}
			// Append key without object prefix
			qr[QRKey{Host: host, Path: path}] = doc.Properties
		}

		// Check for number of total matched documents reached total - no more docs to scan
		if totDocs += len(docs); totDocs >= total {
			break
		}

		// Update offset
		offset += objsPerQuery
	}

	// Return results
	log.D("(RedisCli) RediSearch returned %d records", len(qr))

	// OK
	return qr, nil
}

func rshQueryIDs(ids []string, qa *QueryArgs) string {
	// Make query to search by IDs
	idsQuery := fmt.Sprintf(`(@%s:{%s})`,
		FieldID,
		strings.Join(ids, `|`),
	)

	// Make a summary query with search phrases
	return idsQuery + ` ` + rshArgsQuery(qa)
}

func rshQuerySP(qa *QueryArgs) string {
	if len(qa.sp) == 0 && !qa.isIds() {
		// Return only arguments part
		return rshArgsQuery(qa)
	}

	chunks := make([]string, 0, 2)

	if len(qa.sp) != 0 && !qa.onlyTags {
		// XXX Convert of search phrase values to lowercase because RediSearch
		// XXX does not fully support case insensitivity for non-English locales
		spLower := make([]string, 0, len(qa.sp))
		for _, sp := range qa.sp {
			spLower= append(spLower, strings.ToLower(sp))
		}

		// Make search phrases query - try to search them in found path and real path
		if qa.onlyName {
			// Use only the "name" field to search
			chunks = append(chunks, fmt.Sprintf(`@%s:%s`, FieldName, strings.Join(spLower, `|`)))
		} else {
			// Use the found path and real path fields to search
			chunks = append(chunks,
				fmt.Sprintf(`@%[1]s:%[3]s | @%[2]s:%[3]s`, FieldFPath, FieldRPath, strings.Join(spLower, `|`)))
		}
	}

	if qa.isIds() {
		chunks = append(chunks, `@` + FieldID + `:{` +  strings.Join(qa.ids, `|`) + `}`)
	}

	// Make a summary query with search phrases
	return `(` + strings.Join(chunks, ` | `) + `)` + ` ` + rshArgsQuery(qa)
}

func rshArgsQuery(qa *QueryArgs) string {
	// Arguments query chunks
	chunks := []string{}

	if qa.isMtime() {
		chunks = append(chunks, makeSetRangeQuery(FieldMTime, qa.mtimeStart, qa.mtimeEnd, qa.mtimeSet))
	}
	if qa.isSize() {
		chunks = append(chunks, makeSetRangeQuery(FieldSize, qa.sizeStart, qa.sizeEnd, qa.sizeSet))
	}
	if qa.isType() {
		chunks = append(chunks, `@` + FieldType + `:{` +  strings.Join(qa.types, `|`) + `}`)
	}
	if qa.isChecksum() {
		chunks = append(chunks, `@` + FieldChecksum + `:{` +  strings.Join(qa.csums, `|`) + `}`)
	}
	if qa.isHost() {
		// At least need to escape dashes ("-") inside of hostname to avoid split hostnames by RediSearch tokenizer
		escapedHosts := make([]string, 0, len(qa.hosts))
		for _, host := range qa.hosts {
			escapedHosts = append(escapedHosts, rsh.EscapeTextFileString(host))
		}

		chunks = append(chunks, `@` + FieldHost + `:{` + strings.Join(escapedHosts, `|`) + `}`)
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

	return `(` + strings.Join(chunks, `|`) + `)`
}

/*
 * Additional "deep" search mechanism
 */

func (rc *RedisClient) scanSearch(rsc *rsh.Client, qa *QueryArgs, retFields []string, qrTop *QueryResults) (int, error) {
	// Check for empty search phrases
	if len(qa.sp) == 0 {
		// Nothing to search using SCAN
		return 0, nil
	}

	// 1. Prepare matches list for all search phrases
	matches := make([]string, 0, len(qa.sp) * len(qa.hosts))
	for _, sp := range qa.sp {
		if !strings.HasPrefix(sp, "*") {
			sp = "*" + sp
		}
		if !strings.HasSuffix(sp, "*") {
			sp += "*"
		}

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
	var matched []string
	for _, match := range matches {
		log.D("(RedisCli) Do SCAN search for: %s", match)
		m, err := rc.scanKeyMatch(match, func(val string) bool {
			// Split identifier without object prefix from host:path format to separate values
			host, path, ok := strings.Cut(val[len(RedisObjPrefix):], ":")
			if !ok {
				return false
			}

			// Check for key does not exist in the query results
			if _, ok := (*qrTop)[QRKey{Host: host, Path: path}]; !ok {
				// Then - need to append it
				return true
			}
			// Skip this key otherwise
			return false
		})

		if err != nil {
			return 0, err
		}

		// Append summary result
		matched = append(matched, m...)
	}

	log.D("(RedisCli) %d keys matched by SCAN operation", len(matched))

	// Check for nothing to do
	if len(matched) == 0 {
		return 0, nil
	}

	// 3. Get ID for each matched key
	log.D("(RedisCli) Loading identifiers for all matched keys...")
	ids := make([]string, 0, len(matched))
	for _, k := range matched {
		id, err := rc.c.HGet(rc.ctx, k, FieldID).Result()
		if err != nil {
			if err == RedisNotFound {
				return 0, fmt.Errorf("identificator field %q does not exist for key %q", FieldID, k)
			}
			return 0, fmt.Errorf("cannot get ID for key %q: %v", k, err)
		}
		// Append extracted ID
		ids = append(ids, id)
	}
	log.D("(RedisCli) Identifiers of found objects extracted")

	// 4. Run RediSearch with extracted ID and provided query arguments

	// Make redisearch initial query
	q := rsh.NewQuery(rshQueryIDs(ids, qa))
	// Run search to get results by IDs
	qr, err := rshSearch(rsc, q, retFields)
	// Merge selected results with the previous results
	for k, v := range qr {
		(*qrTop)[k] = v
	}

	return len(qr), err
}

func (rc *RedisClient) scanKeyMatch(match string, filter FilterFunc) ([]string, error) {
	// Output slice
	out := []string{}

	// Scan() intermediate  variables
	var cursor uint64
	var sKeys []string
	var err error

	// Scan keys space prefixed by pref
	for i := 0; ; i++ {
		// Scan for RedisMaxScanKeys items (max)
		sKeys, cursor, err = rc.c.Scan(rc.ctx, cursor, match, RedisMaxScanKeys).Result()
		if err != nil {
			return nil, err
		}

		// Append scanned keys to the resulted list as a set of paths without prefix
		for _, k := range sKeys {
			// Append only filtered values
			if filter(k) {
				out = append(out, k)
			} else {
				log.D("(RedisCli) SCAN search skips already found key %q", k)
			}
		}

		// Is the end of keys space reached
		if cursor == 0 {
			// Scan finished
			return out, nil
		}
	}
}
