//go:build dbi_redis
package redis

import (
	"fmt"
	"strings"
	"strconv"
	"errors"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"
	"github.com/gomodule/redigo/redis"
    rsh "github.com/RediSearch/redisearch-go/redisearch"
)

const (
	metaRschIdx		=	"obj-meta-idx"
	aiiRschIdx		=	"aii-idx"
	objsPerQuery	=	1000

	// Estimated maximum number of search results, empiric value
	estResultsCount	=	32
)

func rshSearchAII(cli *rsh.Client, q *rsh.Query) ([]string, error) {
	// Offset from which matched documents should be selected
	offset := 0

	// Content is not needed - only keys should be returned
	q.SetFlags(rsh.QueryNoContent)

	// log.D("(RedisCli:rshSearchAII) Prepared RediSearch query string: %v", q.Raw)	// XXX Raw query may be too long

	// Output result
	ids := make([]string, 0, estResultsCount)

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
			return ids, fmt.Errorf("(RedisCli:rshSearchAII) RediSearch returned %d records and failed: %w", len(ids), err)
		}

		log.D("(RedisCli) Scanned offset: %d .. %d, selected %d (total matched %d)", offset, offset + objsPerQuery, len(docs), total)

		// Convert scanned documents to output result
		for _, doc := range docs {
			if len(doc.Id) <= kpl {
				log.E("RedisCli:rshSearchAII) Found invalid AII with too short key %q, skip it", doc.Id)
				continue
			}
			ids = append(ids, doc.Id[kpl:])
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

func (rc *RedisClient) rschInit(rschIdx string) (*rsh.Client, error) {
	// Read username/password from private data if set
	user, passw, err := userPasswd(rc.Cfg.PrivCfg)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:rschInit) failed to load username/password from private configuration: %w", err)
	}

	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(rc.Cfg.ID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:rschInit) cannot convert database identifier value to unsigned integer: %w", err)
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
				return redis.Dial("tcp", rc.Cfg.HostPort, redis.DialDatabase(int(dbid)))
			}
			// Dial with authentication
			return redis.Dial("tcp", rc.Cfg.HostPort,
				redis.DialDatabase(int(dbid)),
				redis.DialUsername(user),
				redis.DialPassword(passw),
			)
		},
	}

	// OK, return client from pool
	return rsh.NewClientFromPool(pool, rschIdx), nil
}

func rshSearch(cli *rsh.Client, q *rsh.Query, retFields []string) (dbms.QueryResults, error) {
	// Offset from which matched documents should be selected
	offset := 0

	if len(retFields) != 0 {
		// Set list of requested fields
		q.SetReturnFields(retFields...)
	} else {
		q.SetFlags(rsh.QueryNoContent)
	}

	// log.D("(RedisCli:rshSearch) Prepared RediSearch query string: %v", q.Raw)	// XXX Raw query may be too long

	// Output result
	qr := make(dbms.QueryResults, dbms.ExpectedMaxResults)

	// Total selected docs
	totDocs := 0

	// Key prefix length
	kpl := len(RedisObjPrefix)

	for {
		// Update query to set offset/limit
		q.Limit(offset, objsPerQuery)

		// Do search
		docs, total, err := cli.Search(q)
		if err != nil {
			return qr, fmt.Errorf("(RedisCli:rshSearch) RediSearch returned %d records and failed: %w", len(qr), err)
		}

		log.D("(RedisCli) Scanned offset: %d .. %d, selected %d (total matched %d)", offset, offset + objsPerQuery, len(docs), total)

		// Convert scanned documents to output result
		for _, doc := range docs {
			if len(doc.Id) <= kpl {
				log.E("RedisCli:rshSearch) Found invalid AII with too short key %q, skip it", doc.Id)
				continue
			}
			// Split identifier without object prefix from host:path format to separate values
			host, path, ok := strings.Cut(doc.Id[kpl:], ":")
			if !ok {
				log.E("(RedisCli:rshSearch) Skip document with invalid key format: %q", doc.Id)
				continue
			}
			// Append key without object prefix
			qr[types.ObjKey{Host: host, Path: path}] = doc.Properties
		}

		// Check for number of total matched documents reached total - no more docs to scan
		if totDocs += len(docs); totDocs >= total {
			break
		}

		// Update offset
		offset += objsPerQuery
	}

	// Return results
	log.D("(RedisCli:rshSearch) RediSearch returned %d records", len(qr))

	// OK
	return qr, nil
}

func rshQueryByIds(ids []string, qa *dbms.QueryArgs) string {
	// Make query to search by IDs
	idsQuery := fmt.Sprintf(`(@%s:{%s})`, dbms.FieldID, strings.Join(ids, `|`))

	// Make a summary query with search phrases
	return idsQuery + ` ` + rshArgs(qa)
}

func rshQuery(qa *dbms.QueryArgs) string {
	if len(qa.SP) == 0 && !qa.IsIds() {
		// Return only arguments part
		return rshArgs(qa)
	}

	chunks := make([]string, 0, 1)	// At least we need 1 chunk for search

	if len(qa.SP) != 0 {
		// XXX Convert of search phrase values to lowercase because RediSearch
		// XXX does not fully support case insensitivity for non-English locales
		spLower := make([]string, 0, len(qa.SP))
		for _, sp := range qa.SP {
			spLower= append(spLower, strings.ToLower(sp))
		}

		// Make search phrases query - try to search them in found path and real path
		if qa.OnlyName {
			// Use only the "name" field to search
			chunks = append(chunks, fmt.Sprintf(`(@%s:%s)`, dbms.FieldName, strings.Join(spLower, `|`)))
		} else {
			// Use the found path and real path fields to search
			chunks = append(chunks,
				fmt.Sprintf(`(@%[1]s|%[2]s:%[3]s)`, dbms.FieldFPath, dbms.FieldRPath, strings.Join(spLower, `|`)))
		}
	}

	if qa.IsIds() {
		chunks = append(chunks, `(@` + dbms.FieldID + `:{` +  strings.Join(qa.Ids, `|`) + `})`)
	}

	if len(chunks) == 0 {
		// No search phrases/AII data was provided, use only query arguments
		return rshArgs(qa)
	}

	// Make a summary query with search phrases/AII data + query arguments
	return `(` + strings.Join(chunks, ` | `) + `)` + ` ` + rshArgs(qa)
}

func rshArgs(qa *dbms.QueryArgs) string {
	// Arguments query chunks
	chunks := []string{}

	if qa.IsMtime() {
		chunks = append(chunks, makeSetRangeQuery(dbms.FieldMTime, qa.MtimeStart, qa.MtimeEnd, qa.MtimeSet))
	}
	if qa.IsSize() {
		chunks = append(chunks, makeSetRangeQuery(dbms.FieldSize, qa.SizeStart, qa.SizeEnd, qa.SizeSet))
	}
	if qa.IsType() {
		chunks = append(chunks, `(@` + dbms.FieldType + `:{` +  strings.Join(qa.Types, `|`) + `})`)
	}
	if qa.IsChecksum() {
		chunks = append(chunks, `(@` + dbms.FieldChecksum + `:{` +  strings.Join(qa.CSums, `|`) + `})`)
	}
	if qa.IsHost() {
		// At least need to escape dashes ("-") inside of hostname to avoid split hostnames by RediSearch tokenizer
		escapedHosts := make([]string, 0, len(qa.Hosts))
		for _, host := range qa.Hosts {
			escapedHosts = append(escapedHosts, rsh.EscapeTextFileString(host))
		}

		chunks = append(chunks, `(@` + dbms.FieldHost + `:{` + strings.Join(escapedHosts, `|`) + `})`)
	}

	// Check that chunks is not empty
	if len(chunks) == 0 {
		return ""
	}

	// Need to build request from chunks
	var argsQuery string
	negMark := ""
	if qa.NegExpr {
		negMark = "-"
	}

	if qa.OrExpr {
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

//
// Additional "deep" search mechanism
//

func (rc *RedisClient) loadIDsByPatterns(qa *dbms.QueryArgs, qrTop dbms.QueryResults) ([]string, error) {
	// Search for all matching keys
	var matched []string

	for _, pattern := range scanSearchPatterns(qa) {
		log.D("(RedisCli:loadIDsByPatterns) Do SCAN search for: %s", pattern)

		// Perform scan with filter
		m, err := rc.scanKeyMatch(pattern,
			// Match condition to filter keys matched by patterns
			func(val string) bool {
				// Split identifier without object prefix from host:path format to separate values
				host, path, ok := strings.Cut(val[len(RedisObjPrefix):], ":")
				if !ok {
					return false
				}

				// Check for key does not exist in the query results
				if _, ok := qrTop[types.ObjKey{Host: host, Path: path}]; !ok {
					// Then - need to append it
					return true
				}
				// Skip this key otherwise
				return false
			})

		if err != nil {
			return nil, fmt.Errorf("(RedisCli:loadIDsByPatterns) scan with pattern %q failed: %w", pattern, err)
		}

		// Append summary result
		matched = append(matched, m...)
	}

	log.D("(RedisCli:loadIDsByPatterns) %d keys matched by SCAN operation", len(matched))

	return matched, nil
}

func (rc *RedisClient) scanSearch(rsc *rsh.Client, qa *dbms.QueryArgs, retFields []string, qrTop dbms.QueryResults) (int, error) {
	// Check for empty search phrases
	if len(qa.SP) == 0 {
		// Nothing to search using SCAN
		return 0, nil
	}

	// 1. Search for all matching keys
	matched, err := rc.loadIDsByPatterns(qa, qrTop)
	if err != nil {
		return 0, fmt.Errorf("(RedisCli:scanSearch) fail to load identifiers of matched keys: %w", err)
	}

	// Check that no matching keys were found
	if len(matched) == 0 {
		return 0, nil
	}

	// 2. Get ID for each matched key
	log.D("(RedisCli:scanSearch) Loading identifiers for all matched keys...")
	ids := make([]string, 0, len(matched))
	for _, k := range matched {
		// Load the identifier field from the hash associated with key k
		id, err := rc.c.HGet(rc.Ctx, k, dbms.FieldID).Result()
		if err != nil {
			if errors.Is(err, RedisNotFound) {
				return 0, fmt.Errorf("identificator field %q does not exist for key %q", dbms.FieldID, k)
			}

			return 0, fmt.Errorf("cannot get ID for key %q: %w", k, err)
		}

		// Append extracted ID
		ids = append(ids, id)
	}
	log.D("(RedisCli:scanSearch) Identifiers of found objects extracted")

	// 3. Run RediSearch with extracted IDs and provided query arguments

	// Run search to get results by IDs
	qr, err := rshSearch(rsc, rsh.NewQuery(rshQueryByIds(ids, qa)), retFields)

	// 4. Merge selected results with the previous results
	for k, v := range qr {
		qrTop[k] = v
	}

	// Return length of found results and error if occurred
	return len(qr), err
}

func scanSearchPatterns(qa *dbms.QueryArgs) []string {
	// Prepare patterns list for all search phrases
	patterns := make([]string, 0, len(qa.SP) * len(qa.Hosts))

	for _, sp := range qa.SP {
		// Prepend by asterisk
		if !strings.HasPrefix(sp, "*") {
			sp = "*" + sp
		}
		// Append asterisk at the end
		if !strings.HasSuffix(sp, "*") {
			sp += "*"
		}

		// Is hosts list is empty?
		if len(qa.Hosts) == 0 {
			// All hosts match
			patterns = append(patterns, RedisObjPrefix + sp)
		} else {
			// Prepare search phrases for each host separately
			for _, host := range qa.Hosts {
				patterns = append(patterns, RedisObjPrefix + host + ":" + sp)
			}
		}
	}

	return patterns
}

func (rc *RedisClient) scanKeyMatch(match string, filter dbms.MatchStrFunc) ([]string, error) {
	// Output slice
	out := []string{}

	// Scan() intermediate  variables
	var cursor uint64
	var sKeys []string
	var err error

	// Scan keys space prefixed with pref
	for i := 0; ; i++ {
		// Scan for RedisMaxScanKeys items (max)
		sKeys, cursor, err = rc.c.Scan(rc.Ctx, cursor, match, RedisMaxScanKeys).Result()
		if err != nil {
			return nil, err
		}

		// Append scanned keys to the resulted list as a set of paths
		for _, k := range sKeys {
			// Append only filtered values
			if filter(k) {
				out = append(out, k)
			} else {
				log.D("(RedisCli:scanKeyMatch) SCAN search skips already found key %q", k)
			}
		}

		// Is the end of keys space reached
		if cursor == 0 {
			// Scan finished
			return out, nil
		}
	}
}
