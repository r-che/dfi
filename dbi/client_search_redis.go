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

func (rc *RedisClient) Query(searchPhrases []string, qa *QueryArgs) ([]QueryResult, error) {
	// TODO
	// Common order:
	// * Add to query all additional arguments from query args
	// * For each search phrase:
	//   * Use SCAN MATCH
	//   * Try to use it as part of object name (using RediSearch)
	// * Build final query using or/not modifiers


	// Get RediSearch client
	rsc, err := rc.rschInit()
	if err != nil {
		// Log error but do not abort execution
		log.E("(RedisCli) Cannot initialize RediSearch client: %v", err)
	} else {
		qr := rshSearch(rsc, qa, searchPhrases)
		for _, obj := range qr {
			//fmt.Println(obj.ID)	// TODO
			_=obj
		}
	}


	return nil, nil
}

func rshSearch(cli *rsh.Client, qa *QueryArgs, searchPhrases []string) []*QueryResult {
	// Offset from which matched documents should be selected
	offset := 0

	// Make redisearch initial query
	q := rsh.NewQuery(rshQuery(searchPhrases, qa)).
		SetReturnFields(FieldID)

	log.D("(RedisCli) Prepared RediSearch query string: %v", q.Raw)

	// Output result
	qr := make([]*QueryResult, 0, 10)

	// Total selected docs
	totDocs := 0

	// TODO Need hosts filtering
	if qa.isHost() {
		// TODO Results should be filtered by qa.hosts if provided
	}

	for {
		// Update query to set offset/limit
		q.Limit(offset, objsPerQuery)

		// Do search
		docs, total, err := cli.Search(q)
		if err != nil {
			log.E("(RedisCli) RediSearch query failed: %v", err)
			continue
		}

		log.D("Scanned offset: %d .. %d, selected %d (total matched %d)", offset, offset + objsPerQuery, len(docs), total)

		// Convert scanned documents to output result
		for _, doc := range docs {
			if id, ok := doc.Properties[FieldID].(string); ok {
				qr = append(qr, &QueryResult{FullID: doc.Id, ID: id})
			} else {
				log.W("(RedisCli) RediSearch found invalid record %q without %q field", doc.Id, FieldID)
				qr = append(qr, &QueryResult{FullID: doc.Id})
			}
		}

		// Check for number of total matched documents reached total - no mo docs to scan
		if totDocs += len(docs); totDocs >= total {
			// Return results
			log.D("(RedisCli) RediSearch returned %d records", len(qr))
			return qr
		}

		// Update offset
		offset += objsPerQuery
	}
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

func rshQuery(searchPhrases []string, qa *QueryArgs) string {
	// Query chunks
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
	if qa.isID() {
		chunks = append(chunks, prepID(qa))
	}

	// Make search phrases query - try to search them in found path and real path
	var spQuery string
	if len(searchPhrases) != 0 {
		spExpr := strings.Join(searchPhrases, ` | `)
		spQuery = fmt.Sprintf(`(@%s:%s | @%s:%s)`, FieldFPath, spExpr, FieldRPath, spExpr)
	}

	// Check that chunks is not empty
	if len(chunks) == 0 {
		// Using only search phrases
		return spQuery
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

	if spQuery != "" {
		// Make a summary query with search phrases
		return spQuery + ` ` + argsQuery
	}

	// Return only arguments part
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

func prepID(qa *QueryArgs) string {
	return `@` + FieldID+ `:{` +  strings.Join(qa.ids, `|`) + `}`
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
