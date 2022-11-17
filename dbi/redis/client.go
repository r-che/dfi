//go:build dbi_redis
package redis

import (
	"fmt"
	"strings"

	"github.com/r-che/dfi/types/dbms"

	"github.com/r-che/log"

    rsh "github.com/RediSearch/redisearch-go/redisearch"
)

func (rc *RedisClient) Query(qa *dbms.QueryArgs, retFields []string) (dbms.QueryResults, error) {
	// Get RediSearch client
	rsc, err := rc.rschInit(metaRschIdx)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:Query) cannot initialize RediSearch client: %w", err)
	}

	// Create simple (non-deep) query
	q := rsh.NewQuery(rshQuery(qa))

	// Do search
	qr, err := rshSearch(rsc, q, retFields)
	if err != nil {
		return qr, err
	}

	// Check for deep search required
	if qa.DeepSearch {
		// Do additional standard SCAN search
		log.D("(RedisCli:Query) Running deep search using SCAN operation...")
		n, err := rc.scanSearch(rsc, qa, retFields, qr)
		if err != nil {
			return qr, fmt.Errorf("(RedisCli:Query) SCAN search failed: %w", err)
		}
		log.D("(RedisCli:Query) Total of %d records were found with a deep (SCAN) search", n)
	}

	return qr, nil
}

func (rc *RedisClient) QueryAIIIds(qa *dbms.QueryArgs) ([]string, error) {
	// Get RediSearch client to search by additional information items
	rsc, err := rc.rschInit(aiiRschIdx)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:QueryAIIIds) cannot initialize RediSearch client: %w", err)
	}

	var chunks []string

	// Check for need to use tags
	if qa.UseTags {
		et := make([]string, 0, len(qa.SP))	// Escaped tags
		for _, tag := range qa.SP {
			et = append(et, rsh.EscapeTextFileString(tag))
		}
		chunks = append(chunks, `(@` + dbms.AIIFieldTags + `:{` +  strings.Join(et, `|`) + `})`)
	}

	// Check for need to use description
	if qa.UseDescr {
		chunks = append(chunks, `(@` + dbms.AIIFieldDescr + `:` +  strings.Join(qa.SP, `|`) + `)` )
	}

	// Make query to search by AII fields
	q := rsh.NewQuery(strings.Join(chunks, ` | `))

	ids, err := rshSearchAII(rsc, q)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:QueryAIIIds) cannot execute query %q: %w", q.Raw, err)
	}

	log.D("(RedisCli:QueryAIIIds) AII search (tags: %t descr: %t) found identifiers: %v", qa.UseTags, qa.UseDescr, ids)

	return ids, nil
}

func (rc *RedisClient) GetObjects(ids, retFields []string) (dbms.QueryResults, error) {
	// Get RediSearch client
	rsc, err := rc.rschInit(metaRschIdx)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:GetObjects) cannot initialize RediSearch client: %w", err)
	}

	// Make initial query
	q := rsh.NewQuery(rshQueryByIds(ids, &dbms.QueryArgs{}))

	// Do search and return
	return rshSearch(rsc, q, retFields)
}
