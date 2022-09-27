package dbi

import (
	"fmt"

	"github.com/r-che/log"
//	"github.com/r-che/dfi/types"

    rsh "github.com/RediSearch/redisearch-go/redisearch"
)


func (rc *RedisClient) ModifyAII(op DBOperator, add bool, ids []string, args *AIIArgs) error {
	// 0. Get RediSearch client
	rsc, err := rc.rschInit()
	if err != nil {
		return fmt.Errorf("(RedisCli) cannot initialize RediSearch client: %v", err)
	}

	// 1. Check for objects with identifiers ids really exist

	// Empty query arguments - no special search parameters are required
	qa := &QueryArgs{}
	// Create RediSearch query to get identifiers
	q := rsh.NewQuery(rshQueryIDs(ids, qa))
	// Run search to get results by IDs
	qr := rshSearch(rsc, q, []string{FieldID})

	// Create map indentifiers found in DB
	fids := make(map[string]any, len(ids))
	for k, v := range qr {
		id, ok := v[FieldID]
		if !ok {
			log.E("(RedisCli) Loaded invalid object from DB - no ID field (%q) was found: %s:%s", FieldID, k.Host, k.Path)
			continue
		}

		// Convert ID to string representation
		if sid, ok := id.(string); ok {
			fids[sid] = nil
		} else {
			log.E("(RedisCli) Loaded invalid object from DB - " +
				"ID field (%q) cannot be converted to string: %s:%s => %v", FieldID, k.Host, k.Path, id)
		}
	}

	// Check for all ids were found
	// TODO
	fmt.Println("fids:", fids)

	// 2. Check mode - need to add to existing value or set as is

	return nil
}
