package dbi

import (
	"fmt"
	"strings"
	"sort"

	"github.com/r-che/log"
//	"github.com/r-che/dfi/types"

    rsh "github.com/RediSearch/redisearch-go/redisearch"
)


func (rc *RedisClient) ModifyAII(op DBOperator, args *AIIArgs, ids []string, add bool) error {
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
	fids := make(map[string]QRKey, len(ids))
	for k, v := range qr {
		id, ok := v[FieldID]
		if !ok {
			log.E("(RedisCli) Loaded invalid object from DB - no ID field (%q) was found: %s:%s", FieldID, k.Host, k.Path)
			continue
		}

		// Convert ID to string representation
		if sid, ok := id.(string); ok {
			fids[sid] = k
		} else {
			log.E("(RedisCli) Loaded invalid object from DB - " +
				"ID field (%q) cannot be converted to string: %s:%s => %v", FieldID, k.Host, k.Path, id)
		}
	}

	// Check for all ids were found
	var nf []string
	for _, id := range ids {
		if _, ok := fids[id]; !ok {
			nf = append(nf, id)
		}
	}
	if nf != nil {
		return fmt.Errorf("(RedisCli) the following identifiers do not exist in DB: %s", strings.Join(nf, " "))
	}

	// 2. Select modification operator

	log.D("(RedisCli) Do %v operation on identifiers: %v", op, ids)
	switch op {
		case Update:
			return rc.updateAII(args, fids, add)
		case Delete:
			// TODO
		default:
			panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}


	return nil
}

func (rc *RedisClient) updateAII(args *AIIArgs, ids map[string]QRKey, add bool) error {
	var err error

	// Update tags if exist
	if args.Tags != nil {
		if add {
			err = rc.addTags(args.Tags, ids)
		} else {
			err = rc.setTags(args.Tags, ids)
		}
	}

	// Update description if exist
	if args.Descr != "" {
		if add {
			err = rc.addDescr(args.Descr, ids, args.NoNL)
		} else {
			err = rc.setDescr(args.Descr, ids)
		}
	}

	return err
}

func (rc *RedisClient) addTags(tags []string, ids map[string]QRKey) error {
	// Do for each identifier
	for id, objKey := range ids {
		// AII key
		key := RedisAIIPrefix + id

		// Make a map to make a set of new tags + existing tags
		aiiTags := make(map[string]any, len(tags))
		for _, tag := range tags {
			aiiTags[tag] = nil
		}

		// Load existing values of tags field
		if tagsStr, err := rc.c.HGet(rc.ctx, key, AIIFieldTags).Result(); err == nil {
			// Tags field extracted, make union between extracted existing tags and new tags
			for _, tag := range strings.Split(tagsStr, ",") {
				aiiTags[tag] = nil
			}
		} else if err == RedisNotFound {
			// Ok, currently no tags for this object, nothing to do
		} else {
			// Something went wrong
			return fmt.Errorf("cannot get tags field %q for key %q: %v", AIIFieldTags, key, err)
		}

		// Create sorted list of the full set of tags
		fullTags := make([]string, 0, len(aiiTags))
		for tag := range aiiTags {
			fullTags = append(fullTags, tag)
		}
		sort.Strings(fullTags)

		// Set tags for the current identifier
		if err := rc.setTags(fullTags, map[string]QRKey{id: objKey}); err != nil {
			return err
		}
	}

	// OK
	return nil
}

func (rc *RedisClient) setTags(tags []string, ids map[string]QRKey) error {
	// Make tags field value
	tagsVal := strings.Join(tags, ",")

	// Do for each identifier
	for id, objKey := range ids {
		if err := rc.setAIIField(id, AIIFieldTags, tagsVal, objKey); err != nil {
			return err
		}
	}

	// OK
	return nil
}

func (rc *RedisClient) addDescr(descr string, ids map[string]QRKey, noNL bool) error {
	// Do for each identifier
	for id, objKey := range ids {
		// AII key
		key := RedisAIIPrefix + id

		// Full description
		var fullDescr string

		// Load existing values of description field
		if oldDescr, err := rc.c.HGet(rc.ctx, key, AIIFieldDescr).Result(); err == nil {
			// Append new description line to existing
			if noNL {
				fullDescr = oldDescr + "; " + descr
			} else {
				fullDescr = oldDescr + "\n" + descr
			}
		} else if err == RedisNotFound {
			// Ok, currently no description for this object
			fullDescr = descr
		} else {
			// Something went wrong
			return fmt.Errorf("cannot get description field %q for key %q: %v", AIIFieldDescr, key, err)
		}

		// Set description for the current identifier
		if err := rc.setDescr(fullDescr, map[string]QRKey{id: objKey}); err != nil {
			return err
		}
	}

	// OK
	return nil
}

func (rc *RedisClient) setDescr(descr string, ids map[string]QRKey) error {
	// Do for each identifier
	for id, objKey := range ids {
		if err := rc.setAIIField(id, AIIFieldDescr, descr, objKey); err != nil {
			return err
		}
	}

	// OK
	return nil
}

func (rc *RedisClient) setAIIField(id, field, value string, objKey QRKey) error {
	// Set tags
	res := rc.c.HSet(rc.ctx,
		// Make AII key in 'aii:OBJECT_ID' format
		RedisAIIPrefix + id,
		// Set field
		field, value,
		// Set OID field to have ability to identify AII if the object will be deleted
		AIIFieldOID, objKey.Host + `:` + objKey.Path,
	)

	// Handle error
	if err := res.Err(); err != nil {
		return fmt.Errorf("HSET for key %s (%s = %s) returned error: %v",
			RedisAIIPrefix + id, field, value, err)
	}

	// OK
	return nil
}
