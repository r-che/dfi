package dbi

import (
	"fmt"
	"strings"
	"sort"

	"github.com/r-che/log"
//	"github.com/r-che/dfi/types"

    rsh "github.com/RediSearch/redisearch-go/redisearch"
)

type idKeyMap map[string]QRKey
func (ikm idKeyMap) String() string {
	ids := make([]string, 0, len(ikm))
	for id := range ikm {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return "[" + strings.Join(ids, " ") + "]"
}

func (rc *RedisClient) ModifyAII(op DBOperator, args *AIIArgs, ids []string, add bool) (int64, int64, error) {
	// 0. Get RediSearch client
	rsc, err := rc.rschInit()
	if err != nil {
		return 0, 0, fmt.Errorf("(RedisCli:ModifyAII) cannot initialize RediSearch client: %v", err)
	}

	// 1. Check for objects with identifiers ids really exist

	log.D("(RedisCli:ModifyAII) Loading object keys for provided identifiers...")

	// Empty query arguments - no special search parameters are required
	qa := &QueryArgs{}
	// Create RediSearch query to get identifiers
	q := rsh.NewQuery(rshQueryIDs(ids, qa))
	// Run search to get results by IDs
	qr, err := rshSearch(rsc, q, []string{FieldID})
	if err != nil {
		return 0, 0, fmt.Errorf("(RedisCli:ModifyAII) search failed: %v", err)
	}

	// Create map indentifiers found in DB
	fids := make(idKeyMap, len(ids))
	for k, v := range qr {
		id, ok := v[FieldID]
		if !ok {
			log.E("(RedisCli:ModifyAII) Loaded invalid object from DB - no ID field (%q) was found: %s:%s", FieldID, k.Host, k.Path)
			continue
		}

		// Convert ID to string representation
		if sid, ok := id.(string); ok {
			fids[sid] = k
		} else {
			log.E("(RedisCli:ModifyAII) Loaded invalid object from DB - " +
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
		return 0, 0, fmt.Errorf("(RedisCli:ModifyAII) the following identifiers do not exist in DB: %s", strings.Join(nf, " "))
	}

	// 2. Select modification operator

	switch op {
		case Update:
			return rc.updateAII(args, fids, add)
		case Delete:
			return rc.deleteAII(args, fids)
		default:
			panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}

	// Unreachable
	return -1, -1, nil
}

func (rc *RedisClient) GetAIIs(ids, retFields []string) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string, len(ids))

	// Get requested fields for each ID
	for _, id := range ids {
		key := RedisAIIPrefix + id

		res, err := rc.c.HGetAll(rc.ctx, key).Result()
		if err != nil {
			return result, fmt.Errorf("cannot get AII for %q: %v", key, err)
		}

		// Check for nothing was found
		if len(res) == 0 {
			// No AII data for this id
			continue
		}

		result[id] = make(map[string]string, len(retFields))
		for _, field := range retFields {
			if v, ok := res[field]; ok {
				result[id][field] = v
			}
		}
	}

	// OK
	return result, nil
}

func (rc *RedisClient) updateAII(args *AIIArgs, ids idKeyMap, add bool) (int64, int64, error) {
	var err error

	ttu := int64(0)	// Total tags updated
	tdu := int64(0)	// Total descriptions updated

	// Update tags if exist
	if args.Tags != nil {
		if add {
			ttu, err = rc.addTags(args.Tags, ids)
		} else {
			ttu, err = rc.setTags(args.Tags, ids)
		}

		if err != nil {
			return ttu, tdu, fmt.Errorf("(RedisCli) %v", err)
		}
	}

	// Update description if exist
	if args.Descr != "" {
		if add {
			tdu, err = rc.addDescr(args.Descr, ids, args.NoNL)
		} else {
			tdu, err = rc.setDescr(args.Descr, ids)
		}

		if err != nil {
			return ttu, tdu, fmt.Errorf("(RedisCli) %v", err)
		}
	}

	// OK
	return ttu, tdu, nil
}

func (rc *RedisClient) addTags(tags []string, ids idKeyMap) (int64, error) {
	tu := int64(0)	// Total updated tags fields

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
		tagsStr, err := rc.c.HGet(rc.ctx, key, AIIFieldTags).Result()
		if err == nil {
			// Tags field extracted, make union between extracted existing tags and new tags
			for _, tag := range strings.Split(tagsStr, ",") {
				aiiTags[tag] = nil
			}
		} else if err == RedisNotFound {
			// Ok, currently no tags for this object, nothing to do
		} else {
			// Something went wrong
			return tu, fmt.Errorf("cannot get tags field %q for key %q: %v", AIIFieldTags, key, err)
		}

		// Create sorted list of the full set of tags
		fullTags := make([]string, 0, len(aiiTags))
		for tag := range aiiTags {
			fullTags = append(fullTags, tag)
		}
		sort.Strings(fullTags)

		// Compare existing tags and new set
		if tagsStr == strings.Join(fullTags, ",") {
			// Skip update
			log.D("(RedisCli:addTags) No tags update required for %s", id)
			continue
		}

		// Set tags for the current identifier
		if _, err := rc.setTags(fullTags, idKeyMap{id: objKey}); err != nil {
			return tu, err
		}

		tu++
	}

	// OK
	return tu, nil
}

func (rc *RedisClient) setTags(tags []string, ids idKeyMap) (int64, error) {
	// Make tags field value
	tagsVal := strings.Join(tags, ",")

	log.D("(RedisCli:setTags) %q => %s", tagsVal, ids)

	ts := int64(0)	// Total tags set

	// Do for each identifier
	for id, objKey := range ids {
		if err := rc.setAIIField(id, AIIFieldTags, tagsVal, objKey); err != nil {
			return ts, err
		}
		ts++
	}

	// OK
	return ts, nil
}

func (rc *RedisClient) addDescr(descr string, ids idKeyMap, noNL bool) (int64, error) {
	tu := int64(0)	// Total updated description fields

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
			return tu, fmt.Errorf("cannot get description field %q for key %q: %v", AIIFieldDescr, key, err)
		}

		// Set description for the current identifier
		n, err := rc.setDescr(fullDescr, idKeyMap{id: objKey})
		if err != nil {
			return tu, err
		}

		tu += n
	}

	// OK
	return tu, nil
}

func (rc *RedisClient) setDescr(descr string, ids idKeyMap) (int64, error) {
	tu := int64(0)	// Total updated description fields

	// Do for each identifier
	for id, objKey := range ids {
		if err := rc.setAIIField(id, AIIFieldDescr, descr, objKey); err != nil {
			return tu, err
		}

		tu++
	}

	// OK
	return tu, nil
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

func (rc *RedisClient) deleteAII(args *AIIArgs, ids idKeyMap) (int64, int64, error) {
	var err error

	td := int64(0)	// Tags deleted
	dd := int64(0)	// Descriptions deleted

	// Delete tags if requested
	if args.Tags != nil {
		// Check for first tags for ALL value
		if args.Tags[0] == AIIAllTags {
			// Need to clear all tags
			td, err = rc.clearAIIField(AIIFieldTags, ids)
		} else {
			// Need to remove the separate tags
			td, err = rc.delTags(args.Tags, ids)
		}

		if err != nil {
			return td, dd, fmt.Errorf("(RedisCli) %v", err)
		}
	}

	// Delete description if requested
	if args.Descr == AIIDelDescr {
		// Clear description for selected identifiers
		dd, err = rc.clearAIIField(AIIFieldDescr, ids)
		if err != nil {
			return td, dd, fmt.Errorf("(RedisCli) %v", err)
		}
	}

	// OK
	return td, dd, nil
}

func (rc *RedisClient) delTags(tags []string, ids idKeyMap) (int64, error) {
	// Convert list of tags to map to check existing tags for need to be deleted
	toDelTags := make(map[string]bool, len(tags))
	for _, tag := range tags {
		toDelTags[tag] = true
	}

	tu := int64(0) // Total changed values of tags fields

	// List of AII when tags field should be cleared
	clearTags := make(idKeyMap, len(ids))

	log.D("(RedisCli) Collecting AII existing tags")
	// Do for each identifier
	for id, objKey := range ids {
		// Make a key
		key := RedisAIIPrefix + id

		// Get list of keys of this hash
		aiiTagsStr, err := rc.c.HGet(rc.ctx, key, AIIFieldTags).Result()
		if err != nil {
			if err == RedisNotFound {
				// No tags field there, skip
				continue
			}
			return tu, fmt.Errorf("cannot get value %q field of %q: %v", AIIFieldTags, key, err)
		}

		// Split string by set of tags
		aiiTags := strings.Split(aiiTagsStr, ",")

		// Select tags to keep
		keepTags := make([]string, 0, len(aiiTags))
		for _, tag := range aiiTags {
			if !toDelTags[tag] {
				keepTags = append(keepTags, tag)
			}
		}

		// Check for nothing to keep
		if len(keepTags) == 0 {
			// All tags should be removed from this item, add to queue to deletion
			clearTags[id] = objKey
			// Now continue with the next id
			continue
		}

		// Compare value of keepTags and existing tags
		if strings.Join(keepTags, ",") == aiiTagsStr {
			// No tags should be removed from this item, skip
			log.D("(RedisCli:delTags) No tags update required for %s", id)
			continue
		}

		// Need to set new value of tags field value without removed tags
		n, err := rc.setTags(keepTags, idKeyMap{id: objKey})
		if err != nil {
			return tu, fmt.Errorf("cannot remove tags %v from %q: %v", tags, id, err)
		}

		tu += n
	}

	// Check for AII from which need to remove the tags field
	if len(clearTags) != 0 {
		// Call clear tags for this AII
		n, err := rc.clearAIIField(AIIFieldTags, ids)
		if err != nil {
			return tu, fmt.Errorf("cannot clear tags: %v", err)
		}

		tu += n
	}

	// OK
	return tu, nil
}

func (rc *RedisClient) clearAIIField(field string, ids idKeyMap) (int64, error) {
	// List of keys that can be safely deleted to clearing field
	toDelKey := make([]string, 0, len(ids))
	// List of keys on which only the field should be deleted
	toDelField := make([]string, 0, len(ids))

	// Total cleared
	tc := int64(0)

	log.D("(RedisCli) Collecting AII info to clearing field %q...", field)
	// Do for each identifier
	for id := range ids {
		// Make a key
		key := RedisAIIPrefix + id

		// Get list of keys of this hash
		keys, err := rc.c.HKeys(rc.ctx, key).Result()
		if err != nil {
			return tc, fmt.Errorf("cannot get number of keys for %q: %v", key, err)
		}

		// Count number of fields that are not OID or cleared field
		nOther := 0
		ff := false	// Field found
		for _, f := range keys {
			// Check for field name selected from DB (f) is cleared field name
			if field == f {
				ff = true
			} else if f != AIIFieldOID {
				// Selected name f is not cleared field name
				// and not OID field name - this is some other field
				nOther++
			}
		}

		// Check for cleared field was not found
		if !ff {
			// Skip this ID
			continue
		}

		// Check number of other fields
		if nOther == 0 {
			// This key can be deleted because does not contain something other
			// that OID and the cleared field which has to be deleted
			toDelKey = append(toDelKey, key)
		} else {
			// Need to delete only the cleared field
			toDelField = append(toDelField, key)
		}
	}

	// Check for nothing to delete
	if len(toDelKey) == 0 && len(toDelField) == 0 {
		log.D("(RedisCli) Field %q are not set for these objects", field)
		// OK
		return tc, nil
	}

	// Check for keys to delete
	if len(toDelKey) != 0 {
		log.D("(RedisCli) AII will be deleted because there are no valuable fields than %q: %v", field, toDelKey)

		res := rc.c.Del(rc.ctx, toDelKey...)
		if res.Err() != nil {
			return tc, fmt.Errorf("cannot delete AII keys %v: %v", toDelKey, res.Err())
		}

		tc += res.Val()
	}

	if len(toDelField) != 0 {
		log.D("(RedisCli) The field %q will be removed from AII: %v", field, toDelField)
		// Delete the cleared field from each entry
		for _, key := range toDelField {
			n, err := rc.c.HDel(rc.ctx, key, field).Result()
			if err != nil {
				return tc, fmt.Errorf("cannot remove field %q from key %q: %v", field, key, err)
			}
			tc += n
		}
	}

	// OK
	return tc, nil
}
