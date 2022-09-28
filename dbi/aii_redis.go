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
			return rc.deleteAII(args, fids)
		default:
			panic(fmt.Sprintf("Unsupported AAI modification operator %v", op))
	}


	return nil
}

func (rc *RedisClient) deleteAII(args *AIIArgs, ids map[string]QRKey) error {
	var err error

	// Delete tags if requested
	if args.Tags != nil {
		// Check for first tags for ALL value
		if args.Tags[0] == AIIAllTags {
			// Need to clear all tags
			err = rc.clearTags(ids)
		} else {
			// Need to remove the separate tags
			err = rc.delTags(args.Tags, ids)
		}
	}
	if err != nil {
		return fmt.Errorf("(RedisCli) %v", err)
	}

	// Delete description if requested
	if args.Descr == AIIDelDescr {
		err = fmt.Errorf("Not implemented")	// TODO
	}
	if err != nil {
		return fmt.Errorf("(RedisCli) %v", err)
	}

	// OK
	return nil
}

func (rc *RedisClient) clearTags(ids map[string]QRKey) error {
	// List of keys that can be safely deleted to clearing tags
	toDelKey := make([]string, 0, len(ids))
	// List of keys on which only the tags field should be deleted
	toDelField := make([]string, 0, len(ids))

	log.D("(RedisCli) Collecting AII info to clearing tags...")
	// Do for each identifier
	for id := range ids {
		// Make a key
		key := RedisAIIPrefix + id

		// Get list of keys of this hash
		keys, err := rc.c.HKeys(rc.ctx, key).Result()
		if err != nil {
			return fmt.Errorf("cannot get number of keys for %q: %v", key, err)
		}

		// Count number of fields that are not OID or tags
		nOther := 0
		tagsFound := false
		for _, field := range keys {
			if field == AIIFieldTags {
				tagsFound = true
			} else if field != AIIFieldOID {
				nOther++
			}
		}

		// Check for tags field was not found
		if !tagsFound {
			// Skip this ID
			continue
		}

		// Check number of other fields
		if nOther == 0 {
			// This key can be deleted because does not contain something other
			// that OID and the tags field which has to be deleted
			toDelKey = append(toDelKey, key)
		} else {
			// Need to delete only the tags field
			toDelField = append(toDelField, key)
		}
	}

	// Check for nothing to delete
	if len(toDelKey) == 0 && len(toDelField) == 0 {
		log.D("(RedisCli) Tags are not set for these objects")
		// OK
		return nil
	}


	// Check for keys to delete
	if len(toDelKey) != 0 {
		log.D("(RedisCli) AII will be deleted because there are no valuable fields than %q: %v", AIIFieldTags, toDelKey)

		if res := rc.c.Del(rc.ctx, toDelKey...); res.Err() != nil {
			return fmt.Errorf("cannot delete AII keys %v: %v", toDelKey, res.Err())
		}
	}

	if len(toDelField) != 0 {
		log.D("(RedisCli) The field %q will be removed from AII: %v", AIIFieldTags, toDelField)
		// Delete tags field from each entry
		for _, key := range toDelField {
			// Ma
			if _, err := rc.c.HDel(rc.ctx, key, AIIFieldTags).Result(); err != nil {
				return fmt.Errorf("cannot remove field %q from key %q: %v", AIIFieldTags, key, err)
			}
		}
	}

	// OK
	return nil
}

func (rc *RedisClient) delTags(tags []string, ids map[string]QRKey) error {
	// Convert list of tags to map to check existing tags for need to be deleted
	toDelTags := make(map[string]bool, len(tags))
	for _, tag := range tags {
		toDelTags[tag] = true
	}

	// List of AII when tags field should be cleared
	clearTags := make(map[string]QRKey, len(ids))

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
			return fmt.Errorf("cannot get value %q field of %q: %v", AIIFieldTags, key, err)
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
			continue
		}

		// Need to set new value of tags field value without removed tags
		if err := rc.setTags(keepTags, map[string]QRKey{id: objKey}); err != nil {
			return fmt.Errorf("cannot remove tags %v from %q: %v", tags, id, err)
		}
	}

	// Check for AII from which need to remove the tags field
	if len(clearTags) != 0 {
		// Call clear tags for this AII
		if err := rc.clearTags(clearTags); err != nil {
			return fmt.Errorf("cannot clear tags: %v", err)
		}
	}

	// OK
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
	if err != nil {
		return fmt.Errorf("(RedisCli) %v", err)
	}

	// Update description if exist
	if args.Descr != "" {
		if add {
			err = rc.addDescr(args.Descr, ids, args.NoNL)
		} else {
			err = rc.setDescr(args.Descr, ids)
		}
	}
	if err != nil {
		return fmt.Errorf("(RedisCli) %v", err)
	}

	// OK
	return nil
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
