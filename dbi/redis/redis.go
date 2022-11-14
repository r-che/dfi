//go:build dbi_redis
package redis

import (
	"fmt"
	"strconv"

	"github.com/r-che/dfi/types/dbms"

	"github.com/go-redis/redis/v8"
)

const (
	RedisMaxScanKeys	=	1024 * 10

	// Redis namespace prefixes
	RedisObjPrefix		=	"obj:"
	RedisAIIPrefix		=	"aii:"
	RedisAIIDMetaPefix	=	"aii-meta:"
	RedisAIIDSetPrefix	=	RedisAIIDMetaPefix + "set-"

	// Private configuration fields
	userField	=	"user"
	passField	=	"password"

	// Error value of redis.Get* function when requested data is not found
	RedisNotFound	=	redis.Nil
)

type RedisClient struct {
	*dbms.CommonClient

	c	*redis.Client

	// Dynamic members
	toDelete	[]string
	updated		int64
	deleted		int64
}

func NewClient(dbCfg *dbms.DBConfig) (*RedisClient, error) {
	// Convert string representation of database identifier to numeric database index
	dbid, err := strconv.ParseUint(dbCfg.ID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:NewClient) cannot convert database identifier value to unsigned integer: %w", err)
	}

	// Read username/password from private data if set
	user, passw, err := userPasswd(dbCfg.PrivCfg)
	if err != nil {
		return nil, fmt.Errorf("(RedisCli:NewClient) failed to load username/password from private configuration: %w", err)
	}

	// Initialize Redis client
	rc := &RedisClient{
		CommonClient: dbms.NewCommonClient(dbCfg),
		c: redis.NewClient(&redis.Options{
			Addr:		dbCfg.HostPort,
			Username:	user,
			Password:	passw,
			DB:			int(dbid),
		}),
	}

	return rc, nil
}

func userPasswd(pcf map[string]any) (string, string, error) {
	// Check for empty configuration
	if pcf == nil {
		// OK, just return nothing
		return "", "", nil
	}


	loadField := func(field string) (string, error) {
		v, ok := pcf[field]
		if !ok {
			return "", fmt.Errorf("(RedisCli:userPasswd) private configuration does not contain %q field", field)
		}
		if user, ok := v.(string); ok {
			return user, nil
		}
		return "", fmt.Errorf(`(RedisCli:userPasswd) invalid type of %q field in private configuration, got %T, wanted string`,
			field, v)
	}

	// Extract username/password values
	user, err := loadField(userField)
	if err != nil {
		return "", "", err
	}

	passwd, err := loadField(passField)
	if err != nil {
		return "", "", err
	}

	return user, passwd, nil
}