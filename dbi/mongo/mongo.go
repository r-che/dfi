/*
Package mongo provides a driver to work with the Mongo DBMS.

# Authentication configuration

DFI components which use MongoDB server that requires authentication must
provide authentication data (user, password, method, etc.). This is done using
component-specific configuration files. In general, these files contain a
section with MongoDB-specific authentication data in a common format
represented as JSON object.

For example, to authenticate using SCRAM-SHA-1 you must provide the following object:

  {
      "AuthMechanism": "SCRAM-SHA-1",
      "Username": "USER-NAME",
      "Password": "USER-PASSWORD"
  }

See the component's README file, the [official reference], and the [Go-example].

[official reference]: https://www.mongodb.com/docs/manual/core/authentication/
[Go-example]: https://www.mongodb.com/docs/drivers/go/current/fundamentals/auth/
*/
package mongo

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"errors"

//	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

	"github.com/r-che/log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoFieldID		=	"_" + dbms.FieldID
	MongoObjsColl		=	"objs"
	MongoAIIColl		=	"aii"
)


var (
	// Mandatory fields that should be present in all query results
	objMandatoryFields = []string{dbms.FieldHost, dbms.FieldFPath}

	// Sentinel error to return when no private configuration provided
	errNoPrivCfg		=	errors.New("no private configuration")
)

type Client struct {
	*dbms.CommonClient

	c	*mongo.Client

	// Dynamic members
	toDelete	[]string
	updated		int64
	deleted		int64
}

// Once object to execute Ping only once at client creation
var ping = &sync.Once{}

func NewClient(dbCfg *dbms.DBConfig) (*Client, error) {
	// Initialize Mongo client
	mc := &Client{
		CommonClient: dbms.NewCommonClient(dbCfg),
	}

	// Create client options to connect
	opts := options.Client().
		ApplyURI(dbCfg.HostPort)

	// Check for credentials options
	if creds, err := parsePrivCfg(dbCfg.PrivCfg); err == nil {
		opts.SetAuth(*creds)
	} else
	// Check error is not - no private configuration
	if !errors.Is(err, errNoPrivCfg) {
		// Some critical error
		return nil, err
	}

	var err error
	if mc.c, err = mongo.Connect(mc.Ctx, opts); err != nil {
		return nil, fmt.Errorf("(MongoCli:NewClient) cannot create new client to %s: %w", dbCfg.HostPort, err)
	}

	// Run pinging to check that DB is actually available
	go ping.Do(func() {
		log.I("(MongoCli:NewClient) Pinging %s ...", dbCfg.HostPort)
		if err := mc.c.Ping(mc.Ctx, nil); err != nil {
			log.E("(MongoCli:NewClient) Ping returned error: %v", err)
			return
		}
		log.I("(MongoCli:NewClient) Pinging %s was successful", dbCfg.HostPort)
	})

	return mc, nil
}

func DisableStartupPing() {
	// Run once with an empty function to prevent a ping after
	ping.Do(func() {
		log.D("(MongoCli:DisableStartupPing) Disabled pinging on client creation")
	})
}

func parsePrivCfg(pcf map[string]any) (creds *options.Credential, err error) {
	// Check for empty configuration
	if pcf == nil {
		// OK, just return nothing
		return nil, errNoPrivCfg
	}

	// Setting configuration parameter using reflect.Set may raise panic, need to handle it
	var parseField string
	defer func() {
		if p := recover(); p != nil {
			// Clear read configuration
			creds = nil
			// Check for panic value has "string" type
			if s, ok := p.(string); ok {
				// Try to remove unnecessary "reflect.Set:" prefix from the panic value
				p = strings.TrimPrefix(s, "reflect.Set: ")
			}
			// Set error
			err = fmt.Errorf("(MongoCli:parsePrivCfg)" +
				" cannot parse private configuration field %q: %v", parseField, p)
		}
	}()

	// Fill options.Credential structure using reflection, list of available fields see there:
	// https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo/options#Credential
	creds = &options.Credential{}

	s := reflect.ValueOf(creds).Elem()
	for i := 0; i < s.NumField(); i++ {
		// Get the field name
		parseField = s.Type().Field(i).Name
		// Get the field value from configuration
		v, ok := pcf[parseField]
		if !ok {
		// Skip if the field does not exists in the pcf
			continue
		}

		// Set the field value
		s.Field(i).Set(reflect.ValueOf(v))
	}

	return creds, nil
}
