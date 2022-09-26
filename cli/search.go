package main

import (
	"fmt"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func doSearch(dbc dbi.DBClient) error {
	// Get configuration
	c := cfg.Config()

	// Set of requested fields
	rqFields := []string{}
	if c.PrintID() {
		rqFields = append(rqFields, dbi.FieldID)
	}

	qr, err := dbc.Query(c.QueryArgs(), rqFields)
	if err != nil {
		return err
	}

	// TODO
	fmt.Printf("SUCCESS: %#v\n", qr)
	// OK
	return nil
}
