package main

import (
	"fmt"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func doSearch(dbc dbi.DBClient) error {
	// Get configuration
	c := cfg.Config()

	qr, err := dbc.Query(c.CmdArgs(), c.QueryArgs(), []string{"id", "size"}/*TODO*/)
	if err != nil {
		return err
	}

	// TODO
	fmt.Printf("SUCCESS: %#v\n", qr)
	// OK
	return nil
}
