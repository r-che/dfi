package main

import (
	"fmt"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"
)

func doSearch(dbc dbi.DBClient) error {
	// Get configuration
	c := cfg.Config()
	fmt.Println("config:", c)
	// OK
	return nil
}
