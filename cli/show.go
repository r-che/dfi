package main

import (
	"fmt"
	//"strings"
	//"sort"

	"github.com/r-che/dfi/dbi"
	"github.com/r-che/dfi/cli/internal/cfg"

	//"github.com/r-che/log"
)

func doShow(dbc dbi.DBClient) error {
	// Get configuration
	c := cfg.Config()

	// Get objects list from DB
	objs, errObjs := dbc.GetObjects(c.CmdArgs, nil/*TODO Set of requested fields*/)
	if errObjs != nil && len(objs) == 0 {
		return fmt.Errorf("cannot get requested objects from DB: %v", errObjs)
	}

	// Get AII for objects
	aiis, errAIIs := dbc.GetAIIs(c.CmdArgs, nil/*TODO Set of requested fields*/)

	printObjs(objs, aiis)

	switch {
	// No errors
	case errObjs == nil && errAIIs == nil:
		return nil
	// Only objects related errors
	case errAIIs == nil:
		return fmt.Errorf("cannot get some objects from DB: %v", errObjs)
	// Only AIIs related errors
	case errObjs == nil:
		return fmt.Errorf("cannot get additional information about some objects: %v", errAIIs)
	// Both get operations returned errors
	default:
		return fmt.Errorf("cannot get some objects from DB: %v; " +
			"cannot get additional information about some objects: %v",
			errObjs, errAIIs)
	}

	// Unreachable
	return fmt.Errorf("Unreachable code")
}

func printObjs(objs, aiis map[string]any) {
	// TODO
}
