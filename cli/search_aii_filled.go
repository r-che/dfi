package main

import (
	"fmt"
	//"sort"

	//"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
	//"github.com/r-che/dfi/cli/internal/cfg"
)

func searchAIIFilled(dbc dbms.Client) ([]string, error) {
	
	ids, err := dbc.GetAIIs(nil /* Load all AII */, []string{dbms.AIIFieldTags, dbms.AIIFieldDescr})
	if err != nil {
		return nil, err
	}

	fmt.Println("IDS:", ids)
	// TODO
	return nil, nil
}
