package mongo

import (
	"fmt"

	"github.com/r-che/dfi/types"
	"github.com/r-che/dfi/types/dbms"
//	"github.com/r-che/dfi/dbi/common"

//	"github.com/r-che/log"
)

//
// Agent client interface
//

func (mc *MongoClient) UpdateObj(fso *types.FSObject) error {
	return fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) DeleteObj(fso *types.FSObject) error {
	return fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) Commit() (int64, int64, error) {
	return -1, -1, fmt.Errorf("Not implemented")	// TODO
}
func (mc *MongoClient) SetReadOnly(ro bool) {
	// TODO
}
func (mc *MongoClient) TermLong() {
	// TODO
}
func (mc *MongoClient) Stop() {
	// TODO
}
func (mc *MongoClient) LoadHostPaths(match dbms.MatchStrFunc) ([]string, error) {
	return nil, fmt.Errorf("Not implemented")	// TODO
}
