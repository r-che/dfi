package dbi

import "github.com/r-che/dfi/types"

type DBConfig struct {
	HostPort	string

	User		string
	Password	string
}

type DBOperator int
const (
	Update = DBOperator(iota)
	Delete
)

type DBOperation struct {
	Op DBOperator
	ObjectInfo *types.FSObject
}
