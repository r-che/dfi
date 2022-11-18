package common

import (
	"crypto/sha1"
	"fmt"

	"github.com/r-che/dfi/types"
)

// MakeID makes the identifier (most unique) for a particular filesystem object
func MakeID(host string, fso *types.FSObject) string {
	// Use found path as value to generate the identifier
	return fmt.Sprintf("%x", sha1.Sum([]byte(host + ":" + fso.FPath)))
}
