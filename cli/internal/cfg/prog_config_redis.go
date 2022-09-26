//go:build dbi_redis
package cfg

import "fmt"

func (pc *progConfig) prepareDBMS() error {
	if pc.deepSearch && pc.onlyName {
		return fmt.Errorf("(Redis) --deep and --only-name modes are incompatible")
	}
	return nil
}
