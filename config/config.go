// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

// Config represents the configuration of a Hackerbeat
type Config struct {
	Period time.Duration `config:"period"`
}

// DefaultConfig is the default configuration of a Hackerbeat
var DefaultConfig = Config{
	Period: 10 * time.Second,
}
