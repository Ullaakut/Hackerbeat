// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

// Config represents the configuration of a Hackerbeat
type Config struct {
	// Period determines the frequency at which the beat is running
	Period time.Duration `config:"period"`

	// Timeout is the limit time value after which we consider an HTTP request failed
	Timeout time.Duration `config:"timeout"`

	// NumberOfStories is the number of stories we want to index from the top stories
	NumberOfStories int `config:"number_of_stories"`
}

// DefaultConfig is the default configuration of a Hackerbeat
var DefaultConfig = Config{
	Period:          60 * time.Second,
	Timeout:         10 * time.Second,
	NumberOfStories: 10,
}
