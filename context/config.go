package context

import (
	"io/ioutil"
	"launchpad.net/goyaml"
)

// Config contains the configuration information that is used in the utilities exposed by the Context.
type Config struct {
	Version     string `yaml:"version,omitempty"`
	LogFile     string `yaml:"log,omitempty"`
	LogLevel    string `yaml:"log_level,omitempty"`
	StatsAddr   string `yaml:"statsd,omitempty"`
	StatsPrefix string `yaml:"statsd_prefix,omitempty"`
}

// ConfigFromFile loads the specified yaml file and creates a Config from it.
func ConfigFromFile(path string) (Config, error) {
	var config Config
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return config, err
	}
	err = goyaml.Unmarshal(configBytes, &config)
	return config, err
}
