package configs

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
}

func NewConfig(prefix string) *Config {

	// From the environment
	viper.SetEnvPrefix(prefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// From config file
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No configuration file was loaded")
	}

	runtime.GOMAXPROCS(8)

	config := &Config{}

	return config
}

func (config *Config) SetConfigs(configs map[string]interface{}) {

	for k, v := range configs {
		if !viper.IsSet(k) {
			viper.Set(k, v)
		}
	}
}
