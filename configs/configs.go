package configs

import (
	"fmt"
	"reflect"
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

func (config *Config) GetAllSettings() map[string]interface{} {
	return viper.AllSettings()
}

func (config *Config) PrintAllSettings() {

	fmt.Println("[List of current configs]")
	config.PrintSettings("", config.GetAllSettings())
	fmt.Println("")
}

func (config *Config) PrintSettings(scope string, settings map[string]interface{}) {

	for k, v := range settings {

		domain := k
		if len(scope) > 0 {
			domain = scope + "." + k
		}

		switch reflect.TypeOf(v).Kind() {
		case reflect.Map:
			config.PrintSettings(domain, v.(map[string]interface{}))
		default:
			fmt.Printf("%s=%v\n", domain, v)
		}
	}
}
