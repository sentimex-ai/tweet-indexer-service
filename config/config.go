package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

const (
	devPrefix     = "development"
	envProfileKey = "ENVIRONMENT"
)

var cfg *Config

func Load(cfgpath string) *Config {
	fmt.Fprintln(os.Stderr, "Starting to load configuration")

	if cfg != nil {
		fmt.Fprintln(os.Stderr, "Configuration already loaded")
		return cfg
	}

	viper.AddConfigPath(cfgpath)
	viper.SetConfigName("app")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	fmt.Fprintln(os.Stderr, "Attempting to read configuration file")
	if err := viper.ReadInConfig(); err != nil {
		panic("Error reading configuration file: " + err.Error())
	}

	bindEnvVars()
	mergeProfileVars()
	fmt.Fprintln(os.Stderr, "Attempting to unmarshal configuration")
	if err := viper.Unmarshal(&cfg); err != nil {
		panic("Error unmarshalling configuration: " + err.Error())
	}

	return cfg
}

// mergeProfileVars merge configurations to current profile
// app.yaml is the default + app-{current-profile}.yaml
// any configuration under profile development,staging,production
// will override app.yaml default configurations
func mergeProfileVars() {
	env := os.Getenv(envProfileKey)

	switch {
	case env != "":
		viper.SetConfigName("app-" + env)
	default:
		viper.SetConfigName("app-" + devPrefix)
	}

	if err := viper.MergeInConfig(); err != nil {
		panic("Error merging profile config vars. Reason: " + err.Error())
	}
}

// bindEnvVars bind any entry env var
func bindEnvVars() {
	bindEnvVar("persistence.mongodb.url", "MONGODB_URI")
	bindEnvVar("memory.redis.password", "REDIS_PASSWORD")
	bindEnvVar("services.twitterapiservices.bearertoken", "TWITTER_TOKEN")
	bindEnvVar("persistence.s3.key", "S3_KEY")
	bindEnvVar("persistence.s3.secret", "S3_SECRET")
}

func bindEnvVar(key string, envKey string) {
	err := viper.BindEnv(key, envKey)
	if err != nil {
		panic("Error binding env vars. Reason: " + err.Error())
	}
}
func EnvProfile() string {
	return os.Getenv(envProfileKey)
}
