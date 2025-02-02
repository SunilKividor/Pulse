package config

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/spf13/viper"
)

type AwsConfig struct {
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Region    string `mapstructure:"region"`
}

func LoadAwsConfig() (*AwsConfig, error) {
	viper.SetConfigName("aws")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to laod the config: %v", err)
	}

	var cfg AwsConfig
	if err := viper.UnmarshalKey("aws", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshall config: %v", err)
	}

	return &cfg, nil
}

func (awsConfig *AwsConfig) NewAwsSession() aws.Config {
	return aws.Config{
		Credentials: credentials.NewStaticCredentials(
			awsConfig.AccessKey,
			awsConfig.SecretKey,
			"",
		),
		Region: &awsConfig.Region,
	}
}
