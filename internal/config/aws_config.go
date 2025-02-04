package config

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type AwsConfig struct {
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Region    string `mapstructure:"region"`
}

func NewAwsConfigModel() *AwsConfig {
	return &AwsConfig{
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Region:    os.Getenv("AWS_SQS_REGION"),
	}
}

func (awsConfig *AwsConfig) NewAwsConfig() aws.Config {
	return aws.Config{
		Credentials: credentials.NewStaticCredentials(
			awsConfig.AccessKey,
			awsConfig.SecretKey,
			"",
		),
		Region: &awsConfig.Region,
	}
}
