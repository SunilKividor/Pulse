package models

//aws sqs

type S3Event struct {
	Records []Records `json:"Records"`
}

type Records struct {
	S3Events S3 `json:"s3"`
}

type S3 struct {
	SchemaVersion   string   `json:"s3SchemaVersion"`
	ConfigurationId string   `json:"configurationId"`
	Bucket          S3Bucket `json:"bucket"`
	Object          S3Object `json:"object"`
}

type S3Bucket struct {
	Name string `json:"name"`
	ARN  string `json:"arn"`
}

type S3Object struct {
	Key       string `json:"key"`
	Size      int    `json:"size"`
	ETag      string `json:"eTag"`
	Sequencer string `json:"sequencer"`
}
