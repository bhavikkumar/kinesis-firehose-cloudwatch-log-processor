package firehose_test

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/bhavikkumar/kinesis-firehose-cloudwatch-log-processor/kinesis/firehose"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetSourceStream(t *testing.T) {
	invalidParmeterError := "invalid parameters, both kinesis and firehose streams specified"
	parseError := "unable to parse value"

	var tests = []struct {
		kinesisStream   string
		firehoseStream  string
		isSourceAStream bool
		region          string
		targetName      string
		err             string
	}{
		{"arn:aws:kinesis:us-west-2:123456789012:stream/test", "", true, "us-west-2", "test", ""},
		{"", "arn:aws:firehose:us-east-1:123456789012:deliverystream/firehoseTest", false, "us-east-1", "firehoseTest", ""},
		{"arn:aws:kinesis:us-west-2:123456789012:stream/test", "arn:aws:firehose:us-east-1:123456789012:deliverystream/firehose", false, "", "", invalidParmeterError},
		{"someRandomString", "", true, "", "", parseError},
		{"", "someRandomString", false, "", "", parseError},
		{"someRandomString", "someRandomString", false, "", "", invalidParmeterError},
		{"arn:aws:kinesis:us-west-2:123456789012:invalidArn", "", true, "us-west-2", "", parseError},
	}

	for _, test := range tests {
		isSourceAStream, region, targetName, err := firehose.GetSourceStream(test.kinesisStream, test.firehoseStream)
		assert.Equal(t, test.isSourceAStream, isSourceAStream)
		assert.Equal(t, test.region, region)
		assert.Equal(t, test.targetName, targetName)
		if err != nil {
			assert.EqualError(t, err, test.err)
		}
	}
}

func TestCreateReingestData(t *testing.T) {
	var tests = []struct {
		record          events.KinesisFirehoseEventRecord
		isSourceAStream bool
		reingestData    firehose.ReingestRecord
	}{
		{events.KinesisFirehoseEventRecord{Data: []byte("test"), KinesisFirehoseRecordMetadata: events.KinesisFirehoseRecordMetadata{PartitionKey: "partitionKey"}}, true, firehose.ReingestRecord{Data: []byte("test"), PartitionKey: "partitionKey"}},
		{events.KinesisFirehoseEventRecord{Data: []byte("firehose")}, false, firehose.ReingestRecord{Data: []byte("firehose")}},
	}

	for _, test := range tests {
		reingestData := firehose.CreateReingestData(test.record, test.isSourceAStream)
		assert.Equal(t, test.reingestData, reingestData)
	}
}
