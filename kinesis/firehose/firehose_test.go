package firehose_test

import (
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"github.com/bhavikkumar/kinesis-firehose-cloudwatch-log-processor/kinesis/firehose"
	"github.com/stretchr/testify/assert"
	"math/rand"
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

func TestProcessRecords(t *testing.T) {
	payload, err := base64.StdEncoding.DecodeString("H4sIAAAAAAAAAJWRTWsbMRCG/8ueLZjRjL5yc9NNLnZDapemlFAkrTYstb3Lep0Qgv97x00KgTSHnAQzmkeP3nmqtmW/j3dl/TiU6qz6PF/Pfy3r1Wp+WVezqn/YlVHK2pK3Hr0Jxkt5099djv1hkE7uh0eVHzZqE7epiarb3fe/ixzDYVJoELRhssYQqsXLlEJ3jd8//biy4QYWz7jVNJa4/TDveQwV+qsada0v/HnthLg/pH0eu2Hq+t1Ft5nKuK/Ofn4EvnpDUAu7Xi6/LL9en3/z1e1f7fq+7KYT+qnqGrEnsi54AGS2wbHWxjCjoWAYGawmzawByIG3Dp0JzjOxsaI8dbKJKW4l1BcTdgg+zP5tSPCeQ/Bso/I+o+I2kUptjgrRlQyasslUHWdvZRwGJ4+HYJGCtiKgQTYKSJ4gODLgAkpFk3f0rkyA1zLGSsvoVsVCRTFakUkNqKxt1IyFc8T/y0gEmoHZo5a/W9HhU0TeWHMyIJaoQC6zDvC+DL6WSW3MqZSkiolJcWoalWybJSNIJTXcRgjV8fb4BwwLrNzwAgAA")
	record := events.KinesisFirehoseEventRecord{
		Data:     payload,
		RecordID: "49578734086442259037497492980620233840400173390482112514000000",
	}

	var records []events.KinesisFirehoseEventRecord
	records = append(records, record)

	responseRecords, reingestRecords := firehose.ProcessRecords(records, true)

	assert.NoError(t, err)
	assert.Len(t, responseRecords, 1)
	assert.Len(t, reingestRecords, 1)
	assert.Equal(t, reingestRecords[0].Data, record.Data)
}

func TestProcessRecordsForReingst(t *testing.T) {
	var responseRecords []events.KinesisFirehoseResponseRecord
	for i := 0; i < 1002; i++ {
		record := events.KinesisFirehoseResponseRecord{
			Data:     randStringBytes(11997),
			Result:   events.KinesisFirehoseTransformedStateOk,
			RecordID: "1",
		}
		responseRecords = append(responseRecords, record)
	}

	failedRecord := events.KinesisFirehoseResponseRecord{
		Result: events.KinesisFirehoseTransformedStateProcessingFailed,
	}
	responseRecords = append(responseRecords, failedRecord)

	var reingestRecords []firehose.ReingestRecord
	for i := 0; i < len(responseRecords); i++ {
		reingestRecord := firehose.ReingestRecord{
			Data: []byte("49578734086442259037497492980620233840400173390482112514000000"),
		}

		reingestRecords = append(reingestRecords, reingestRecord)
	}

	records, reingst, recordsToReingest := firehose.ProcessRecordsForReingst(responseRecords, reingestRecords)

	assert.Equal(t, 502, recordsToReingest)
	assert.Len(t, reingst, 2)
	assert.Equal(t, len(responseRecords), len(records))
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}
