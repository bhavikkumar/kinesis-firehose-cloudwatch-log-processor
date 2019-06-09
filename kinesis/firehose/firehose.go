package firehose

import (
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/bhavikkumar/kinesis-firehose-cloudwatch-log-processor/cloudwatch/logs"
	"strings"
)

func ProcessFirehoseEvent(firehoseEvent events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	records := firehoseEvent.Records
	response := make([]events.KinesisFirehoseResponseRecord, len(records))
	for i, record := range records {
		cloudwatchLogData, _ := logs.ProcessFirehoseRecord(record)
		response[i] = logs.GetFirehoseResponse(record.RecordID, cloudwatchLogData)
	}
	return events.KinesisFirehoseResponse{Records: response}, nil
}

func GetSourceStream(sourceKinesisStream string, firehoseDeliveryStream string) (bool, string, string, error) {
	if sourceKinesisStream != "" && firehoseDeliveryStream != "" {
		return false, "", "", errors.New("invalid parameters, both kinesis and firehose streams specified")
	}

	isSourceAStream := sourceKinesisStream != ""
	streamARN := sourceKinesisStream
	if !isSourceAStream {
		streamARN = firehoseDeliveryStream
	}

	region, err := splitAndGetValue(streamARN, ":", 3)
	if err != nil {
		return isSourceAStream, "", "", err
	}

	targetName, err := splitAndGetValue(streamARN, "/", 1)
	if err != nil {
		return isSourceAStream, region, "", err
	}
	return isSourceAStream, region, targetName, nil
}

func splitAndGetValue(s string, sep string, location int) (string, error) {
	values := strings.Split(s, sep)
	if len(values) <= location {
		return "", errors.New("unable to parse value")
	}
	return values[location], nil
}
