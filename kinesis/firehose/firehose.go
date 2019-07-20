package firehose

import (
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/bhavikkumar/kinesis-firehose-cloudwatch-log-processor/cloudwatch/logs"
	"strings"
)

type ReingestRecord struct {
	Data         []byte `json:"data"`
	PartitionKey string `json:"partitionKey,omitempty"`
}

func ProcessFirehoseEvent(firehoseEvent events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	isSourceAStream, _, _, err := GetSourceStream(firehoseEvent.SourceKinesisStreamArn, firehoseEvent.DeliveryStreamArn)
	if err != nil {
		return events.KinesisFirehoseResponse{}, err
	}

	responseRecords, _ := ProcessRecords(firehoseEvent.Records, isSourceAStream)
	// Process for reingestion

	return events.KinesisFirehoseResponse{Records: responseRecords}, err
}

func ProcessRecords(records []events.KinesisFirehoseEventRecord, isSourceAStream bool) ([]events.KinesisFirehoseResponseRecord, []ReingestRecord) {
	response := make([]events.KinesisFirehoseResponseRecord, len(records))
	reingestData := make([]ReingestRecord, len(records))
	for i, record := range records {
		cloudwatchLogData, _ := logs.ProcessFirehoseRecord(record)
		response[i] = logs.GetFirehoseResponse(record.RecordID, cloudwatchLogData)
		reingestData[i] = CreateReingestData(record, isSourceAStream)
	}
	return response, reingestData
}

func ProcessRecordsForReingst(processedRecords []events.KinesisFirehoseResponseRecord, reingestRecords []ReingestRecord) ([]events.KinesisFirehoseResponseRecord, [][]ReingestRecord, int) {
	projectedSize := 0
	totalRecordsToBeReingested := 0
	var responseRecords []events.KinesisFirehoseResponseRecord
	var putRecordBatches [][]ReingestRecord
	var recordsToReingest []ReingestRecord

	for i, record := range processedRecords {
		if record.Result != events.KinesisFirehoseTransformedStateOk {
			responseRecords = append(responseRecords, record)
			continue
		}

		projectedSize += len(record.RecordID) + len(record.Result) + len(record.Data)
		// 6000000 instead of 6291456 to leave ample headroom for the stuff we didn't account for
		if projectedSize > 6000000 {
			totalRecordsToBeReingested += 1
			recordsToReingest = append(recordsToReingest, reingestRecords[i])

			record.Result = events.KinesisFirehoseTransformedStateDropped
			record.Data = nil

			if len(recordsToReingest) == 500 {
				putRecordBatches = append(putRecordBatches, recordsToReingest) // Send to source
				recordsToReingest = recordsToReingest[:0]
			}
		}
		responseRecords = append(responseRecords, record)
	}

	if len(recordsToReingest) > 0 {
		putRecordBatches = append(putRecordBatches, recordsToReingest) // Send to source
		recordsToReingest = recordsToReingest[:0]
	}
	return responseRecords, putRecordBatches, totalRecordsToBeReingested
}

func CreateReingestData(record events.KinesisFirehoseEventRecord, isSourceAStream bool) ReingestRecord {
	data := ReingestRecord{Data: record.Data}
	if isSourceAStream {
		data.PartitionKey = record.KinesisFirehoseRecordMetadata.PartitionKey
	}
	return data
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
