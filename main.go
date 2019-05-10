/*
For processing data sent to Firehose by Cloudwatch Logs subscription filters.

Cloudwatch Logs sends to Firehose records that look like this:

{
	"messageType": "DATA_MESSAGE",
	"owner": "123456789012",
	"logGroup": "log_group_name",
	"logStream": "log_stream_name",
	"subscriptionFilters": [
		"subscription_filter_name"
	],
	"logEvents": [
		{
			"id": "01234567890123456789012345678901234567890123456789012345",
			"timestamp": 1510109208016,
			"message": "log message 1"
		},
		{
			"id": "01234567890123456789012345678901234567890123456789012345",
			"timestamp": 1510109208017,
			"message": "log message 2"
		}
		...
	]
}

The data is additionally compressed with GZIP.

The code below will:

1) Gunzip the data
2) Parse the json
3) Set the result to Dropped for any record whose messageType is CONTROL_MESSAGE. Set the result to ProcessingFailed for
any record whose messageType is not DATA_MESSAGE, thus redirecting them to the processing error output. Such records do
not contain any log events. You can modify the code to set the result to Dropped instead to get rid of these records
completely.
4) For records whose messageType is DATA_MESSAGE, pass the Cloudwatch Log Data to the transformLogEvent method. You can
modify the transformLogEvent method to perform custom transformations on the log events.
5) Concatenate the result from (4) together and set the result as the data of the record returned to Firehose. Note that
this step will not add any delimiters. Delimiters should be appended by the logic within the transformLogEvent
method.
*/
package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	log "github.com/sirupsen/logrus"
	"os"
)

type KinesisFirehoseEventExtended struct {
	KinesisStreamArn string `json:"sourceKinesisStreamArn"`
	events.KinesisFirehoseEvent
}

type LogOutput struct {
	Owner     string `json:"owner"`
	LogGroup  string `json:"logGroup"`
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(firehoseEvent KinesisFirehoseEventExtended) (events.KinesisFirehoseResponse, error) {
	records := processRecords(firehoseEvent.Records)
	return events.KinesisFirehoseResponse{Records: records}, nil
}

func processRecords(records []events.KinesisFirehoseEventRecord) []events.KinesisFirehoseResponseRecord {
	response := make([]events.KinesisFirehoseResponseRecord, len(records))
	for i, record := range records {
		cloudwatchLogData, _ := processRecord(record)
		if "CONTROL_MESSAGE" == cloudwatchLogData.MessageType {
			response[i] = events.KinesisFirehoseResponseRecord{
				RecordID: record.RecordID,
				Result:   events.KinesisFirehoseTransformedStateDropped,
			}
		} else if "DATA_MESSAGE" == cloudwatchLogData.MessageType {
			response[i] = events.KinesisFirehoseResponseRecord{
				RecordID: record.RecordID,
				Result:   events.KinesisFirehoseTransformedStateOk,
				Data:     transformLogEvent(cloudwatchLogData),
			}
		} else {
			response[i] = events.KinesisFirehoseResponseRecord{
				RecordID: record.RecordID,
				Result:   events.KinesisFirehoseTransformedStateProcessingFailed,
			}
		}
	}
	return response
}

func processRecord(record events.KinesisFirehoseEventRecord) (events.CloudwatchLogsData, error) {
	cloudwatchLogsRawData := events.CloudwatchLogsRawData{Data: base64.StdEncoding.EncodeToString(record.Data)}
	return cloudwatchLogsRawData.Parse()
}

func transformLogEvent(cloudwatchLogData events.CloudwatchLogsData) []byte {
	var logs []byte
	for _, logEvent := range cloudwatchLogData.LogEvents {
		output := LogOutput{
			Owner:     cloudwatchLogData.Owner,
			LogGroup:  cloudwatchLogData.LogGroup,
			ID:        logEvent.ID,
			Message:   logEvent.Message,
			Timestamp: logEvent.Timestamp,
		}
		logs = append(logs, output.ToBytes()...)
	}
	return logs
}

func (lo LogOutput) ToBytes() []byte {
	var response []byte
	bytes, err := json.Marshal(lo)
	if err != nil {
		log.WithError(err).Error("Failed to encode log to JSON")
	} else {
		response = append(bytes, "\n"...)
	}
	return response
}
