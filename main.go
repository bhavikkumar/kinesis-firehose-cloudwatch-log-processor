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
6) Any additional records which exceed 6MB will be re-ingested back into the source Kinesis stream or Firehose.
*/
package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/bhavikkumar/kinesis-firehose-cloudwatch-log-processor/cloudwatch/logs"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

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

func handleRequest(firehoseEvent events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	processedRecords := processRecords(firehoseEvent.Records)
	//isSourceAStream, region, streamName := getSourceStream(firehoseEvent.SourceKinesisStreamArn, firehoseEvent.DeliveryStreamArn)

	//processRecordsForReingst(processedRecords, isSourceAStream, region, streamName)

	return events.KinesisFirehoseResponse{Records: processedRecords}, nil
}

func getSourceStream(sourceKinesisStream string, firehoseDeliveryStream string) (bool, string, string) {
	isSourceAStream := sourceKinesisStream != ""
	streamARN := sourceKinesisStream
	if !isSourceAStream {
		streamARN = firehoseDeliveryStream
	}
	region := strings.Split(streamARN, ":")[3]
	streamName := strings.Split(streamARN, "/")[1]
	return isSourceAStream, region, streamName
}

//func processRecordsForReingst(processedRecords []events.KinesisFirehoseResponseRecord) {
//	projectedSize := 0
//	for i, record := range processedRecords {
//		projectedSize += len(record.RecordID) + len(record.Result) + len(record.Data)
//		// 6000000 instead of 6291456 to leave ample headroom for the stuff we didn't account for
//		if projectedSize > 6000000 && record.Result != events.KinesisFirehoseTransformedStateOk {
//
//		}
//	}
//
//}

func processRecords(records []events.KinesisFirehoseEventRecord) []events.KinesisFirehoseResponseRecord {
	response := make([]events.KinesisFirehoseResponseRecord, len(records))
	for i, record := range records {
		cloudwatchLogData, _ := logs.ProcessFirehoseRecord(record)
		response[i] = logs.GetFirehoseResponse(record.RecordID, cloudwatchLogData)
	}
	return response
}
