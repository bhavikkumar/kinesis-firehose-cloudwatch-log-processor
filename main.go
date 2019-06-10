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
	"github.com/bhavikkumar/kinesis-firehose-cloudwatch-log-processor/kinesis/firehose"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(firehoseEvent events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	return firehose.ProcessFirehoseEvent(firehoseEvent)
}
