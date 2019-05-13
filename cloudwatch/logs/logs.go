package logs

import (
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	log "github.com/sirupsen/logrus"
)

type CloudwatchLog struct {
	Owner     string `json:"owner"`
	LogGroup  string `json:"logGroup"`
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

func ProcessFirehoseRecord(record events.KinesisFirehoseEventRecord) (events.CloudwatchLogsData, error) {
	cloudwatchLogsRawData := events.CloudwatchLogsRawData{Data: base64.StdEncoding.EncodeToString(record.Data)}
	return cloudwatchLogsRawData.Parse()
}

func GetFirehoseResponse(recordID string, record events.CloudwatchLogsData) events.KinesisFirehoseResponseRecord {
	if "CONTROL_MESSAGE" == record.MessageType {
		return events.KinesisFirehoseResponseRecord{
			RecordID: recordID,
			Result:   events.KinesisFirehoseTransformedStateDropped,
		}
	} else if "DATA_MESSAGE" == record.MessageType {
		return events.KinesisFirehoseResponseRecord{
			RecordID: recordID,
			Result:   events.KinesisFirehoseTransformedStateOk,
			Data:     transformLogEvent(record),
		}
	} else {
		return events.KinesisFirehoseResponseRecord{
			RecordID: recordID,
			Result:   events.KinesisFirehoseTransformedStateProcessingFailed,
		}
	}
}

func transformLogEvent(cloudwatchLogData events.CloudwatchLogsData) []byte {
	var logs []byte
	for _, logEvent := range cloudwatchLogData.LogEvents {
		output := CloudwatchLog{
			Owner:     cloudwatchLogData.Owner,
			LogGroup:  cloudwatchLogData.LogGroup,
			ID:        logEvent.ID,
			Message:   logEvent.Message,
			Timestamp: logEvent.Timestamp,
		}
		logs = append(logs, output.toBytes()...)
	}
	return logs
}

func (lo *CloudwatchLog) toBytes() []byte {
	var response []byte
	bytes, err := json.Marshal(lo)
	if err != nil {
		log.WithError(err).Error("Failed to encode log to JSON")
	} else {
		response = append(bytes, "\n"...)
	}
	return response
}
