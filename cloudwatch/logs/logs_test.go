package logs_test

import (
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/bhavikkumar/kinesis-firehose-cloudwatch-log-processor/cloudwatch/logs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProcessFirehoseRecord(t *testing.T) {
	payload, err := base64.StdEncoding.DecodeString("H4sIAAAAAAAAAJWRTWsbMRCG/8ueLZjRjL5yc9NNLnZDapemlFAkrTYstb3Lep0Qgv97x00KgTSHnAQzmkeP3nmqtmW/j3dl/TiU6qz6PF/Pfy3r1Wp+WVezqn/YlVHK2pK3Hr0Jxkt5099djv1hkE7uh0eVHzZqE7epiarb3fe/ixzDYVJoELRhssYQqsXLlEJ3jd8//biy4QYWz7jVNJa4/TDveQwV+qsada0v/HnthLg/pH0eu2Hq+t1Ft5nKuK/Ofn4EvnpDUAu7Xi6/LL9en3/z1e1f7fq+7KYT+qnqGrEnsi54AGS2wbHWxjCjoWAYGawmzawByIG3Dp0JzjOxsaI8dbKJKW4l1BcTdgg+zP5tSPCeQ/Bso/I+o+I2kUptjgrRlQyasslUHWdvZRwGJ4+HYJGCtiKgQTYKSJ4gODLgAkpFk3f0rkyA1zLGSsvoVsVCRTFakUkNqKxt1IyFc8T/y0gEmoHZo5a/W9HhU0TeWHMyIJaoQC6zDvC+DL6WSW3MqZSkiolJcWoalWybJSNIJTXcRgjV8fb4BwwLrNzwAgAA")
	record := events.KinesisFirehoseEventRecord{
		Data:     payload,
		RecordID: "49578734086442259037497492980620233840400173390482112514000000",
	}

	expected := events.CloudwatchLogsData{
		MessageType:         "DATA_MESSAGE",
		LogGroup:            "copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L",
		Owner:               "263868185958",
		LogStream:           "copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7",
		SubscriptionFilters: []string{"copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8"},
		LogEvents: []events.CloudwatchLogsLogEvent{
			{
				ID:        "33679800144697422554415395414062324420037086717597843456",
				Message:   "8499846a-88c1-4fb3-bfca-117ec023c5c3",
				Timestamp: 1510254471089,
			},
			{
				ID:        "33679800144719723299613926037203860138309735079103823873",
				Message:   "5615152f-ae3e-4163-bbd0-c26a241e4ca1",
				Timestamp: 1510254471090,
			}, {
				ID:        "33679800144742024044812456660345395856582383440609804290",
				Message:   "bfacbeeb-e5ab-4bdd-b6fc-4f0bebd4fa09",
				Timestamp: 1510254471091,
			},
		},
	}

	actual, err := logs.ProcessFirehoseRecord(record)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestControlMessageDropped(t *testing.T) {
	logData := events.CloudwatchLogsData{
		MessageType: "CONTROL_MESSAGE",
	}

	expected := events.KinesisFirehoseResponseRecord{
		RecordID: "test001",
		Result:   events.KinesisFirehoseTransformedStateDropped,
	}

	actual := logs.GetFirehoseResponse("test001", logData)
	assert.Equal(t, expected, actual)
}

func TestUnknownMessageFailed(t *testing.T) {
	logData := events.CloudwatchLogsData{
		MessageType: "UNKNOWN_MESSAGE",
	}

	expected := events.KinesisFirehoseResponseRecord{
		RecordID: "test002",
		Result:   events.KinesisFirehoseTransformedStateProcessingFailed,
	}

	actual := logs.GetFirehoseResponse("test002", logData)
	assert.Equal(t, expected, actual)
}

func TestLogMessageWithSingleEvent(t *testing.T) {
	cwData := events.CloudwatchLogsData{
		MessageType:         "DATA_MESSAGE",
		LogGroup:            "copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L",
		Owner:               "263868185958",
		LogStream:           "copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7",
		SubscriptionFilters: []string{"copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8"},
		LogEvents: []events.CloudwatchLogsLogEvent{
			{
				ID:        "33679800144697422554415395414062324420037086717597843456",
				Message:   "8499846a-88c1-4fb3-bfca-117ec023c5c3",
				Timestamp: 1510254471089,
			},
		},
	}

	logData := logs.CloudwatchLog{
		Owner:     "263868185958",
		ID:        "33679800144697422554415395414062324420037086717597843456",
		LogGroup:  "copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L",
		Message:   "8499846a-88c1-4fb3-bfca-117ec023c5c3",
		Timestamp: 1510254471089,
	}

	expectedPayload, _ := json.Marshal(&logData)
	expectedPayload = append(expectedPayload, "\n"...)

	expected := events.KinesisFirehoseResponseRecord{
		RecordID: "test003",
		Result:   events.KinesisFirehoseTransformedStateOk,
		Data:     expectedPayload,
	}

	actual := logs.GetFirehoseResponse("test003", cwData)
	assert.Equal(t, expected, actual)
}

func TestLogMessageWithMultipleEvents(t *testing.T) {
	cwData := events.CloudwatchLogsData{
		MessageType:         "DATA_MESSAGE",
		LogGroup:            "copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L",
		Owner:               "263868185958",
		LogStream:           "copy-cwl-lambda-invoke-input-1510254365531-LogStream1-18OE12E2F8CE7",
		SubscriptionFilters: []string{"copy-cwl-lambda-invoke-input-1510254365531-SubscriptionFilter-L6TMMNMRQCU8"},
		LogEvents: []events.CloudwatchLogsLogEvent{
			{
				ID:        "33679800144697422554415395414062324420037086717597843456",
				Message:   "8499846a-88c1-4fb3-bfca-117ec023c5c3",
				Timestamp: 1510254471089,
			},
			{
				ID:        "33679800144719723299613926037203860138309735079103823873",
				Message:   "5615152f-ae3e-4163-bbd0-c26a241e4ca1",
				Timestamp: 1510254471090,
			}, {
				ID:        "33679800144742024044812456660345395856582383440609804290",
				Message:   "bfacbeeb-e5ab-4bdd-b6fc-4f0bebd4fa09",
				Timestamp: 1510254471091,
			},
		},
	}

	logData := []logs.CloudwatchLog{
		{
			Owner:     "263868185958",
			ID:        "33679800144697422554415395414062324420037086717597843456",
			LogGroup:  "copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L",
			Message:   "8499846a-88c1-4fb3-bfca-117ec023c5c3",
			Timestamp: 1510254471089,
		},
		{
			Owner:     "263868185958",
			ID:        "33679800144719723299613926037203860138309735079103823873",
			LogGroup:  "copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L",
			Message:   "5615152f-ae3e-4163-bbd0-c26a241e4ca1",
			Timestamp: 1510254471090,
		},
		{
			Owner:     "263868185958",
			ID:        "33679800144742024044812456660345395856582383440609804290",
			LogGroup:  "copy-cwl-lambda-invoke-input-1510254365531-LogGroup-17Q1WBYO69X0L",
			Message:   "bfacbeeb-e5ab-4bdd-b6fc-4f0bebd4fa09",
			Timestamp: 1510254471091,
		},
	}

	var expectedPayload []byte
	for i := range logData {
		payload, _ := json.Marshal(&logData[i])
		expectedPayload = append(expectedPayload, payload...)
		expectedPayload = append(expectedPayload, "\n"...)
	}

	expected := events.KinesisFirehoseResponseRecord{
		RecordID: "test004",
		Result:   events.KinesisFirehoseTransformedStateOk,
		Data:     expectedPayload,
	}

	actual := logs.GetFirehoseResponse("test004", cwData)
	assert.Equal(t, expected, actual)
}
