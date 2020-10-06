package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	// "reflect"
	"strings"
	"math"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
)


type transformedLogs struct {
	data     [][]byte
	result   string
	recordID string
}

type transformedLogsFinal struct {
	data     string
	result   string
	recordID string
	ags      string
}

type EventDictResult struct {
	Timename       float64 `json:"time"`
	Sourcetypename string `json:"sourcetype"`
	Sourcename     string `json:"source"`
	Indexname      string `json:"index"`
	Hostname       string `json:"host"`
	Fieldsname     *EventDictFieldsResult `json:"fields"`
	Eventname      string `json:"event"`


}

type EventDictFieldsResult struct {
	Regionname             string `json:"region"`
	Accountname            string `json:"account"`
	LogGroupname           string `json:"logGroup"`
	LogStreamname          string `json:"logStream"`
	FunctionNamename       string `json:"functionName"`
	SubscriptionFiltername string `json:"subscriptionFilter"`
	SpkTransformRIDname    string `json:"spk_transform_rID"`
}


// Don't touch
type logEvent struct {
	ID        string  `json:"id"`
	Timestamp float64 `json:"timestamp"`
	Message   string  `json:"message"`
}

// Don't touch
type logEvents struct {
	MessageType         string     `json:"MessageType"`
	Owner               string     `json:"Owner"`
	LogGroup            string     `json:"logGroup"`
	LogStream           string     `json:"logStream"`
	SubscriptionFilters []string   `json:"subscriptionFilters"`
	LogEvents           []logEvent `json:"logEvents"`
	RecordID            int        `json:"recordId"`
}

type gzreadCloser struct {
	*gzip.Reader
	io.Closer
}

func (gz gzreadCloser) Close() error {
	return gz.Closer.Close()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func processRecords(records []events.KinesisFirehoseEventRecord, region *string, account *string, requestID *string) events.KinesisFirehoseResponse {
	logCount := make(map[string]int)
	var responseP events.KinesisFirehoseResponse
	for _, r := range records {
		var transformedRecordP events.KinesisFirehoseResponseRecord
		logs := new(logEvents)
		var tmpData transformedLogs
		tmpData.recordID = r.RecordID
		rawdata := r.Data
		str := base64.StdEncoding.EncodeToString(rawdata)
		decodedData, err := base64.StdEncoding.DecodeString(str)

		check(err)

		reader, err := gzip.NewReader(bytes.NewReader(decodedData))
		decompData := gzreadCloser{reader, reader}
		if err := json.NewDecoder(decompData).Decode(&logs); err != nil {

		}
		check(err)
		logCountKey := logs.LogGroup + "::" + logs.LogStream
		_, ok := logCount[logCountKey]
		if ok {
			logCount[logCountKey]++
		} else {
			logCount[logCountKey] = 1
		}

		for _, event := range logs.LogEvents {
			payload := transformLogEvent(&event, &logs.LogGroup, &logs.LogStream, &logs.SubscriptionFilters[0], *region, *account, *requestID)

			if len(payload) > 0 {
				tmpData.data = append(tmpData.data, payload)
				tmpData.result = "Ok"
			}
		}

		fmt.Printf("Number of logs processed in %s: %d\n", r.RecordID, len(tmpData.data))
		joined := bytes.Join(tmpData.data, []byte(""))
		if len(tmpData.data) > 0 {
			fmt.Println("joined len: ", len(joined))
			// encoded := base64.StdEncoding.EncodeToString(joined)
			transformedRecordP.RecordID = tmpData.recordID
	    	transformedRecordP.Result = events.KinesisFirehoseTransformedStateOk
			transformedRecordP.Data = joined
			responseP.Records = append(responseP.Records, transformedRecordP)
		}

	}
	return responseP
}

func roundTo(n float64, decimals uint32) float64 {
  return math.Round(n*math.Pow(10, float64(decimals))) / math.Pow(10, float64(decimals))
}

func transformLogEvent(logEvent *logEvent, logGroup *string, logStream *string, subscriptionFilter *string, region string, account string, requestID string) []byte {
	timeValue := float64(int(logEvent.Timestamp*1000)) / 1000.000
	timeValue = roundTo(float64(timeValue)/1000, 3)
	logGroupSlice := strings.Split(*logGroup, "/")
	functionName := logGroupSlice[len(logGroupSlice)-1]
	ags := "UNK"
	found := strings.Contains(functionName, "-")
	if found == true {
		ags = strings.Split(functionName, "-")[0]
	}
	feildsdict := &EventDictFieldsResult{Regionname: strings.ToLower(region),Accountname: account,LogGroupname: *logGroup,LogStreamname: *logStream,FunctionNamename: functionName,SubscriptionFiltername: *subscriptionFilter,SpkTransformRIDname: requestID}
	eventresultsdict := &EventDictResult{Timename: timeValue,Sourcetypename: strings.ToLower(ags),Sourcename: "aws:lambda:go",Indexname: "application",Hostname: account,Fieldsname: feildsdict,Eventname: strings.TrimSpace(logEvent.Message)}
	e, err := json.Marshal(eventresultsdict )
	check(err)
	return e
}

func handleRequest(ctx context.Context, evnt events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	lc, _ := lambdacontext.FromContext(ctx)
	requestID := lc.AwsRequestID
	streamARN := evnt.DeliveryStreamArn
	region := strings.Split(streamARN, ":")[3]
	account := strings.Split(streamARN, ":")[4]
	processedResponse := processRecords(evnt.Records, &region, &account, &requestID)
	return processedResponse, nil
}

func main() {
	lambda.Start(handleRequest)
}
