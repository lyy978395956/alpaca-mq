/*
* @Author: leiyuya
* @Date:   2020-07-31 11:27:13
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-07-31 11:27:31
 */

package alpaca

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
)

type HttpRequest struct {
	logger *Logger
}

func NewHttpRequest(klogger *Logger) *HttpRequest {
	return &HttpRequest{
		logger: klogger,
	}
}
func (h *HttpRequest) Post(url string, data string, LogId string) error {

	bytedate := []byte(data)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bytedate))

	if err != nil {
		h.logger.WithFields(Fields{"method": "POST", "url": url, "data": data, "logId": LogId}).Warnf("Init Request Failed Err:%s", err)
		return errors.New("Request Error")
	}

	client := &http.Client{}

	resp, err := client.Do(req)

	defer resp.Body.Close()

	if err != nil {
		h.logger.WithFields(Fields{"method": "POST", "url": url, "data": data, "logId": LogId}).Warn("Request Error")
		return errors.New("Request Error")
	}

	statuscode := resp.StatusCode

	if statuscode != 200 {
		h.logger.WithFields(Fields{"method": "POST", "url": url, "data": data, "http_status": statuscode, "logId": LogId}).Warn("Request Http_status Not 200")
		return errors.New("Request Error")
	}

	respBody, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		h.logger.WithFields(Fields{"method": "POST", "url": url, "data": data, "http_status": statuscode, "logId": LogId}).Warnf("Read Response Data Failed Err:%s", readErr)
		return errors.New("Request Error")
	}
	var response map[string]interface{}

	err1 := json.Unmarshal(respBody, &response)

	if err1 != nil {
		h.logger.WithFields(Fields{"method": "POST", "url": url, "data": data, "http_status": statuscode, "response": string(respBody), "logId": LogId}).Warnf("Response Decode Failed Err:%s", err1)
		return err1
	}

	errno, _ := response["errno"].(int)

	if errno != 0 {
		h.logger.WithFields(Fields{"method": "POST", "url": url, "data": data, "http_status": statuscode, "response": string(respBody), "logId": LogId}).Warn("Request Error Errno Not 0")
		return errors.New("Request failed!")
	}

	h.logger.WithFields(Fields{"method": "POST", "url": url, "data": data, "http_status": statuscode, "response": string(respBody), "logId": LogId}).Info("Request Success")
	return nil
}
