// ndutils
package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func httpRequest(url string, method string, data string, maxReadLimit int64) ([]byte, int, error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(method, url, strings.NewReader(data))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	if maxReadLimit >= 0 {
		res.Body = &LimitedReadCloser{res.Body, maxReadLimit}
	}

	result, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	defer res.Body.Close()
	defer tr.CloseIdleConnections()

	return result, res.StatusCode, err
}
