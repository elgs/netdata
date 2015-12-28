// ndutils
package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
)

func httpRequest(url string, method string, data string) ([]byte, int, error) {
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
	buffer := make([]byte, len(data))
	_, err = res.Body.Read(buffer)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	defer res.Body.Close()
	defer tr.CloseIdleConnections()

	return buffer, res.StatusCode, err
}
