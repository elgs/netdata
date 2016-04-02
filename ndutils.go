// ndutils
package main

import (
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/elgs/gosplitargs"
	"github.com/elgs/gosqljson"
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

func batchExecuteTx(tx *sql.Tx, script *string, scriptParams []string, params [][]interface{}, replaceContext map[string]string) ([][]int64, error) {

	rowsAffectedArray := [][]int64{}
	for i, v := range scriptParams {
		*script = strings.Replace(*script, fmt.Sprint("$", i), v, -1)
	}

	for k, v := range replaceContext {
		*script = strings.Replace(*script, k, v, -1)
	}

	scriptsArray, err := gosplitargs.SplitArgs(*script, ";", true)
	if err != nil {
		return rowsAffectedArray, err
	}
	for _, params1 := range params {
		totalCount := 0
		rowsAffectedArray1 := []int64{}
		for _, s := range scriptsArray {
			sqlNormalize(&s)
			if len(s) == 0 {
				continue
			}
			count, err := gosplitargs.CountSeparators(s, "\\?")
			if err != nil {
				return nil, err
			}
			if len(params1) < totalCount+count {
				return nil, errors.New(fmt.Sprintln("Incorrect param count. Expected: ", totalCount+count, " actual: ", len(params1)))
			}
			rowsAffected, err := gosqljson.ExecTx(tx, s, params1[totalCount:totalCount+count]...)
			if err != nil {
				return nil, err
			}
			rowsAffectedArray1 = append(rowsAffectedArray1, rowsAffected)
			totalCount += count
		}
		rowsAffectedArray = append(rowsAffectedArray, rowsAffectedArray1)
	}

	return rowsAffectedArray, nil
}
