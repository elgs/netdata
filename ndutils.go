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

	"github.com/elgs/gojq"
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

func batchExecuteTx(tx *sql.Tx, db *sql.DB, script *string, scriptParams []string, params [][]interface{}, replaceContext map[string]string) ([][]int64, error) {

	rowsAffectedArray := [][]int64{}

	innerTrans := false
	if tx == nil {
		var err error
		tx, err = db.Begin()
		innerTrans = true
		if err != nil {
			return rowsAffectedArray, err
		}
	}

	for i, v := range scriptParams {
		*script = strings.Replace(*script, fmt.Sprint("$", i), v, -1)
	}

	for k, v := range replaceContext {
		*script = strings.Replace(*script, k, v, -1)
	}

	scriptsArray, err := gosplitargs.SplitArgs(*script, ";", true)
	if err != nil {
		if innerTrans {
			tx.Rollback()
		}
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
				if innerTrans {
					tx.Rollback()
				}
				return nil, err
			}
			if len(params1) < totalCount+count {
				if innerTrans {
					tx.Rollback()
				}
				return nil, errors.New(fmt.Sprintln("Incorrect param count. Expected: ", totalCount+count, " actual: ", len(params1)))
			}
			rowsAffected, err := gosqljson.ExecTx(tx, s, params1[totalCount:totalCount+count]...)
			if err != nil {
				if innerTrans {
					tx.Rollback()
				}
				return nil, err
			}
			rowsAffectedArray1 = append(rowsAffectedArray1, rowsAffected)
			totalCount += count
		}
		rowsAffectedArray = append(rowsAffectedArray, rowsAffectedArray1)
	}

	if innerTrans {
		tx.Commit()
	}

	return rowsAffectedArray, nil
}

func buildReplaceContext(context map[string]interface{}) map[string]string {
	replaceContext := map[string]string{}
	if clientIp, ok := context["client_ip"].(string); ok {
		replaceContext["__ip__"] = clientIp
	}
	if loginUserId, ok := context["user_id"].(string); ok {
		replaceContext["__login_user_id__"] = loginUserId
	}
	if loginUserCode, ok := context["email"].(string); ok {
		replaceContext["__login_user_code__"] = loginUserCode
	}
	if tokenUserId, ok := context["token_user_id"].(string); ok {
		replaceContext["__token_user_id__"] = tokenUserId
	}
	if tokenUserCode, ok := context["token_user_code"].(string); ok {
		replaceContext["__token_user_code__"] = tokenUserCode
	}
	return replaceContext
}

func buildParams(clientData string) ([]string, [][]interface{}, error) {
	// assume the clientData is a json object with two arrays: query_params and params
	parser, err := gojq.NewStringQuery(clientData)
	if err != nil {
		return nil, nil, err
	}
	qp, err := parser.QueryToArray("query_params")
	if err != nil {
		return nil, nil, err
	}
	queryParams, err := convertInterfaceArrayToStringArray(qp)
	if err != nil {
		return nil, nil, err
	}
	p, err := parser.Query("params")
	if p1, ok := p.([]interface{}); ok {
		params := [][]interface{}{}
		for _, p2 := range p1 {
			if param, ok := p2.([]interface{}); ok {
				params = append(params, param)
			} else {
				return nil, nil, errors.New("Failed to build params.")
			}
		}
		return queryParams, params, nil
	}
	return nil, nil, errors.New("Failed to build.")
}

func convertInterfaceArrayToStringArray(arrayOfInterfaces []interface{}) ([]string, error) {
	ret := []string{}
	for _, v := range arrayOfInterfaces {
		if s, ok := v.(string); ok {
			ret = append(ret, s)
		} else {
			return nil, errors.New("Failed to convert.")
		}
	}
	return ret, nil
}
