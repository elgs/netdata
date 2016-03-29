// nd_data_operator
package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/elgs/gorest2"
	"github.com/elgs/gosplitargs"
	"github.com/elgs/gosqljson"
)

type NdDataOperator struct {
	*gorest2.MySqlDataOperator
}

func NewDbo(ds, dbType string) gorest2.DataOperator {
	return &NdDataOperator{
		MySqlDataOperator: &gorest2.MySqlDataOperator{
			Ds:     ds,
			DbType: dbType,
		},
		//		QueryRegistry: make(map[string]map[string]string),
	}
}

func (this *NdDataOperator) loadQuery(projectId, queryName string) (map[string]string, error) {
	key := fmt.Sprint("query:", projectId, ":", queryName)
	queryMap := gorest2.RedisLocal.HGetAllMap(key).Val()
	if len(queryMap) > 0 {
		return queryMap, nil
	}

	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		return nil, err
	}
	queryData, err := gosqljson.QueryDbToMap(defaultDb, "upper",
		"SELECT * FROM query WHERE PROJECT_ID=? AND NAME=?", projectId, queryName)
	if err != nil {
		return nil, err
	}
	if len(queryData) == 0 {
		return nil, errors.New("Query not found.")
	}

	err = gorest2.RedisMaster.HMSet(key, "name", queryData[0]["NAME"], "script", queryData[0]["SCRIPT"]).Err()
	return queryData[0], nil
}

func (this *NdDataOperator) QueryMap(tableId string, params []interface{}, queryParams []string, context map[string]interface{}) ([]map[string]string, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return nil, err
	}

	ret := make([]map[string]string, 0)

	script := query["script"]

	count, err := gosplitargs.CountSeparators(script, "\\?")
	if err != nil {
		return ret, err
	}
	if count > len(params) {
		return nil, errors.New(fmt.Sprintln("Incorrect param count. Expected: ", count, " actual: ", len(params)))
	}

	for i, v := range queryParams {
		script = strings.Replace(script, fmt.Sprint("$", i), v, -1)
	}

	db, err := this.GetConn()
	if err != nil {
		return ret, err
	}

	globalDataInterceptors, globalSortedKeys := gorest2.GetGlobalDataInterceptors()
	for _, k := range globalSortedKeys {
		globalDataInterceptor := globalDataInterceptors[k]
		ctn, err := globalDataInterceptor.BeforeQueryMap(tableId, script, &params, db, context)
		if !ctn {
			return ret, err
		}
	}
	dataInterceptors, sortedKeys := gorest2.GetDataInterceptors(tableId)
	for _, k := range sortedKeys {
		dataInterceptor := dataInterceptors[k]
		if dataInterceptor != nil {
			ctn, err := dataInterceptor.BeforeQueryMap(tableId, script, &params, db, context)
			if !ctn {
				return ret, err
			}
		}
	}

	if clientIp, ok := context["client_ip"].(string); ok {
		script = strings.Replace(script, "__ip__", clientIp, -1)
	}
	if tokenUserId, ok := context["token_user_id"].(string); ok {
		script = strings.Replace(script, "__token_user_id__", tokenUserId, -1)
	}
	if tokenUserCode, ok := context["token_user_code"].(string); ok {
		script = strings.Replace(script, "__token_user_code__", tokenUserCode, -1)
	}
	if loginUserId, ok := context["user_id"].(string); ok {
		script = strings.Replace(script, "__login_user_id__", loginUserId, -1)
	}
	if loginUserCode, ok := context["email"].(string); ok {
		script = strings.Replace(script, "__login_user_code__", loginUserCode, -1)
	}

	c := context["case"].(string)
	m, err := gosqljson.QueryDbToMap(db, c, script, params[:count]...)
	if err != nil {
		fmt.Println(err)
		return ret, err
	}

	for _, k := range sortedKeys {
		dataInterceptor := dataInterceptors[k]
		if dataInterceptor != nil {
			dataInterceptor.AfterQueryMap(tableId, script, &params, db, context, &m)
		}
	}
	for _, k := range globalSortedKeys {
		globalDataInterceptor := globalDataInterceptors[k]
		globalDataInterceptor.AfterQueryMap(tableId, script, &params, db, context, &m)
	}

	return m, err
}
func (this *NdDataOperator) QueryArray(tableId string, params []interface{}, queryParams []string, context map[string]interface{}) ([]string, [][]string, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return nil, nil, err
	}

	script := query["script"]
	count, err := gosplitargs.CountSeparators(script, "\\?")
	if err != nil {
		return nil, nil, err
	}
	if count > len(params) {
		return nil, nil, errors.New(fmt.Sprintln("Incorrect param count. Expected: ", count, " actual: ", len(params)))
	}

	for i, v := range queryParams {
		script = strings.Replace(script, fmt.Sprint("$", i), v, -1)
	}

	db, err := this.GetConn()
	if err != nil {
		return nil, nil, err
	}

	globalDataInterceptors, globalSortedKeys := gorest2.GetGlobalDataInterceptors()
	for _, k := range globalSortedKeys {
		globalDataInterceptor := globalDataInterceptors[k]
		ctn, err := globalDataInterceptor.BeforeQueryArray(tableId, script, &params, db, context)
		if !ctn {
			return nil, nil, err
		}
	}
	dataInterceptors, sortedKeys := gorest2.GetDataInterceptors(tableId)
	for _, k := range sortedKeys {
		dataInterceptor := dataInterceptors[k]
		if dataInterceptor != nil {
			ctn, err := dataInterceptor.BeforeQueryArray(tableId, script, &params, db, context)
			if !ctn {
				return nil, nil, err
			}
		}
	}

	if clientIp, ok := context["client_ip"].(string); ok {
		script = strings.Replace(script, "__ip__", clientIp, -1)
	}
	if tokenUserId, ok := context["token_user_id"].(string); ok {
		script = strings.Replace(script, "__token_user_id__", tokenUserId, -1)
	}
	if tokenUserCode, ok := context["token_user_code"].(string); ok {
		script = strings.Replace(script, "__token_user_code__", tokenUserCode, -1)
	}
	if loginUserId, ok := context["user_id"].(string); ok {
		script = strings.Replace(script, "__login_user_id__", loginUserId, -1)
	}
	if loginUserCode, ok := context["email"].(string); ok {
		script = strings.Replace(script, "__login_user_code__", loginUserCode, -1)
	}

	c := context["case"].(string)
	h, a, err := gosqljson.QueryDbToArray(db, c, script, params[:count]...)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}

	for _, k := range sortedKeys {
		dataInterceptor := dataInterceptors[k]
		if dataInterceptor != nil {
			dataInterceptor.AfterQueryArray(tableId, script, &params, db, context, &h, &a)
		}
	}
	for _, k := range globalSortedKeys {
		globalDataInterceptor := globalDataInterceptors[k]
		globalDataInterceptor.AfterQueryArray(tableId, script, &params, db, context, &h, &a)
	}

	return h, a, err
}
func (this *NdDataOperator) Exec(tableId string, params [][]interface{}, queryParams []string, context map[string]interface{}) ([][]int64, error) {
	rowsAffectedArray := [][]int64{}
	projectId := context["app_id"].(string)

	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return rowsAffectedArray, err
	}
	scripts := query["script"]
	for i, v := range queryParams {
		scripts = strings.Replace(scripts, fmt.Sprint("$", i), v, -1)
	}

	db, err := this.GetConn()
	if err != nil {
		return rowsAffectedArray, err
	}
	tx, err := db.Begin()
	if err != nil {
		return rowsAffectedArray, err
	}
	globalDataInterceptors, globalSortedKeys := gorest2.GetGlobalDataInterceptors()
	for _, k := range globalSortedKeys {
		globalDataInterceptor := globalDataInterceptors[k]
		ctn, err := globalDataInterceptor.BeforeExec(tableId, scripts, &params, tx, context)
		if !ctn {
			tx.Rollback()
			return rowsAffectedArray, err
		}
	}
	dataInterceptors, sortedKeys := gorest2.GetDataInterceptors(tableId)
	for _, k := range sortedKeys {
		dataInterceptor := dataInterceptors[k]
		if dataInterceptor != nil {
			ctn, err := dataInterceptor.BeforeExec(tableId, scripts, &params, tx, context)
			if !ctn {
				tx.Rollback()
				return rowsAffectedArray, err
			}
		}
	}
	if clientIp, ok := context["client_ip"].(string); ok {
		scripts = strings.Replace(scripts, "__ip__", clientIp, -1)
	}
	if tokenUserId, ok := context["token_user_id"].(string); ok {
		scripts = strings.Replace(scripts, "__token_user_id__", tokenUserId, -1)
	}
	if tokenUserCode, ok := context["token_user_code"].(string); ok {
		scripts = strings.Replace(scripts, "__token_user_code__", tokenUserCode, -1)
	}
	if loginUserId, ok := context["user_id"].(string); ok {
		scripts = strings.Replace(scripts, "__login_user_id__", loginUserId, -1)
	}
	if loginUserCode, ok := context["email"].(string); ok {
		scripts = strings.Replace(scripts, "__login_user_code__", loginUserCode, -1)
	}
	totalCount := 0
	scriptsArray, err := gosplitargs.SplitArgs(scripts, ";", true)
	if err != nil {
		return rowsAffectedArray, err
	}
	for _, params1 := range params {
		rowsAffectedArray1 := []int64{}
		for _, s := range scriptsArray {
			sqlNormalize(&s)
			if len(s) == 0 {
				continue
			}
			count, err := gosplitargs.CountSeparators(s, "\\?")
			if err != nil {
				tx.Rollback()
				return rowsAffectedArray, err
			}
			if len(params1) < totalCount+count {
				tx.Rollback()
				return nil, errors.New(fmt.Sprintln("Incorrect param count. Expected: ", totalCount+count, " actual: ", len(params1)))
			}
			rowsAffected, err := gosqljson.ExecTx(tx, s, params1[totalCount:totalCount+count]...)
			if err != nil {
				tx.Rollback()
				return rowsAffectedArray, err
			}
			rowsAffectedArray1 = append(rowsAffectedArray1, rowsAffected)
			totalCount += count
		}
		rowsAffectedArray = append(rowsAffectedArray, rowsAffectedArray1)
	}

	for _, k := range sortedKeys {
		dataInterceptor := dataInterceptors[k]
		if dataInterceptor != nil {
			err := dataInterceptor.AfterExec(tableId, scripts, &params, tx, context, rowsAffectedArray)
			if err != nil {
				tx.Rollback()
				return rowsAffectedArray, err
			}
		}
	}
	for _, k := range globalSortedKeys {
		globalDataInterceptor := globalDataInterceptors[k]
		err := globalDataInterceptor.AfterExec(tableId, scripts, &params, tx, context, rowsAffectedArray)
		if err != nil {
			tx.Rollback()
			return rowsAffectedArray, err
		}
	}

	tx.Commit()

	return rowsAffectedArray, err
}
