// nd_data_operator
package main

import (
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
)

type NdDataOperator struct {
	*gorest2.MySqlDataOperator
	QueryRegistry map[string]map[string]string
}

func NewDbo(ds, dbType string) gorest2.DataOperator {
	return &NdDataOperator{
		MySqlDataOperator: &gorest2.MySqlDataOperator{
			Ds:     ds,
			DbType: dbType,
		},
		QueryRegistry: make(map[string]map[string]string),
	}
}

func (this *NdDataOperator) loadQuery(projectId, queryName string) (map[string]string, error) {
	query := this.QueryRegistry[queryName]
	if query != nil {
		return query, nil
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

	this.QueryRegistry[queryName] = queryData[0]
	return queryData[0], nil
}

func (this *NdDataOperator) QueryMap(tableId string, params []interface{}, context map[string]interface{}) ([]map[string]string, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return nil, err
	}

	script := query["SCRIPT"]

	ret := make([]map[string]string, 0)
	db, err := this.GetConn()
	if err != nil {
		return ret, err
	}

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeQueryMap(tableId, script, params, db, context)
		if !ctn {
			return ret, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeQueryMap(tableId, script, params, db, context)
		if !ctn {
			return ret, err
		}
	}

	c := context["case"].(string)
	m, err := gosqljson.QueryDbToMap(db, c, script, params...)
	if err != nil {
		fmt.Println(err)
		return ret, err
	}

	if dataInterceptor != nil {
		dataInterceptor.AfterQueryMap(tableId, script, params, db, context, m)
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		globalDataInterceptor.AfterQueryMap(tableId, script, params, db, context, m)
	}

	return m, err
}
func (this *NdDataOperator) QueryArray(tableId string, params []interface{}, context map[string]interface{}) ([]string, [][]string, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	script := query["SCRIPT"]
	if err != nil {
		return nil, nil, err
	}

	db, err := this.GetConn()
	if err != nil {
		return nil, nil, err
	}

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeQueryArray(tableId, script, params, db, context)
		if !ctn {
			return nil, nil, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeQueryArray(tableId, script, params, db, context)
		if !ctn {
			return nil, nil, err
		}
	}

	c := context["case"].(string)
	h, a, err := gosqljson.QueryDbToArray(db, c, script, params...)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}

	if dataInterceptor != nil {
		dataInterceptor.AfterQueryArray(tableId, script, params, db, context, h, a)
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		globalDataInterceptor.AfterQueryArray(tableId, script, params, db, context, h, a)
	}

	return h, a, err
}
func (this *NdDataOperator) Exec(tableId string, params []interface{}, context map[string]interface{}) (int64, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return -1, err
	}
	scripts := query["SCRIPT"]
	db, err := this.GetConn()
	if err != nil {
		return -1, err
	}
	tx, err := db.Begin()
	if err != nil {
		return -1, err
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeExec(tableId, scripts, params, tx, context)
		if !ctn {
			tx.Rollback()
			return 0, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeExec(tableId, scripts, params, tx, context)
		if !ctn {
			tx.Rollback()
			return 0, err
		}
	}
	var rowsAffected int64
	rowsAffected, err = gosqljson.ExecTx(tx, scripts, params...)
	if err != nil {
		fmt.Println(err)
		tx.Rollback()
		return -1, err
	}

	if dataInterceptor != nil {
		err := dataInterceptor.AfterExec(tableId, scripts, params, tx, context)
		if err != nil {
			tx.Rollback()
			return -1, err
		}
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		err := globalDataInterceptor.AfterExec(tableId, scripts, params, tx, context)
		if err != nil {
			tx.Rollback()
			return -1, err
		}
	}

	tx.Commit()

	return rowsAffected, err
}
