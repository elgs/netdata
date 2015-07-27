// nd_data_operator
package main

import (
	"database/sql"
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

	ret := make([]map[string]string, 0)
	db, err := this.GetConn()

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeQueryMap(tableId, params, db, context)
		if !ctn {
			return ret, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeQueryMap(tableId, params, db, context)
		if !ctn {
			return ret, err
		}
	}

	c := context["case"].(string)
	m, err := gosqljson.QueryDbToMap(db, c, query["SCRIPT"], params...)
	if err != nil {
		fmt.Println(err)
		return ret, err
	}

	if dataInterceptor != nil {
		dataInterceptor.AfterQueryMap(tableId, params, db, context, m)
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		globalDataInterceptor.AfterQueryMap(tableId, params, db, context, m)
	}

	return m, err
}
func (this *NdDataOperator) QueryArray(tableId string, params []interface{}, context map[string]interface{}) ([]string, [][]string, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return nil, nil, err
	}

	db, err := this.GetConn()

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeQueryArray(tableId, params, db, context)
		if !ctn {
			return nil, nil, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeQueryArray(tableId, params, db, context)
		if !ctn {
			return nil, nil, err
		}
	}

	c := context["case"].(string)
	h, a, err := gosqljson.QueryDbToArray(db, c, query["SCRIPT"], params...)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}

	if dataInterceptor != nil {
		dataInterceptor.AfterQueryArray(tableId, params, db, context, h, a)
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		globalDataInterceptor.AfterQueryArray(tableId, params, db, context, h, a)
	}

	return h, a, err
}
func (this *NdDataOperator) Exec(tableId string, params []interface{}, context map[string]interface{}) (int64, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return -1, err
	}
	db, err := this.GetConn()

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeExec(tableId, params, db, context)
		if !ctn {
			if tx, ok := context["tx"].(*sql.Tx); ok {
				tx.Rollback()
			}
			return 0, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeExec(tableId, params, db, context)
		if !ctn {
			if tx, ok := context["tx"].(*sql.Tx); ok {
				tx.Rollback()
			}
			return 0, err
		}
	}
	var rowsAffected int64
	if tx, ok := context["tx"].(*sql.Tx); ok {
		rowsAffected, err = gosqljson.ExecTx(tx, query["SCRIPT"], params...)
		if err != nil {
			fmt.Println(err)
			tx.Rollback()
			return -1, err
		}
	} else {
		rowsAffected, err = gosqljson.ExecDb(db, query["SCRIPT"], params...)
		if err != nil {
			fmt.Println(err)
			return -1, err
		}
	}

	if dataInterceptor != nil {
		err := dataInterceptor.AfterExec(tableId, params, db, context)
		if err != nil {
			if tx, ok := context["tx"].(*sql.Tx); ok {
				tx.Rollback()
			}
			return -1, err
		}
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		err := globalDataInterceptor.AfterExec(tableId, params, db, context)
		if err != nil {
			if tx, ok := context["tx"].(*sql.Tx); ok {
				tx.Rollback()
			}
			return -1, err
		}
	}

	if tx, ok := context["tx"].(*sql.Tx); ok {
		tx.Commit()
	}

	return rowsAffected, err
}
