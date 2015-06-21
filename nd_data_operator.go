// nd_data_operator
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"strconv"
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

func (this *NdDataOperator) QueryMap(tableId string, start int64, limit int64, includeTotal bool, context map[string]interface{}) ([]map[string]string, int64, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return nil, -1, err
	}

	ret := make([]map[string]string, 0)
	db, err := this.GetConn()

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeQueryMap(tableId, db, context, start, limit, includeTotal)
		if !ctn {
			return ret, -1, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeQueryMap(tableId, db, context, start, limit, includeTotal)
		if !ctn {
			return ret, -1, err
		}
	}

	c := context["case"].(string)
	m, err := gosqljson.QueryDbToMap(db, c, query["SCRIPT"], start, limit)
	if err != nil {
		fmt.Println(err)
		return ret, -1, err
	}
	cnt := -1
	if includeTotal {
		c, err := gosqljson.QueryDbToMap(db, "upper",
			fmt.Sprint("SELECT FOUND_ROWS()"))
		if err != nil {
			fmt.Println(err)
			return ret, -1, err
		}
		cnt, err = strconv.Atoi(c[0]["FOUND_ROWS()"])
		if err != nil {
			fmt.Println(err)
			return ret, -1, err
		}
	}

	if dataInterceptor != nil {
		dataInterceptor.AfterQueryMap(tableId, db, context, m, int64(cnt))
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		globalDataInterceptor.AfterQueryMap(tableId, db, context, m, int64(cnt))
	}

	return m, int64(cnt), err
}
func (this *NdDataOperator) QueryArray(tableId string, start int64, limit int64, includeTotal bool, context map[string]interface{}) ([]string, [][]string, int64, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return nil, nil, -1, err
	}

	db, err := this.GetConn()

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeQueryArray(tableId, db, context, start, limit, includeTotal)
		if !ctn {
			return nil, nil, -1, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeQueryArray(tableId, db, context, start, limit, includeTotal)
		if !ctn {
			return nil, nil, -1, err
		}
	}

	c := context["case"].(string)
	h, a, err := gosqljson.QueryDbToArray(db, c, query["SCRIPT"], start, limit)
	if err != nil {
		fmt.Println(err)
		return nil, nil, -1, err
	}
	cnt := -1
	if includeTotal {
		c, err := gosqljson.QueryDbToMap(db, "upper",
			fmt.Sprint("SELECT FOUND_ROWS()"))
		if err != nil {
			fmt.Println(err)
			return nil, nil, -1, err
		}
		cnt, err = strconv.Atoi(c[0]["FOUND_ROWS()"])
		if err != nil {
			fmt.Println(err)
			return nil, nil, -1, err
		}
	}

	if dataInterceptor != nil {
		dataInterceptor.AfterQueryArray(tableId, db, context, h, a, int64(cnt))
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		globalDataInterceptor.AfterQueryArray(tableId, db, context, h, a, int64(cnt))
	}

	return h, a, int64(cnt), err
}
func (this *NdDataOperator) Exec(tableId string, context map[string]interface{}) (int64, error) {
	projectId := context["app_id"].(string)
	query, err := this.loadQuery(projectId, tableId)
	if err != nil {
		return -1, err
	}
	db, err := this.GetConn()

	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		ctn, err := globalDataInterceptor.BeforeExec(tableId, db, context)
		if !ctn {
			if tx, ok := context["tx"].(*sql.Tx); ok {
				tx.Rollback()
			}
			return 0, err
		}
	}
	dataInterceptor := gorest2.GetDataInterceptor(tableId)
	if dataInterceptor != nil {
		ctn, err := dataInterceptor.BeforeExec(tableId, db, context)
		if !ctn {
			if tx, ok := context["tx"].(*sql.Tx); ok {
				tx.Rollback()
			}
			return 0, err
		}
	}
	var rowsAffected int64
	if tx, ok := context["tx"].(*sql.Tx); ok {
		rowsAffected, err = gosqljson.ExecTx(tx, query["SCRIPT"])
		if err != nil {
			fmt.Println(err)
			tx.Rollback()
			return -1, err
		}
	} else {
		rowsAffected, err = gosqljson.ExecDb(db, query["SCRIPT"])
		if err != nil {
			fmt.Println(err)
			return -1, err
		}
	}

	if dataInterceptor != nil {
		err := dataInterceptor.AfterExec(tableId, db, context)
		if err != nil {
			if tx, ok := context["tx"].(*sql.Tx); ok {
				tx.Rollback()
			}
			return -1, err
		}
	}
	for _, globalDataInterceptor := range gorest2.GlobalDataInterceptorRegistry {
		err := globalDataInterceptor.AfterExec(tableId, db, context)
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
