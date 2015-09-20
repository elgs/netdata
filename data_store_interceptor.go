package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"strings"
)

func init() {
	tableId := "netdata.data_store"
	gorest2.RegisterDataInterceptor(tableId, &DataStoreInterceptor{Id: tableId})
}

type DataStoreInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func removeDataStorePassword(data map[string]interface{}) {
	if strings.TrimSpace(data["PASSWORD"].(string)) == "" {
		delete(data, "PASSWORD")
	}
}

func (this *DataStoreInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	removeDataStorePassword(data)
	return true, nil
}
func (this *DataStoreInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	removeDataStorePassword(data)
	context["DATA_STORE_NAME"] = data["DATA_STORE_NAME"]
	delete(data, "DATA_STORE_NAME")
	return true, nil
}

func (this *DataStoreInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	query := `SELECT ID FROM project WHERE DATA_STORE_NAME=?`
	projects, err := gosqljson.QueryDbToMap(db, "", query, context["DATA_STORE_NAME"])
	if err != nil {
		fmt.Println(err)
		return nil
	}
	for _, project := range projects {
		delete(gorest2.DboRegistry, project["ID"])
	}
	return nil
}

func filterDataStore(context map[string]interface{}, filter *string) (bool, error) {
	userToken := context["user_token"]
	if v, ok := userToken.(map[string]string); ok {
		userId := v["ID"]
		gorest2.MysqlSafe(&userId)
		*filter += fmt.Sprint(` AND (CREATOR_ID='`, userId, `' OR TYPE='public') `)
		return true, nil
	} else {
		return false, errors.New("Invalid user token.")
	}
}

func (this *DataStoreInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return filterDataStore(context, filter)
}
func (this *DataStoreInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return false, nil
}

func (this *DataStoreInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data []map[string]string, total int64) error {
	for _, dataStore := range data {
		if dataStore["password"] != "" {
			dataStore["password"] = ""
		}
		if dataStore["PASSWORD"] != "" {
			dataStore["PASSWORD"] = ""
		}
	}
	return nil
}
