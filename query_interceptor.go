package main

import (
	"database/sql"
	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.query"
	gorest2.RegisterDataInterceptor(tableId, &QueryInterceptor{Id: tableId})
}

type QueryInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *QueryInterceptor) commonAfterCreateOrUpdateQuery(context map[string]interface{}) {
	queryName := context["old_data"].(map[string]string)["NAME"]
	appId := context["old_data"].(map[string]string)["PROJECT_ID"]
	dbo := gorest2.GetDbo(appId).(*NdDataOperator)
	delete(dbo.QueryRegistry, queryName)
}

func (this *QueryInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *QueryInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	this.commonAfterCreateOrUpdateQuery(context)
	return nil
}

func (this *QueryInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *QueryInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	this.commonAfterCreateOrUpdateQuery(context)
	return nil
}
