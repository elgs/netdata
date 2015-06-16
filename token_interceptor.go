package main

import (
	"database/sql"
	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.token"
	gorest2.RegisterDataInterceptor(tableId, &TokenInterceptor{Id: tableId})
}

type TokenInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func commonAfterCreateOrUpdate(token string) {
	delete(projectTokenRegistry, token)
}

func (this *TokenInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *TokenInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	commonAfterCreateOrUpdate(context["old_data"].(map[string]string)["TOKEN"])
	return nil
}

func (this *TokenInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *TokenInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	commonAfterCreateOrUpdate(context["old_data"].(map[string]string)["TOKEN"])
	return nil
}
