package main

import (
	"database/sql"
	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.job"
	gorest2.RegisterDataInterceptor(tableId, &JobInterceptor{Id: tableId})
}

type JobInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *JobInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *JobInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return nil
}

func (this *JobInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *JobInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	return nil
}
