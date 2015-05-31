package main

import (
	"database/sql"
	"github.com/elgs/gorest2"
)

func init() {
	gorest2.RegisterGlobalDataInterceptor(&GlobalDataInterceptor{Id: "GlobalDataInterceptor"})
}

type GlobalDataInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *GlobalDataInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	wsMsgQueue <- data
	return nil
}

func (this *GlobalDataInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	wsMsgQueue <- data
	return nil
}

func (this *GlobalDataInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	wsMsgQueue <- newId
	return nil
}
func (this *GlobalDataInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	wsMsgQueue <- id
	return nil
}
