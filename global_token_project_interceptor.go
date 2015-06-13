package main

import (
	"database/sql"
	"github.com/elgs/gorest2"
	"time"
)

func init() {
	gorest2.RegisterGlobalDataInterceptor(&GlobalTokenProjectInterceptor{Id: "GlobalTokenProjectInterceptor"})
}

type GlobalTokenProjectInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func isDefaultProjectRequest(context map[string]interface{}) bool {
	return len(context["project_id"].(string)) != 36
}

func (this *GlobalTokenProjectInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	ctn, err := checkToken(db, context["api_token_id"].(string), context["api_token_key"].(string), context, resourceId)
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			data["CREATE_TIME"] = time.Now()
			data["UPDATE_TIME"] = time.Now()
		}
	}
	return ctn, err
}
func (this *GlobalTokenProjectInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "load"); !ok {
		return false, err
	}
	return checkToken(db, context["api_token_id"].(string), context["api_token_key"].(string), context, resourceId)
}
func (this *GlobalTokenProjectInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	ctn, err := checkToken(db, context["api_token_id"].(string), context["api_token_key"].(string), context, resourceId)
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			data["UPDATE_TIME"] = time.Now()
		}
	}
	return ctn, err
}
func (this *GlobalTokenProjectInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkToken(db, context["api_token_id"].(string), context["api_token_key"].(string), context, resourceId)
}
func (this *GlobalTokenProjectInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkToken(db, context["api_token_id"].(string), context["api_token_key"].(string), context, resourceId)
}
func (this *GlobalTokenProjectInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64, includeTotal bool) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkToken(db, context["api_token_id"].(string), context["api_token_key"].(string), context, resourceId)
}
func (this *GlobalTokenProjectInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data []map[string]string, total int64) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64, includeTotal bool) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkToken(db, context["api_token_id"].(string), context["api_token_key"].(string), context, resourceId)
}
func (this *GlobalTokenProjectInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers []string, data [][]string, total int64) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
