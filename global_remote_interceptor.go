// global_remote_interceptor
package main

import (
	"database/sql"
	"fmt"
	"github.com/elgs/gorest2"
)

func init() {
	loadACL()
	gorest2.RegisterGlobalDataInterceptor(30, &GlobalRemoteInterceptor{Id: "GlobalRemoteInterceptor"})
	loadAllRemoteInterceptor()
}

var RemoteInterceptorRegistry = map[string]*RemoteInterceptorDefinition{}

type GlobalRemoteInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

type RemoteInterceptorDefinition struct {
	ProjectId  string
	Target     string // table name, query name
	Method     string // POST, GET
	Url        string
	Type       string // before, after
	ActionType string // create, load, update, delete, ...
}

func loadAllRemoteInterceptor() error {
	// load all remote interceptor definitions into RemoteInterceptorRegistry
	return nil
}

func loadRemoteInterceptor(projectId, target, theType, actionType string) error {
	// load specific remote interceptor definitions into RemoteInterceptorRegistry
	return nil
}

func unloadRemoteInterceptor(projectId, target, theType, actionType string) error {
	// unload specific remote interceptor definitions into RemoteInterceptorRegistry
	return nil
}

func (this *GlobalRemoteInterceptor) checkAgainstBeforeRemoteInterceptor(ri *RemoteInterceptorDefinition) (bool, error) {
	return true, nil
}

func (this *GlobalRemoteInterceptor) checkAgainstAfterRemoteInterceptor(ri *RemoteInterceptorDefinition) error {
	return nil
}

func (this *GlobalRemoteInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "create")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "create")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "load")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "load")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "update")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "update")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "duplicate")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "duplicate")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "delete")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "delete")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "list_map")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "list_map")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "list_array")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "list_array")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "query_map")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "query_map")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "query_array")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "query_array")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	key := fmt.Sprint(context["app_id"], resourceId, "before", "exec")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) error {
	key := fmt.Sprint(context["app_id"], resourceId, "after", "exec")
	ri := RemoteInterceptorRegistry[key]
	if ri == nil {
		return nil
	}
	return this.checkAgainstAfterRemoteInterceptor(ri)
}
