// global_remote_interceptor
package main

import (
	"database/sql"
	"github.com/elgs/gorest2"
)

func init() {
	loadACL()
	gorest2.RegisterGlobalDataInterceptor(30, &GlobalRemoteInterceptor{Id: "GlobalRemoteInterceptor"})
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

func (this *GlobalRemoteInterceptor) loadRemoteInterceptor(projectId, target, theType, actionType string) *RemoteInterceptorDefinition {
	return nil
}

func (this *GlobalRemoteInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "create")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"create"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "create")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"create"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "load")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"load"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "load")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"load"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "update")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"update"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "update")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"update"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "duplicate")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"duplicate"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "duplicate")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"duplicate"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "delete")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"delete"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "delete")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"delete"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "listmap")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"listmap"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "listmap")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"listmap"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "listarray")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"listarray"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "listarray")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"listarray"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "querymap")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"querymap"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "querymap")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"querymap"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "queryarray")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"queryarray"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "queryarray")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"queryarray"] = ri
	}
	return nil
}
func (this *GlobalRemoteInterceptor) BeforeExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "before", "exec")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"before"+"exec"] = ri
	}
	return true, nil
}
func (this *GlobalRemoteInterceptor) AfterExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) error {
	projectId := context["app_id"].(string)
	ri := this.loadRemoteInterceptor(projectId, resourceId, "after", "exec")
	if ri != nil {
		RemoteInterceptorRegistry[projectId+resourceId+"after"+"exec"] = ri
	}
	return nil
}
