// global_remote_interceptor
package main

import (
	"database/sql"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
)

func init() {
	loadACL()
	gorest2.RegisterGlobalDataInterceptor(30, &GlobalRemoteInterceptor{Id: "GlobalRemoteInterceptor"})
}

type GlobalRemoteInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func loadAllRemoteInterceptor() error {
	pipe := redisMaster.Pipeline()
	defer pipe.Close()

	// load all remote interceptor definitions into RemoteInterceptorRegistry
	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	riData, err := gosqljson.QueryDbToMap(defaultDb, "upper", "SELECT * FROM remote_interceptor")
	if err != nil {
		return err
	}
	for _, riMap := range riData {
		projectId := riMap["PROJECT_ID"]
		target := riMap["TARGET"]
		theType := riMap["TYPE"]
		actionType := riMap["ACTION_TYPE"]
		method := riMap["METHOD"]
		url := riMap["URL"]
		key := fmt.Sprint("ri:", projectId, ":", target, ":", theType, ":", actionType)
		pipe.HMSet(key, "method", method, "url", url)
	}
	_, err = pipe.Exec()
	return err
}

func loadRemoteInterceptor(projectId, target, theType, actionType string) error {
	// load specific remote interceptor definitions into RemoteInterceptorRegistry
	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	riData, err := gosqljson.QueryDbToMap(defaultDb,
		"upper", "SELECT * FROM remote_interceptor WHERE PROJECT_ID=? AND TARGET=? AND TYPE=? AND ACTION_TYPE=?",
		projectId, target, theType, actionType)
	if err != nil {
		return err
	}
	if riData != nil && len(riData) == 1 {
		riMap := riData[0]
		projectId := riMap["PROJECT_ID"]
		target := riMap["TARGET"]
		method := riMap["METHOD"]
		url := riMap["URL"]
		theType := riMap["TYPE"]
		actionType := riMap["ACTION_TYPE"]
		key := fmt.Sprint("ri:", projectId, ":", target, ":", theType, ":", actionType)
		redisMaster.HMSet(key, "method", method, "url", url)
	}
	return nil
}

func unloadRemoteInterceptor(projectId, target, theType, actionType string) error {
	// unload specific remote interceptor definitions into RemoteInterceptorRegistry
	key := fmt.Sprint("ri:", projectId, ":", target, ":", theType, ":", actionType)
	err := redisMaster.HDel(key).Err()
	return err
}

func (this *GlobalRemoteInterceptor) checkAgainstBeforeRemoteInterceptor(ri map[string]string) (bool, error) {
	fmt.Println(ri)
	return true, nil
}

func (this *GlobalRemoteInterceptor) executeAfterRemoteInterceptor(ri map[string]string) error {
	fmt.Println(ri)
	return nil
}

func (this *GlobalRemoteInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:create")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:create")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:load")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:load")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:update")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:update")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:duplicate")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:duplicate")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:delete")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:delete")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:listmap")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:listmap")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:listarray")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:listarray")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:querymap")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:querymap")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:queryarray")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:queryarray")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) BeforeExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:exec")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(ri)
}
func (this *GlobalRemoteInterceptor) AfterExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) error {
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:exec")
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(ri)
}
