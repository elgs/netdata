// global_remote_interceptor
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/satori/go.uuid"
	"strings"
	"time"
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

func (this *GlobalRemoteInterceptor) checkAgainstBeforeRemoteInterceptor(data string, appId string, resourceId string, action string, ri map[string]string) (bool, error) {
	res, status, err := httpRequest(ri["url"], ri["method"], data, int64(len([]byte(data))))
	if err != nil {
		return false, err
	}
	if status == 200 && string(res) == data {
		return true, nil
	}
	return false, errors.New("Client rejected.")
}

func (this *GlobalRemoteInterceptor) executeAfterRemoteInterceptor(data string, appId string, resourceId string, action string, ri map[string]string) error {
	dataId := strings.Replace(uuid.NewV4().String(), "-", "", -1)
	insert := `INSERT INTO push_notification(ID,PROJECT_ID,TARGET,METHOD,URL,TYPE,ACTION_TYPE,STATUS,DATA,CREATE_TIME,UPDATE_TIME) 
	VALUES(?,?,?,?,?,?,?,?,?,?,?)`
	now := time.Now().UTC()
	params := []interface{}{dataId, appId, resourceId, ri["method"], ri["url"], "after", action, "0", data, now, now}
	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	_, err = gosqljson.ExecDb(defaultDb, insert, params...)
	return err
}

func (this *GlobalRemoteInterceptor) commonBefore(resourceId string, context map[string]interface{}, action string, data string) (bool, error) {
	rts := strings.Split(strings.Replace(resourceId, "`", "", -1), ".")
	resourceId = rts[len(rts)-1]
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":before:", action)
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}
	return this.checkAgainstBeforeRemoteInterceptor(data, appId, resourceId, action, ri)
}

func (this *GlobalRemoteInterceptor) commonAfter(resourceId string, context map[string]interface{}, action string, data string) error {
	rts := strings.Split(strings.Replace(resourceId, "`", "", -1), ".")
	resourceId = rts[len(rts)-1]
	appId := context["app_id"].(string)
	key := fmt.Sprint("ri:", appId, ":", resourceId, ":after:", action)
	ri := redisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}
	return this.executeAfterRemoteInterceptor(data, appId, resourceId, action, ri)
}

func (this *GlobalRemoteInterceptor) createPayPload(target string, action string, data interface{}) (string, error) {
	rts := strings.Split(strings.Replace(target, "`", "", -1), ".")
	target = rts[len(rts)-1]
	m := map[string]interface{}{
		"target": target,
		"action": action,
		"data":   data,
	}
	jsonData, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

func (this *GlobalRemoteInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_create", data)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "create", payload)
}
func (this *GlobalRemoteInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	payload, err := this.createPayPload(resourceId, "after_create", data)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "create", payload)
}
func (this *GlobalRemoteInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_load", id)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "load", payload)
}
func (this *GlobalRemoteInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	payload, err := this.createPayPload(resourceId, "after_load", data)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "load", payload)
}
func (this *GlobalRemoteInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_update", data)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "update", payload)
}
func (this *GlobalRemoteInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	payload, err := this.createPayPload(resourceId, "after_update", data)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "update", payload)
}
func (this *GlobalRemoteInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_duplicate", id)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "duplicate", payload)
}
func (this *GlobalRemoteInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	payload, err := this.createPayPload(resourceId, "after_update", newId)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "duplicate", payload)
}
func (this *GlobalRemoteInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_delete", id)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "delete", payload)
}
func (this *GlobalRemoteInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	payload, err := this.createPayPload(resourceId, "after_delete", id)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "delete", payload)
}
func (this *GlobalRemoteInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_list_map", filter)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "list_map", payload)
}
func (this *GlobalRemoteInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	payload, err := this.createPayPload(resourceId, "after_list_map", data)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "list_map", payload)
}
func (this *GlobalRemoteInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_list_array", filter)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "list_array", payload)
}
func (this *GlobalRemoteInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	payload, err := this.createPayPload(resourceId, "after_list_array", []interface{}{headers, data})
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "list_array", payload)
}
func (this *GlobalRemoteInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_query_map", params)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "query_map", payload)
}
func (this *GlobalRemoteInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	payload, err := this.createPayPload(resourceId, "after_query_map", data)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "query_map", payload)
}
func (this *GlobalRemoteInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_query_array", params)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "query_array", payload)
}
func (this *GlobalRemoteInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	payload, err := this.createPayPload(resourceId, "after_query_array", []interface{}{headers, data})
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "query_array", payload)
}
func (this *GlobalRemoteInterceptor) BeforeExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	payload, err := this.createPayPload(resourceId, "before_exec", params)
	if err != nil {
		return false, err
	}
	return this.commonBefore(resourceId, context, "exec", payload)
}
func (this *GlobalRemoteInterceptor) AfterExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}, rowsAffectedArray []int64) error {
	payload, err := this.createPayPload(resourceId, "after_exec", rowsAffectedArray)
	if err != nil {
		return err
	}
	return this.commonAfter(resourceId, context, "exec", payload)
}
