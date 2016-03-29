// global_remote_interceptor
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/elgs/jsonql"
	"github.com/satori/go.uuid"
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
	pipe := gorest2.RedisMaster.Pipeline()
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
		criteria := riMap["CRITERIA"]
		method := riMap["METHOD"]
		url := riMap["URL"]
		key := strings.Join([]string{"ri", projectId, target, theType, actionType}, ":")
		pipe.HMSet(key, "method", method, "url", url, "criteria", criteria)
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
		criteria := riMap["CRITERIA"]
		key := strings.Join([]string{"ri", projectId, target, theType, actionType}, ":")
		gorest2.RedisMaster.HMSet(key, "method", method, "url", url, "criteria", criteria)
	}
	return nil
}

func unloadRemoteInterceptor(projectId, target, theType, actionType string) error {
	// unload specific remote interceptor definitions into RemoteInterceptorRegistry
	key := strings.Join([]string{"ri", projectId, target, theType, actionType}, ":")
	err := gorest2.RedisMaster.Del(key).Err()
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

func (this *GlobalRemoteInterceptor) commonBefore(resourceId string, context map[string]interface{}, action string, data interface{}) (bool, error) {
	rts := strings.Split(strings.Replace(resourceId, "`", "", -1), ".")
	resourceId = rts[len(rts)-1]
	appId := context["app_id"].(string)
	key := strings.Join([]string{"ri", appId, resourceId, "before", action}, ":")
	ri := gorest2.RedisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return true, nil
	}

	criteria := ri["criteria"]
	if len(strings.TrimSpace(criteria)) > 0 {
		parser := jsonql.NewQuery(data)
		criteriaResult, err := parser.Query(criteria)
		if err != nil {
			return true, err
		}

		switch v := criteriaResult.(type) {
		case []interface{}:
			if len(v) == 0 {
				return true, nil
			}
		case map[string]interface{}:
			if v == nil {
				return true, nil
			}
		default:
			return true, nil
		}
		data = criteriaResult
	}

	payload, err := this.createPayload(resourceId, "before"+action, data)
	if err != nil {
		return false, err
	}

	return this.checkAgainstBeforeRemoteInterceptor(payload, appId, resourceId, action, ri)
}

func (this *GlobalRemoteInterceptor) commonAfter(resourceId string, context map[string]interface{}, action string, data interface{}) error {
	rts := strings.Split(strings.Replace(resourceId, "`", "", -1), ".")
	resourceId = rts[len(rts)-1]
	appId := context["app_id"].(string)
	key := strings.Join([]string{"ri", appId, resourceId, "after", action}, ":")
	ri := gorest2.RedisLocal.HGetAllMap(key).Val()
	if len(ri) == 0 {
		return nil
	}

	criteria := ri["criteria"]
	if len(strings.TrimSpace(criteria)) > 0 {
		parser := jsonql.NewQuery(data)
		criteriaResult, err := parser.Query(criteria)
		if err != nil {
			return err
		}

		switch v := criteriaResult.(type) {
		case []interface{}:
			if len(v) == 0 {
				return nil
			}
		case map[string]interface{}:
			if v == nil {
				return nil
			}
		default:
			return nil
		}
		data = criteriaResult
	}
	payload, err := this.createPayload(resourceId, "after_"+action, data)
	if err != nil {
		return err
	}
	return this.executeAfterRemoteInterceptor(payload, appId, resourceId, action, ri)
}

func (this *GlobalRemoteInterceptor) createPayload(target string, action string, data interface{}) (string, error) {
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

func (this *GlobalRemoteInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) (bool, error) {
	ret, err := true, error(nil)
	for _, data1 := range data {
		ret, err = this.commonBefore(resourceId, context, "create", data1)
		if !ret || err != nil {
			return ret, err
		}
	}
	return ret, err
}
func (this *GlobalRemoteInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) error {
	err := error(nil)
	for _, data1 := range data {
		err = this.commonAfter(resourceId, context, "create", data1)
		if err != nil {
			return err
		}
	}
	return err
}
func (this *GlobalRemoteInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	return this.commonBefore(resourceId, context, "load", map[string]string{"id": id})
}
func (this *GlobalRemoteInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	return this.commonAfter(resourceId, context, "load", data)
}
func (this *GlobalRemoteInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) (bool, error) {
	ret, err := true, error(nil)
	for _, data1 := range data {
		ret, err = this.commonBefore(resourceId, context, "update", data)
		if !ret || err != nil {
			return ret, err
		}
	}
	return ret, err
}
func (this *GlobalRemoteInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) error {
	err := error(nil)
	for _, data1 := range data {
		err = this.commonAfter(resourceId, context, "update", data1)
		if err != nil {
			return err
		}
	}
	return err
}
func (this *GlobalRemoteInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id []string) (bool, error) {
	ret, err := true, error(nil)
	for _, id1 := range id {
		ret, err = this.commonBefore(resourceId, context, "duplicate", map[string]string{"id": id1})
		if !ret || err != nil {
			return ret, err
		}
	}
	return ret, err
}
func (this *GlobalRemoteInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id []string, newId []string) error {
	err := error(nil)
	for _, newId1 := range newId {
		err = this.commonAfter(resourceId, context, "duplicate", map[string]string{"new_id": newId1})
		if err != nil {
			return err
		}
	}
	return err
}
func (this *GlobalRemoteInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id []string) (bool, error) {
	ret, err := true, error(nil)
	for _, id1 := range id {
		ret, err = this.commonBefore(resourceId, context, "delete", map[string]string{"id": id1})
		if !ret || err != nil {
			return ret, err
		}
	}
	return ret, err
}
func (this *GlobalRemoteInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id []string) error {
	err := error(nil)
	for _, id1 := range id {
		err = this.commonAfter(resourceId, context, "delete", map[string]string{"id": id1})
		if err != nil {
			return err
		}
	}
	return err
}
func (this *GlobalRemoteInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.commonBefore(resourceId, context, "list_map", map[string]interface{}{"fields": fields, "filter": *filter, "sort": *sort, "group": *group, "start": start, "limit": limit})
}
func (this *GlobalRemoteInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	return this.commonAfter(resourceId, context, "list_map", *data)
}
func (this *GlobalRemoteInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.commonBefore(resourceId, context, "list_array", map[string]interface{}{"fields": fields, "filter": *filter, "sort": *sort, "group": *group, "start": start, "limit": limit})
}
func (this *GlobalRemoteInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	return this.commonAfter(resourceId, context, "list_array", map[string]interface{}{"headers": *headers, "data": *data})
}
func (this *GlobalRemoteInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	return this.commonBefore(resourceId, context, "query_map", map[string]interface{}{"params": *params})
}
func (this *GlobalRemoteInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	return this.commonAfter(resourceId, context, "query_map", *data)
}
func (this *GlobalRemoteInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	return this.commonBefore(resourceId, context, "query_array", map[string]interface{}{"params": *params})
}
func (this *GlobalRemoteInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	return this.commonAfter(resourceId, context, "query_array", map[string]interface{}{"headers": *headers, "data": *data})
}
func (this *GlobalRemoteInterceptor) BeforeExec(resourceId string, scripts string, params *[][]interface{}, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	ret, err := true, error(nil)
	for _, params1 := range *params {
		ret, err = this.commonBefore(resourceId, context, "exec", map[string]interface{}{"params": params1})
		if !ret || err != nil {
			return ret, err
		}
	}
	return ret, err
}
func (this *GlobalRemoteInterceptor) AfterExec(resourceId string, scripts string, params *[][]interface{}, tx *sql.Tx, context map[string]interface{}, rowsAffectedArray []int64) error {
	err := error(nil)
	for _, rowsAffectedArray1 := range rowsAffectedArray {
		err = this.commonAfter(resourceId, context, "exec", map[string]interface{}{"rows_affected": rowsAffectedArray1})
		if err != nil {
			return err
		}
	}
	return err
}
