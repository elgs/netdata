// global_local_interceptor
package main

import (
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/elgs/jsonql"
)

func init() {
	loadACL()
	gorest2.RegisterGlobalDataInterceptor(40, &GlobalLocalInterceptor{Id: "GlobalLocalInterceptor"})
}

type GlobalLocalInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func loadAllLocalInterceptor() error {
	pipe := gorest2.RedisMaster.Pipeline()
	defer pipe.Close()

	// load all local interceptor definitions into LocalInterceptorRegistry
	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	liData, err := gosqljson.QueryDbToMap(defaultDb, "upper", "SELECT * FROM local_interceptor")
	if err != nil {
		return err
	}
	for _, liMap := range liData {
		projectId := liMap["PROJECT_ID"]
		target := liMap["TARGET"]
		theType := liMap["TYPE"]
		actionType := liMap["ACTION_TYPE"]
		script := liMap["SCRIPT"]
		key := strings.Join([]string{"li", projectId, target, theType, actionType}, ":")
		pipe.HMSet(key, "script", script)
	}
	_, err = pipe.Exec()
	return err
}

func loadLocalInterceptor(projectId, target, theType, actionType string) error {
	// load specific local interceptor definitions into LocalInterceptorRegistry
	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	liData, err := gosqljson.QueryDbToMap(defaultDb,
		"upper", "SELECT * FROM local_interceptor WHERE PROJECT_ID=? AND TARGET=? AND TYPE=? AND ACTION_TYPE=?",
		projectId, target, theType, actionType)
	if err != nil {
		return err
	}
	if liData != nil && len(liData) == 1 {
		liMap := liData[0]
		projectId := liMap["PROJECT_ID"]
		target := liMap["TARGET"]
		theType := liMap["TYPE"]
		actionType := liMap["ACTION_TYPE"]
		script := liMap["SCRIPT"]
		key := strings.Join([]string{"li", projectId, target, theType, actionType}, ":")
		gorest2.RedisMaster.HMSet(key, "script", script)
	}
	return nil
}

func unloadLocalInterceptor(projectId, target, theType, actionType string) error {
	// unload specific local interceptor definitions into LocalInterceptorRegistry
	key := strings.Join([]string{"li", projectId, target, theType, actionType}, ":")
	err := gorest2.RedisMaster.Del(key).Err()
	return err
}

func (this *GlobalLocalInterceptor) checkAgainstBeforeLocalInterceptor(tx *sql.Tx, db *sql.DB, context map[string]interface{}, data string, appId string, action string, li map[string]string, params [][]interface{}, queryParams []string) (bool, error) {
	callback := li["callback"]

	if strings.TrimSpace(callback) != "" {
		// return a array of array as parameters for callback
		query, err := loadQuery(appId, callback)
		if err != nil {
			return false, err
		}
		scripts := query["script"]
		replaceContext := buildReplaceContext(context)
		_, err = batchExecuteTx(tx, db, &scripts, queryParams, params, replaceContext)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (this *GlobalLocalInterceptor) executeAfterLocalInterceptor(tx *sql.Tx, db *sql.DB, data string, appId string, resourceId string, action string, li map[string]string, params [][]interface{}, queryParams []string) error {
	return nil
}

func (this *GlobalLocalInterceptor) commonBefore(tx *sql.Tx, db *sql.DB, resourceId string, context map[string]interface{}, action string, data interface{}, params [][]interface{}, queryParams []string) (bool, error) {
	rts := strings.Split(strings.Replace(resourceId, "`", "", -1), ".")
	resourceId = rts[len(rts)-1]
	appId := context["app_id"].(string)
	key := strings.Join([]string{"li", appId, resourceId, "before", action}, ":")
	li := gorest2.RedisLocal.HGetAllMap(key).Val()
	if len(li) == 0 {
		return true, nil
	}

	criteria := li["criteria"]
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

	payload, err := this.createPayload(resourceId, "before_"+action, data)
	if err != nil {
		return false, err
	}

	return this.checkAgainstBeforeLocalInterceptor(tx, db, context, payload, appId, action, li, params, queryParams)
}

func (this *GlobalLocalInterceptor) commonAfter(tx *sql.Tx, db *sql.DB, resourceId string, context map[string]interface{}, action string, data interface{}, params [][]interface{}, queryParams []string) error {
	rts := strings.Split(strings.Replace(resourceId, "`", "", -1), ".")
	resourceId = rts[len(rts)-1]
	appId := context["app_id"].(string)
	key := strings.Join([]string{"li", appId, resourceId, "after", action}, ":")
	li := gorest2.RedisLocal.HGetAllMap(key).Val()
	if len(li) == 0 {
		return nil
	}

	criteria := li["criteria"]
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
	return this.executeAfterLocalInterceptor(tx, db, payload, appId, resourceId, action, li, params, queryParams)
}

func (this *GlobalLocalInterceptor) createPayload(target string, action string, data interface{}) (string, error) {
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

func (this *GlobalLocalInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) (bool, error) {
	ret, err := this.commonBefore(nil, db, resourceId, context, "create", data, nil, nil)
	if !ret || err != nil {
		return ret, err
	}
	return ret, err
}
func (this *GlobalLocalInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) error {
	err := this.commonAfter(nil, db, resourceId, context, "create", data, nil, nil)
	if err != nil {
		return err
	}
	return err
}
func (this *GlobalLocalInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	return this.commonBefore(nil, db, resourceId, context, "load", map[string]string{"id": id}, nil, nil)
}
func (this *GlobalLocalInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	return this.commonAfter(nil, db, resourceId, context, "load", data, nil, nil)
}
func (this *GlobalLocalInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) (bool, error) {
	ret, err := this.commonBefore(nil, db, resourceId, context, "update", data, nil, nil)
	if !ret || err != nil {
		return ret, err
	}
	return ret, err
}
func (this *GlobalLocalInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) error {
	err := this.commonAfter(nil, db, resourceId, context, "update", data, nil, nil)
	if err != nil {
		return err
	}
	return err
}
func (this *GlobalLocalInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id []string) (bool, error) {
	ret, err := this.commonBefore(nil, db, resourceId, context, "duplicate", map[string][]string{"id": id}, nil, nil)
	if !ret || err != nil {
		return ret, err
	}
	return ret, err
}
func (this *GlobalLocalInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id []string, newId []string) error {
	err := this.commonAfter(nil, db, resourceId, context, "duplicate", map[string][]string{"new_id": newId}, nil, nil)
	if err != nil {
		return err
	}
	return nil
}
func (this *GlobalLocalInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id []string) (bool, error) {
	ret, err := this.commonBefore(nil, db, resourceId, context, "delete", map[string][]string{"id": id}, nil, nil)
	if !ret || err != nil {
		return ret, err
	}
	return ret, nil
}
func (this *GlobalLocalInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id []string) error {
	err := this.commonAfter(nil, db, resourceId, context, "delete", map[string][]string{"id": id}, nil, nil)
	if err != nil {
		return err
	}
	return nil
}
func (this *GlobalLocalInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.commonBefore(nil, db, resourceId, context, "list_map", map[string]interface{}{"fields": fields, "filter": *filter, "sort": *sort, "group": *group, "start": start, "limit": limit}, nil, nil)
}
func (this *GlobalLocalInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	return this.commonAfter(nil, db, resourceId, context, "list_map", *data, nil, nil)
}
func (this *GlobalLocalInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.commonBefore(nil, db, resourceId, context, "list_array", map[string]interface{}{"fields": fields, "filter": *filter, "sort": *sort, "group": *group, "start": start, "limit": limit}, nil, nil)
}
func (this *GlobalLocalInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	return this.commonAfter(nil, db, resourceId, context, "list_array", map[string]interface{}{"headers": *headers, "data": *data}, nil, nil)
}
func (this *GlobalLocalInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	return this.commonBefore(nil, db, resourceId, context, "query_map", map[string]interface{}{"params": *params}, nil, nil)
}
func (this *GlobalLocalInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	return this.commonAfter(nil, db, resourceId, context, "query_map", *data, nil, nil)
}
func (this *GlobalLocalInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	return this.commonBefore(nil, db, resourceId, context, "query_array", map[string]interface{}{"params": *params}, nil, nil)
}
func (this *GlobalLocalInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	return this.commonAfter(nil, db, resourceId, context, "query_array", map[string]interface{}{"headers": *headers, "data": *data}, nil, nil)
}
func (this *GlobalLocalInterceptor) BeforeExec(resourceId string, scripts string, params *[][]interface{}, queryParams []string, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	ret, err := this.commonBefore(tx, nil, resourceId, context, "exec", map[string]interface{}{"params": *params, "query_params": queryParams}, *params, queryParams)
	if !ret || err != nil {
		return ret, err
	}
	return ret, err
}
func (this *GlobalLocalInterceptor) AfterExec(resourceId string, scripts string, params *[][]interface{}, queryParams []string, tx *sql.Tx, context map[string]interface{}, rowsAffectedArray [][]int64) error {
	err := this.commonAfter(tx, nil, resourceId, context, "exec", map[string]interface{}{"params": *params, "query_params": queryParams, "rows_affected": rowsAffectedArray}, *params, queryParams)
	if err != nil {
		return err
	}
	return err
}
