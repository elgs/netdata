package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"io/ioutil"
	"strings"
	"time"
)

func init() {
	loadACL()
	gorest2.RegisterGlobalDataInterceptor(10, &GlobalTokenInterceptor{Id: "GlobalTokenInterceptor"})
}

func isDefaultProjectRequest(context map[string]interface{}) bool {
	return len(context["app_id"].(string)) != 36 && len(context["app_id"].(string)) != 32
}

var acl = make(map[string]map[string]bool)

//var defaultTokenRegistry = make(map[string]map[string]string)

func checkDefaultToken(dToken string, resouceId string) (bool, map[string]string, error) {
	if strings.HasPrefix(resouceId, "__") {
		return true, nil, nil
	}

	key := fmt.Sprint("dtoken:", dToken)
	dTokenMap := redisLocal.HGetAllMap(key).Val()
	if dToken != "" && len(dTokenMap) > 0 {
		return true, dTokenMap, nil
	}

	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		fmt.Println(err)
		return false, nil, err
	}
	userQuery := `SELECT user.* FROM user WHERE user.TOKEN_KEY=? AND user.STATUS=?`
	userData, err := gosqljson.QueryDbToMap(defaultDb, "upper", userQuery, dToken, "0")
	if err != nil {
		fmt.Println(err)
		return false, nil, err
	}
	if userData != nil && len(userData) == 1 {
		record := userData[0]
		redisMaster.HMSet(key, "id", record["ID"], "email", record["EMAIL"])
		return true, record, nil
	}
	return false, nil, errors.New("Authentication failed.")
}

func loadACL() {
	// load acl from configuration files.
	configFile := "gorest_acl.json"
	aclConfig, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println(configFile, " not found, default policies are used.")
	}
	err = json.Unmarshal(aclConfig, &acl)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println(len(acl), acl)
}

func checkACL(tableId string, op string) (bool, error) {
	tableId = strings.Replace(tableId, "`", "", -1)
	if acl[tableId] != nil && !acl[tableId][op] {
		return false, errors.New("Access denied.")
	}
	return true, nil
}

type GlobalTokenInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *GlobalTokenInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "create"); !ok {
		return false, err
	}
	ctn, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			data["CREATOR_ID"] = userToken["ID"]
			data["CREATOR_CODE"] = userToken["EMAIL"]
			data["CREATE_TIME"] = time.Now().UTC()
			data["UPDATER_ID"] = userToken["ID"]
			data["UPDATER_CODE"] = userToken["EMAIL"]
			data["UPDATE_TIME"] = time.Now().UTC()
		}
	}
	context["user_token"] = userToken
	return ctn, err
}
func (this *GlobalTokenInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "load"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "update"); !ok {
		return false, err
	}
	ctn, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			data["UPDATER_ID"] = userToken["ID"]
			data["UPDATER_CODE"] = userToken["EMAIL"]
			data["UPDATE_TIME"] = time.Now().UTC()
		}
	}
	context["user_token"] = userToken
	return ctn, err
}
func (this *GlobalTokenInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "duplicate"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "delete"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "list"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "list"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "query"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "query"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	return nil
}
func (this *GlobalTokenInterceptor) BeforeExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "exec"); !ok {
		return false, err
	}
	allow, userToken, err := checkDefaultToken(context["token"].(string), resourceId)
	context["user_token"] = userToken
	return allow, err
}
func (this *GlobalTokenInterceptor) AfterExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) error {
	return nil
}
