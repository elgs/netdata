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
	gorest2.RegisterGlobalDataInterceptor(&GlobalTokenInterceptor{Id: "GlobalTokenInterceptor"})
}

func isDefaultProjectRequest(context map[string]interface{}) bool {
	return len(context["app_id"].(string)) != 36
}

var acl = make(map[string]map[string]bool)
var defaultTokenRegistry = make(map[string]map[string]string)

func checkDefaultToken(db *sql.DB, key string, context map[string]interface{}, tableId string) (bool, error) {
	if key != "" && len(defaultTokenRegistry[key]) > 0 && defaultTokenRegistry[key]["TOKEN_KEY"] == key {
		context["user_token"] = defaultTokenRegistry[key]
		return true, nil
	}

	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	userData, err := gosqljson.QueryDbToMap(defaultDb, "upper",
		"SELECT * FROM user WHERE TOKEN_KEY=? AND STATUS=?", key, "0")
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	if userData != nil && len(userData) == 1 {
		record := userData[0]
		defaultTokenRegistry[key] = record
		context["user_token"] = record
		return true, nil
	}
	return false, errors.New("Authentication failed.")
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
	ctn, err := checkDefaultToken(db, context["token"].(string), context, resourceId)
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			userToken := context["user_token"]
			if v, ok := userToken.(map[string]string); ok {
				data["CREATOR_ID"] = v["ID"]
				data["CREATOR_CODE"] = v["EMAIL"]
				data["CREATE_TIME"] = time.Now()
				data["UPDATER_ID"] = v["ID"]
				data["UPDATER_CODE"] = v["EMAIL"]
				data["UPDATE_TIME"] = time.Now()
			}
		}
	}
	return ctn, err
}
func (this *GlobalTokenInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	if !isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "load"); !ok {
		return false, err
	}
	return checkDefaultToken(db, context["token"].(string), context, resourceId)
}
func (this *GlobalTokenInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	if !isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "update"); !ok {
		return false, err
	}
	ctn, err := checkDefaultToken(db, context["token"].(string), context, resourceId)
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			userToken := context["user_token"]
			if v, ok := userToken.(map[string]string); ok {
				data["UPDATER_ID"] = v["ID"]
				data["UPDATER_CODE"] = v["EMAIL"]
				data["UPDATE_TIME"] = time.Now()
			}
		}
	}
	return ctn, err
}
func (this *GlobalTokenInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	if !isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "duplicate"); !ok {
		return false, err
	}
	return checkDefaultToken(db, context["token"].(string), context, resourceId)
}
func (this *GlobalTokenInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	if !isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "delete"); !ok {
		return false, err
	}
	return checkDefaultToken(db, context["token"].(string), context, resourceId)
}
func (this *GlobalTokenInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	if !isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64, includeTotal bool) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "list"); !ok {
		return false, err
	}
	return checkDefaultToken(db, context["token"].(string), context, resourceId)
}
func (this *GlobalTokenInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data []map[string]string, total int64) error {
	if !isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64, includeTotal bool) (bool, error) {
	if !isDefaultProjectRequest(context) {
		return true, nil
	}
	if ok, err := checkACL(resourceId, "list"); !ok {
		return false, err
	}
	return checkDefaultToken(db, context["token"].(string), context, resourceId)
}
func (this *GlobalTokenInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers []string, data [][]string, total int64) error {
	if !isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
