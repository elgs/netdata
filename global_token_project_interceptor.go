package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"strings"
	"time"
)

func init() {
	gorest2.RegisterGlobalDataInterceptor(20, &GlobalTokenProjectInterceptor{Id: "GlobalTokenProjectInterceptor"})
}

type GlobalTokenProjectInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

//var projectTokenRegistry = make(map[string]map[string]string)

// server, client, server, client
func checkAccessPermission(targets, tableId, mode, op string) bool {
	tableMatch, opMatch := false, true
	if targets == "*" {
		tableMatch = true
	} else {
		ts := strings.Split(strings.Replace(tableId, "`", "", -1), ".")
		tableName := ts[len(ts)-1]
		targetsArray := strings.Split(targets, ",")
		for _, target := range targetsArray {
			if target == tableName {
				tableMatch = true
				break
			}
		}
	}
	if !tableMatch {
		return false
	}
	for _, c := range op {
		if !strings.ContainsRune(mode, c) {
			return false
		}
	}
	return tableMatch && opMatch
}

func checkProjectToken(projectId string, token string, tableId string, op string) (bool, error) {
	key := fmt.Sprint("token:", projectId, ":", token)
	tokenMap := gorest2.RedisLocal.HGetAllMap(token).Val()

	if projectId != "" && token != "" && len(tokenMap) > 0 {
		if checkAccessPermission(tokenMap["targets"], tableId, tokenMap["mode"], op) {
			return true, nil
		} else {
			return false, errors.New("Authentication failed.")
		}
	}

	defaultDbo := gorest2.GetDbo("default")
	defaultDb, err := defaultDbo.GetConn()
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	userData, err := gosqljson.QueryDbToMap(defaultDb, "upper",
		"SELECT * FROM token WHERE PROJECT_ID=? AND TOKEN=? AND STATUS=?", projectId, token, "0")
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	if userData != nil && len(userData) == 1 {
		record := userData[0]
		err := gorest2.RedisMaster.HMSet(key, "targets", record["TARGETS"], "mode", record["MODE"]).Err()
		if err != nil {
			return false, err
		}
		if checkAccessPermission(record["TARGETS"], tableId, record["MODE"], op) {
			return true, nil
		} else {
			return false, errors.New("Authentication failed.")
		}
	} else {
		userData, err := gosqljson.QueryDbToMap(defaultDb, "upper",
			`SELECT u.TOKEN_KEY AS TOKEN,up.PROJECT_ID FROM user AS u INNER JOIN user_project AS up ON u.EMAIL=up.USER_EMAIL 
			WHERE u.TOKEN_KEY=? AND up.PROJECT_ID=? AND u.STATUS=? AND up.STATUS=?`,
			token, projectId, "0", "0")
		if err != nil {
			fmt.Println(err)
			return false, err
		}
		if userData != nil && len(userData) > 0 {
			err := gorest2.RedisMaster.HMSet(key, "targets", "*", "mode", "rwx").Err()
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, errors.New("Authentication failed.")
}

func (this *GlobalTokenProjectInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	ctn, err := checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "w")
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			data["CREATE_TIME"] = time.Now().UTC()
			data["UPDATE_TIME"] = time.Now().UTC()
			if userId, found := context["user_id"]; found {
				data["CREATOR_ID"] = userId
				data["UPDATER_ID"] = userId
			}
			if email, found := context["email"]; found {
				data["CREATOR_CODE"] = email
				data["UPDATER_CODE"] = email
			}
		}
	}
	return ctn, err
}
func (this *GlobalTokenProjectInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, id string) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "r")
}
func (this *GlobalTokenProjectInterceptor) AfterLoad(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data map[string]string) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	ctn, err := checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "w")
	if ctn && err == nil {
		if context["meta"] != nil && context["meta"].(bool) {
			data["UPDATE_TIME"] = time.Now().UTC()
		}
		if userId, found := context["user_id"]; found {
			data["UPDATER_ID"] = userId
		}
		if email, found := context["email"]; found {
			data["UPDATER_CODE"] = email
		}
	}
	return ctn, err
}
func (this *GlobalTokenProjectInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "w")
}
func (this *GlobalTokenProjectInterceptor) AfterDuplicate(resourceId string, db *sql.DB, context map[string]interface{}, id string, newId string) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "w")
}
func (this *GlobalTokenProjectInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "r")
}
func (this *GlobalTokenProjectInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "r")
}
func (this *GlobalTokenProjectInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "rx")
}
func (this *GlobalTokenProjectInterceptor) AfterQueryMap(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, data *[]map[string]string) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "rx")
}
func (this *GlobalTokenProjectInterceptor) AfterQueryArray(resourceId string, script string, params *[]interface{}, db *sql.DB, context map[string]interface{}, headers *[]string, data *[][]string) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}) (bool, error) {
	if isDefaultProjectRequest(context) {
		return true, nil
	}
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "wx")
}
func (this *GlobalTokenProjectInterceptor) AfterExec(resourceId string, scripts string, params *[]interface{}, tx *sql.Tx, context map[string]interface{}, rowsAffectedArray []int64) error {
	return nil
}
