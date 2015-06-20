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
	gorest2.RegisterGlobalDataInterceptor(&GlobalTokenProjectInterceptor{Id: "GlobalTokenProjectInterceptor"})
}

type GlobalTokenProjectInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

var projectTokenRegistry = make(map[string]map[string]string)

func checkAccessPermission(targets, tableId, mode, op string) bool {
	tableMatch, opMatch := false, false
	if targets == "*" {
		tableMatch = true
	} else {
		tableName := strings.Split(strings.Replace(tableId, "`", "", -1), ".")[1]
		targetsArray := strings.Split(targets, ",")
		for _, target := range targetsArray {
			if target == tableName {
				tableMatch = true
				break
			}
		}
	}
	if strings.Contains(mode, op) {
		opMatch = true
	}
	return tableMatch && opMatch
}

func checkProjectToken(projectId string, key string, tableId string, op string) (bool, error) {
	if projectId != "" && key != "" && len(projectTokenRegistry[key]) > 0 &&
		projectTokenRegistry[key]["TOKEN"] == key && projectTokenRegistry[key]["PROJECT_ID"] == projectId {
		if checkAccessPermission(projectTokenRegistry[key]["TARGETS"], tableId, projectTokenRegistry[key]["MODE"], op) {
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
		"SELECT * FROM token WHERE PROJECT_ID=? AND TOKEN=? AND STATUS=?", projectId, key, "0")
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	if userData != nil && len(userData) == 1 {
		record := userData[0]
		projectTokenRegistry[key] = record
		if checkAccessPermission(projectTokenRegistry[key]["TARGETS"], tableId, projectTokenRegistry[key]["MODE"], op) {
			return true, nil
		} else {
			return false, errors.New("Authentication failed.")
		}
	} else {
		userData, err := gosqljson.QueryDbToMap(defaultDb, "upper",
			`SELECT u.TOKEN_KEY AS TOKEN,up.PROJECT_ID FROM user AS u INNER JOIN user_project AS up ON u.EMAIL=up.USER_EMAIL 
			WHERE u.TOKEN_KEY=? AND up.PROJECT_ID=? AND u.STATUS=? AND up.STATUS=?`,
			key, projectId, "0", "0")
		if err != nil {
			fmt.Println(err)
			return false, err
		}
		if userData != nil && len(userData) > 0 {
			userData[0]["MODE"] = "rw"
			userData[0]["TARGETS"] = "*"
			record := userData[0]
			projectTokenRegistry[key] = record
			if checkAccessPermission(projectTokenRegistry[key]["TARGETS"], tableId, projectTokenRegistry[key]["MODE"], op) {
				return true, nil
			} else {
				return false, errors.New("Authentication failed.")
			}
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
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "r")
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
	ctn, err := checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "w")
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
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "w")
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
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "w")
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
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "r")
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
	return checkProjectToken(context["app_id"].(string), context["token"].(string), resourceId, "r")
}
func (this *GlobalTokenProjectInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers []string, data [][]string, total int64) error {
	if isDefaultProjectRequest(context) {
		return nil
	}
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeQueryMap(resourceId string, db *sql.DB, context map[string]interface{}, start int64, limit int64, includeTotal bool) (bool, error) {
	return true, nil
}
func (this *GlobalTokenProjectInterceptor) AfterQueryMap(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]string, total int64) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeQueryArray(resourceId string, db *sql.DB, context map[string]interface{}, start int64, limit int64, includeTotal bool) (bool, error) {
	return true, nil
}
func (this *GlobalTokenProjectInterceptor) AfterQueryArray(resourceId string, db *sql.DB, context map[string]interface{}, headers []string, data [][]string, total int64) error {
	return nil
}
func (this *GlobalTokenProjectInterceptor) BeforeExec(resourceId string, db *sql.DB, context map[string]interface{}) (bool, error) {
	return true, nil
}
func (this *GlobalTokenProjectInterceptor) AfterExec(resourceId string, db *sql.DB, context map[string]interface{}) error {
	return nil
}
