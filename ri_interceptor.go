package main

import (
	"database/sql"
	"errors"
	//	"errors"
	"fmt"
	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.remote_interceptor"
	gorest2.RegisterDataInterceptor(tableId, 0, &RiInterceptor{Id: tableId})
}

type RiInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *RiInterceptor) commonAfterInterceptor(context map[string]interface{}, data map[string]interface{}) error {
	if oldData, found := context["old_data"].(map[string]string); found {
		projectId := oldData["PROJECT_ID"]
		target := oldData["TARGET"]
		theType := oldData["TYPE"]
		actionType := oldData["ACTION_TYPE"]
		err := unloadRemoteInterceptor(projectId, target, theType, actionType)
		if err != nil {
			return err
		}
	}
	if data != nil {
		return loadRemoteInterceptor(data["PROJECT_ID"].(string), data["TARGET"].(string), data["TYPE"].(string), data["ACTION_TYPE"].(string))
	}
	return nil
}

func (this *RiInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return this.commonAfterInterceptor(context, data)
}

func (this *RiInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *RiInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	fmt.Println("after update")
	return this.commonAfterInterceptor(context, data)
}

func (this *RiInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *RiInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	return this.commonAfterInterceptor(context, nil)
}

func (this *RiInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.filterInterceptors(context, filter)
}
func (this *RiInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.filterInterceptors(context, filter)
}

func (this *RiInterceptor) filterInterceptors(context map[string]interface{}, filter *string) (bool, error) {
	userToken := context["user_token"]
	if v, ok := userToken.(map[string]string); ok {
		userId := v["ID"]
		userEmail := v["EMAIL"]
		gorest2.MysqlSafe(&userId)
		*filter += fmt.Sprint(` AND (CREATOR_ID='`, userId, `' 
			OR EXISTS (SELECT 1 FROM user_project WHERE remote_interceptor.PROJECT_ID=user_project.PROJECT_ID AND user_project.USER_EMAIL='`+userEmail+`'))`)
		return true, nil
	} else {
		return false, errors.New("Invalid interceptor.")
	}
}
