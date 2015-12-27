package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.query"
	gorest2.RegisterDataInterceptor(tableId, 0, &QueryInterceptor{Id: tableId})
}

type QueryInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *QueryInterceptor) commonAfterCreateOrUpdateQuery(context map[string]interface{}) error {
	queryName := context["old_data"].(map[string]string)["NAME"]
	appId := context["old_data"].(map[string]string)["PROJECT_ID"]
	key := fmt.Sprint("query:", appId, ":", queryName)
	return redisMaster.Del(key).Err()
}

func (this *QueryInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *QueryInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return this.commonAfterCreateOrUpdateQuery(context)
}

func (this *QueryInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *QueryInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	return this.commonAfterCreateOrUpdateQuery(context)
}

func (this *QueryInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return filterQueries(context, filter)
}
func (this *QueryInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return filterQueries(context, filter)
}

func filterQueries(context map[string]interface{}, filter *string) (bool, error) {
	userToken := context["user_token"]
	if v, ok := userToken.(map[string]string); ok {
		userId := v["id"]
		userEmail := v["email"]
		gorest2.MysqlSafe(&userId)
		*filter += fmt.Sprint(` AND (CREATOR_ID='`, userId, `' 
			OR EXISTS (SELECT 1 FROM user_project WHERE query.PROJECT_ID=user_project.PROJECT_ID AND user_project.USER_EMAIL='`+userEmail+`'))`)
		return true, nil
	} else {
		return false, errors.New("Invalid user token.")
	}
}
