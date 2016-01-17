package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.token"
	gorest2.RegisterDataInterceptor(tableId, 0, &TokenInterceptor{Id: tableId})
}

type TokenInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *TokenInterceptor) commonAfterCreateOrUpdateToken(projectId, token string) error {
	key := fmt.Sprint("token:", projectId, ":", token)
	err := gorest2.RedisMaster.Del(key).Err()
	return err
}

func (this *TokenInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *TokenInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	token := context["old_data"].(map[string]string)["TOKEN"]
	projectId := context["old_data"].(map[string]string)["PROJECT_ID"]
	return this.commonAfterCreateOrUpdateToken(projectId, token)
}

func (this *TokenInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *TokenInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	token := context["old_data"].(map[string]string)["TOKEN"]
	projectId := context["old_data"].(map[string]string)["PROJECT_ID"]
	return this.commonAfterCreateOrUpdateToken(projectId, token)
}

func (this *TokenInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.filterTokens(context, filter)
}
func (this *TokenInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.filterTokens(context, filter)
}

func (this *TokenInterceptor) filterTokens(context map[string]interface{}, filter *string) (bool, error) {
	userToken := context["user_token"]
	if v, ok := userToken.(map[string]string); ok {
		userId := v["ID"]
		userEmail := v["EMAIL"]
		gorest2.MysqlSafe(&userId)
		*filter += fmt.Sprint(` AND (CREATOR_ID='`, userId, `' 
			OR EXISTS (SELECT 1 FROM user_project WHERE token.PROJECT_ID=user_project.PROJECT_ID AND user_project.USER_EMAIL='`+userEmail+`'))`)
		return true, nil
	} else {
		return false, errors.New("Invalid user token.")
	}
}
