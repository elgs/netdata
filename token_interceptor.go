package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/satori/go.uuid"
	"strings"
	"time"
)

func init() {
	tableId := "netdata.token"
	gorest2.RegisterDataInterceptor(tableId, 0, &TokenInterceptor{Id: tableId})
}

type TokenInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *TokenInterceptor) commonAfterCreateOrUpdateToken(token string) {
	delete(projectTokenRegistry, token)
}

func (this *TokenInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *TokenInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	this.commonAfterCreateOrUpdateToken(context["old_data"].(map[string]string)["TOKEN"])
	return nil
}

func (this *TokenInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	context["load"] = true
	return true, nil
}

func (this *TokenInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	token := context["old_data"].(map[string]string)["TOKEN"]
	this.commonAfterCreateOrUpdateToken(token)
	recordId := strings.Replace(uuid.NewV4().String(), "-", "", -1)
	now := time.Now().UTC()
	_, err := gosqljson.ExecDb(db, `INSERT INTO revoked_list(ID,PROJECT_ID,OBJECT_ID,OBJECT_TYPE,CREATE_TIME,UPDATE_TIME)
	VALUES(?,?,?,?,?,?)`, recordId, context["old_data"].(map[string]string)["PROJECT_ID"], token, "token", now, now)
	if err != nil {
		return err
	}
	return nil
}

func (this *TokenInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return filterTokens(context, filter)
}
func (this *TokenInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return filterTokens(context, filter)
}

func filterTokens(context map[string]interface{}, filter *string) (bool, error) {
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
