package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.user_stats"
	gorest2.RegisterDataInterceptor(tableId, 0, &UserStatsInterceptor{Id: tableId})
}

type UserStatsInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *UserStatsInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.filter(context, filter)
}
func (this *UserStatsInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return this.filter(context, filter)
}

func (this *UserStatsInterceptor) filter(context map[string]interface{}, filter *string) (bool, error) {
	userToken := context["user_token"]
	if v, ok := userToken.(map[string]string); ok {
		userId := v["ID"]
		userEmail := v["EMAIL"]
		gorest2.MysqlSafe(&userId)
		*filter += fmt.Sprint(` AND EXISTS (SELECT 1 FROM user_project WHERE user_stats.PROJECT_ID=user_project.PROJECT_ID AND user_project.USER_EMAIL='` + userEmail + `')`)
		return true, nil
	} else {
		return false, errors.New("Invalid user token.")
	}
}
