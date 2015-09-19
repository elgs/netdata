package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/elgs/gostrgen"
	"github.com/satori/go.uuid"
	"strings"
	"time"
)

func init() {
	tableId := "netdata.project"
	gorest2.RegisterDataInterceptor(tableId, &ProjectInterceptor{Id: tableId})
}

type ProjectInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *ProjectInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	context["tx"] = tx

	projectKey, err := gostrgen.RandGen(16, gostrgen.LowerDigit, "", "")
	if err != nil {
		return false, err
	}
	data["PROJECT_KEY"] = projectKey
	data["STATUS"] = "0"

	return true, nil
}
func (this *ProjectInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	context["tx"] = tx
	return true, nil
}

func afterCreateOrUpdateProject(db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	projectId := data["ID"].(string)
	projectKey := data["PROJECT_KEY"].(string)
	// Update members
	members := strings.Split(data["MEMBERS"].(string), ",")
	tx := context["tx"].(*sql.Tx)
	_, err := gosqljson.ExecTx(tx, "DELETE FROM user_project WHERE PROJECT_ID=?", projectId)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" {
			continue
		}

		userToken := context["user_token"]
		v := userToken.(map[string]string)
		memberData := map[string]interface{}{
			"ID":           uuid.NewV4().String(),
			"USER_EMAIL":   member,
			"PROJECT_ID":   projectId,
			"PROJECT_NAME": data["NAME"],
			"STATUS":       "0",
			"CREATOR_ID":   v["ID"],
			"CREATOR_CODE": v["EMAIL"],
			"CREATE_TIME":  time.Now(),
			"UPDATER_ID":   v["ID"],
			"UPDATER_CODE": v["EMAIL"],
			"UPDATE_TIME":  time.Now(),
		}
		_, err = TxInsert(tx, "user_project", memberData)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	// Create database
	query := `SELECT * FROM data_store WHERE DATA_STORE_NAME=?`
	projectData, err := gosqljson.QueryDbToMap(db, "", query, data["DATA_STORE_NAME"])
	if err != nil {
		fmt.Println(err)
		return err
	}
	if projectData == nil || len(projectData) == 0 {
		return errors.New("Failed to create project.")
	}
	dboData := projectData[0]
	ds := fmt.Sprintf("%v:%v@tcp(%v:%v)/", dboData["USERNAME"], dboData["PASSWORD"],
		dboData["HOST"], dboData["PORT"])
	projectDb, err := sql.Open("mysql", ds)
	defer projectDb.Close()
	if err != nil {
		fmt.Println(err)
		return err
	}

	dbName := "nd_" + projectKey

	_, err = gosqljson.ExecDb(projectDb, "CREATE DATABASE IF NOT EXISTS "+dbName+
		" DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci")
	if err != nil {
		fmt.Println(err)
		return err
	}

	sqlGrant := fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.* TO `%s`@`%%` IDENTIFIED BY \"%s\";", dbName, projectKey, projectId)
	_, err = gosqljson.ExecDb(projectDb, sqlGrant)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (this *ProjectInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return afterCreateOrUpdateProject(db, context, data)
}

func (this *ProjectInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data map[string]interface{}) error {
	return afterCreateOrUpdateProject(db, context, data)
}

func filterPorjects(context map[string]interface{}, filter *string) (bool, error) {
	userToken := context["user_token"]
	if v, ok := userToken.(map[string]string); ok {
		userId := v["ID"]
		userEmail := v["EMAIL"]
		gorest2.MysqlSafe(&userId)
		*filter += fmt.Sprint(` AND (CREATOR_ID='`, userId, `' 
			OR EXISTS (SELECT 1 FROM user_project WHERE project.ID=user_project.PROJECT_ID AND user_project.USER_EMAIL='`+userEmail+`'))`)
		return true, nil
	} else {
		return false, errors.New("Invalid user token.")
	}
}

func (this *ProjectInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) (bool, error) {
	// check ownership
	return true, nil
}
func (this *ProjectInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id string) error {
	// cleanup, user_project, table, db_user
	return nil
}

func (this *ProjectInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64, includeTotal bool) (bool, error) {
	return filterPorjects(context, filter)
}
func (this *ProjectInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64, includeTotal bool) (bool, error) {
	return filterPorjects(context, filter)
}
