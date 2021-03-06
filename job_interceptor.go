package main

import (
	"database/sql"
	"fmt"

	"github.com/elgs/gorest2"
)

func init() {
	tableId := "netdata.job"
	gorest2.RegisterDataInterceptor(tableId, 0, &JobInterceptor{Id: tableId})
}

type JobInterceptor struct {
	*gorest2.DefaultDataInterceptor
	Id string
}

func (this *JobInterceptor) BeforeCreate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) (bool, error) {
	for _, data1 := range data {
		data1["CRON"] = fmt.Sprintf("%s %s", "0", data1["CRON"])
	}
	return true, nil
}
func (this *JobInterceptor) AfterCreate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) error {
	for _, data1 := range data {
		job := make(map[string]string)
		for k, v := range data1 {
			job[k] = fmt.Sprint(v)
		}
		jobId := job["ID"]

		mode := job["MODE"]
		cron := job["CRON"]
		jobRuntimeId, err := jobsCron.AddFunc(cron, jobModes[mode](job))
		if err != nil {
			return err
		}
		jobStatus[jobId] = jobRuntimeId
	}
	return nil
}
func (this *JobInterceptor) BeforeUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) (bool, error) {
	for _, data1 := range data {
		data1["CRON"] = fmt.Sprintf("%s %s", "0", data1["CRON"])
	}
	return true, nil
}
func (this *JobInterceptor) AfterUpdate(resourceId string, db *sql.DB, context map[string]interface{}, data []map[string]interface{}) error {
	for _, data1 := range data {
		job := make(map[string]string)
		for k, v := range data1 {
			job[k] = fmt.Sprint(v)
		}
		jobId := job["ID"]
		if jobRuntimeId, ok := jobStatus[jobId]; ok {
			jobsCron.RemoveFunc(jobRuntimeId)
			delete(jobStatus, jobId)

			mode := job["MODE"]
			cron := job["CRON"]
			jobRuntimeId, err := jobsCron.AddFunc(cron, jobModes[mode](job))
			if err != nil {
				return err
			}
			jobStatus[jobId] = jobRuntimeId
		}
	}
	return nil
}
func (this *JobInterceptor) BeforeDelete(resourceId string, db *sql.DB, context map[string]interface{}, id []string) (bool, error) {
	return true, nil
}
func (this *JobInterceptor) AfterDelete(resourceId string, db *sql.DB, context map[string]interface{}, id []string) error {
	for _, id1 := range id {
		if jobRuntimeId, ok := jobStatus[id1]; ok {
			jobsCron.RemoveFunc(jobRuntimeId)
			delete(jobStatus, id1)
		}
	}
	return nil
}
func (this *JobInterceptor) BeforeListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return true, nil
}
func (this *JobInterceptor) AfterListMap(resourceId string, db *sql.DB, fields string, context map[string]interface{}, data *[]map[string]string, total int64) error {
	return nil
}
func (this *JobInterceptor) BeforeListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, filter *string, sort *string, group *string, start int64, limit int64) (bool, error) {
	return true, nil
}
func (this *JobInterceptor) AfterListArray(resourceId string, db *sql.DB, fields string, context map[string]interface{}, headers *[]string, data *[][]string, total int64) error {
	return nil
}
