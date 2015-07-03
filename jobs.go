// jobs
package main

import (
	"fmt"
	"github.com/elgs/cron"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
)

func init() {
	jobModes["sql"] = func(job map[string]string) func() {
		return func() {
			defer func() {
				if err := recover(); err != nil {
					fmt.Println(err)
				}
			}()
			script := job["SCRIPT"]
			projectId := job["PROJECT_ID"]
			dbo := gorest2.GetDbo(projectId)
			db, err := dbo.GetConn()
			if err != nil {
				fmt.Println()
				return
			}
			_, err = gosqljson.ExecDb(db, script)
			if err != nil {
				fmt.Println()
				return
			}
		}
	}
}

var jobsCron = cron.New()
var jobModes = make(map[string]func(map[string]string) func())

var startJobs = func() {
	defaultDbo := gorest2.DboRegistry["default"]
	db, err := defaultDbo.GetConn()
	if err != nil {
		fmt.Println(err)
		return
	}
	query := `SELECT job.*,project.ID AS PROJECT_ID FROM job INNER JOIN project ON project.ID=job.PROJECT_ID WHERE job.STATUS='0'`
	data, err := gosqljson.QueryDbToMap(db, "", query)
	if err != nil {
		fmt.Println(err)
		return
	}
	if data == nil || len(data) == 0 {
		return
	}
	for _, job := range data {
		mode := job["MODE"]
		cron := job["CRON"]
		_, err := jobsCron.AddFunc(cron, jobModes[mode](job))
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
	jobsCron.Start()
}
