// jobs
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/elgs/cron"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosplitargs"
	"github.com/elgs/gosqljson"
)

func init() {
	jobsCron.Start()
	jobModes["sql"] = func(job map[string]string) func() {
		return func() {
			defer func() {
				if err := recover(); err != nil {
					fmt.Println(err)
				}
			}()
			script := job["SCRIPT"]
			projectId := job["PROJECT_ID"]
			loopScript := job["LOOP_SCRIPT"]

			dbo := gorest2.GetDbo(projectId)
			db, err := dbo.GetConn()
			if err != nil {
				fmt.Println(err)
				return
			}
			tx, err := db.Begin()
			if err != nil {
				fmt.Println(err)
				return
			}

			sqlNormalize(&loopScript)
			if len(loopScript) > 0 {
				_, loopData, err := gosqljson.QueryTxToArray(tx, "", loopScript)
				if err != nil {
					fmt.Println(err)
					tx.Rollback()
					return
				}
				for _, row := range loopData {
					scriptReplaced := script
					for i, v := range row {
						scriptReplaced = strings.Replace(script, fmt.Sprint("$", i), v, -1)
					}

					scriptsArray, err := gosplitargs.SplitArgs(scriptReplaced, ";", true)
					if err != nil {
						fmt.Println(err)
						tx.Rollback()
						return
					}

					for _, s := range scriptsArray {
						sqlNormalize(&s)
						if len(s) == 0 {
							continue
						}
						_, err = gosqljson.ExecTx(tx, s)
						if err != nil {
							tx.Rollback()
							fmt.Println(err)
							return
						}
					}
				}
			} else {
				scriptsArray, err := gosplitargs.SplitArgs(script, ";", true)
				if err != nil {
					fmt.Println(err)
					tx.Rollback()
					return
				}

				for _, s := range scriptsArray {
					sqlNormalize(&s)
					if len(s) == 0 {
						continue
					}
					_, err = gosqljson.ExecTx(tx, s)
					if err != nil {
						tx.Rollback()
						fmt.Println(err)
						return
					}
				}
			}
			tx.Commit()
		}
	}

	gorest2.RegisterHandler("/start_job", func(w http.ResponseWriter, r *http.Request) {
		m := map[string]interface{}{}
		jobId := r.FormValue("job_id")
		defaultDbo := gorest2.DboRegistry["default"]
		db, err := defaultDbo.GetConn()
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		query := `SELECT * FROM job WHERE job.STATUS='stopped' AND job.ID=?`
		data, err := gosqljson.QueryDbToMap(db, "", query, jobId)
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		if data == nil || len(data) == 0 {
			m["err"] = jobId + " not found."
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		job := data[0]
		mode := job["MODE"]
		cron := job["CRON"]
		jobRuntimeId, err := jobsCron.AddFunc(cron, jobModes[mode](job))
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		jobStatus[jobId] = jobRuntimeId

		rowsAffected, err := gosqljson.ExecDb(db, "UPDATE job SET STATUS=? WHERE ID=?", "0", jobId)
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		if rowsAffected == 1 {
			m["data"] = jobId
		}

		jsonData, err := json.Marshal(m)
		if err != nil {
			fmt.Println(err)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(err.Error()))
			return
		}
		jsonString := string(jsonData)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprint(w, jsonString)
	})

	gorest2.RegisterHandler("/stop_job", func(w http.ResponseWriter, r *http.Request) {
		m := map[string]interface{}{}
		jobId := r.FormValue("job_id")
		if jobRuntimeId, ok := jobStatus[jobId]; ok {
			jobsCron.RemoveFunc(jobRuntimeId)
			delete(jobStatus, jobId)
		} else {
			m["err"] = jobId + " not found."
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		defaultDbo := gorest2.DboRegistry["default"]
		db, err := defaultDbo.GetConn()
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		rowsAffected, err := gosqljson.ExecDb(db, "UPDATE job SET STATUS=? WHERE ID=?", "stopped", jobId)
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
			jsonData, err := json.Marshal(m)
			if err != nil {
				fmt.Println(err)
			}
			jsonString := string(jsonData)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, jsonString)
			return
		}
		if rowsAffected == 1 {
			m["data"] = jobId
		}

		jsonData, err := json.Marshal(m)
		if err != nil {
			fmt.Println(err)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(err.Error()))
			return
		}
		jsonString := string(jsonData)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprint(w, jsonString)
	})
}

var jobsCron = cron.New()
var jobModes = make(map[string]func(map[string]string) func())
var jobStatus = make(map[string]int)

var startJobs = func() {
	defaultDbo := gorest2.DboRegistry["default"]
	db, err := defaultDbo.GetConn()
	if err != nil {
		fmt.Println(err)
		return
	}

	query := `SELECT * FROM job WHERE job.STATUS='0'`
	data, err := gosqljson.QueryDbToMap(db, "", query)
	if err != nil {
		fmt.Println(err)
		return
	}
	if data == nil || len(data) == 0 {
		return
	}
	for _, job := range data {
		jobId := job["ID"]
		mode := job["MODE"]
		cron := job["CRON"]
		jobRuntimeId, err := jobsCron.AddFunc(cron, jobModes[mode](job))
		if err != nil {
			fmt.Println(err)
			continue
		}
		jobStatus[jobId] = jobRuntimeId
	}
}
