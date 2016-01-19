package main

import (
	"database/sql"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/satori/go.uuid"
	"strings"
	"time"
)

//update db
func updateStorageStats() error {
	defaultDbo := gorest2.DboRegistry["default"]
	db, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	dataStoreArray, err := gosqljson.QueryDbToMap(db, "", "SELECT * FROM data_store")
	if err != nil {
		return err
	}
	for _, dataStore := range dataStoreArray {
		ds := fmt.Sprintf("%v:%v@tcp(%v:%v)/", dataStore["USERNAME"], dataStore["PASSWORD"],
			dataStore["HOST"], dataStore["PORT"])
		projectDb, err := sql.Open("mysql", ds)
		defer projectDb.Close()
		if err != nil {
			fmt.Println(err)
			continue
		}
		_, data, err := gosqljson.QueryDbToArray(projectDb, "", `SELECT SUBSTRING(TABLE_SCHEMA FROM 4), SUM(DATA_LENGTH+INDEX_LENGTH)
			FROM information_schema.tables WHERE TABLE_SCHEMA LIKE 'nd\_%' GROUP BY TABLE_SCHEMA;`)
		if err != nil {
			fmt.Println(err)
			continue
		}
		for _, v := range data {
			projectKey := v[0]
			storageUsed := v[1]
			_, err = gosqljson.ExecDb(db,
				`UPDATE user_stats SET STORAGE_USED=?,UPDATE_TIME=? WHERE PROJECT_KEY=?`, storageUsed, time.Now().UTC(), projectKey)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}
	return nil
}

// update db and cache
func loadRequestStats(projectId string) (int, error) {
	if projectId == "" {
		projectId = "%"
	}
	defaultDbo := gorest2.DboRegistry["default"]
	db, err := defaultDbo.GetConn()
	if err != nil {
		return 0, err
	}

	projectArray, err := gosqljson.QueryDbToMap(db, "", "SELECT * FROM project WHERE STATUS=0 AND ID LIKE ?", projectId)
	if err != nil {
		return 0, err
	}
	for _, project := range projectArray {
		projectId := project["ID"]
		projectKey := project["PROJECT_KEY"]
		projectName := project["NAME"]

		// insert ignore into user_stats
		userStats := map[string]interface{}{
			"ID":                  strings.Replace(uuid.NewV4().String(), "-", "", -1),
			"PROJECT_ID":          projectId,
			"PROJECT_KEY":         projectKey,
			"PROJECT_NAME":        projectName,
			"STORAGE_USED":        0,
			"STORAGE_TOTAL":       (1 << 30) * 10, // 10G
			"HTTP_WRITE_USED":     0,
			"HTTP_READ_USED":      0,
			"HTTP_REQUESTS_TOTAL": 10000000,
			"JOBS_USED":           0,
			"JOBS_TOTAL":          1,
			"RI_USED":             0,
			"RI_TOTAL":            1,
			"CREATOR_ID":          "",
			"CREATOR_CODE":        "",
			"CREATE_TIME":         time.Now().UTC(),
			"UPDATER_ID":          "",
			"UPDATER_CODE":        "",
			"UPDATE_TIME":         time.Now().UTC(),
		}

		_, err := DbInsert(db, "user_stats", userStats, true, false)
		if err != nil {
			fmt.Println(err)
			continue
		}

		// remove orphans in users_stats
		_, err = gosqljson.ExecDb(db, "DELETE FROM user_stats WHERE PROJECT_ID NOT IN (SELECT ID FROM project)")
		if err != nil {
			fmt.Println(err)
			continue
		}

		// update project name
		_, err = gosqljson.ExecDb(db, `UPDATE user_stats,project SET user_stats.PROJECT_NAME=project.NAME 
			WHERE user_stats.PROJECT_ID=project.ID`)
		if err != nil {
			fmt.Println(err)
			continue
		}
	}

	userStatsArray, err := gosqljson.QueryDbToMap(db, "", `SELECT * FROM user_stats 
		WHERE PROJECT_ID IN (SELECT ID FROM project) AND PROJECT_ID LIKE ?`, projectId)
	if err != nil {
		return 0, err
	}
	for _, userStats := range userStatsArray {
		projectId := userStats["PROJECT_ID"]
		storageUsed := userStats["STORAGE_USED"]
		storageTotal := userStats["STORAGE_TOTAL"]
		httpWriteUsed := userStats["HTTP_WRITE_USED"]
		httpReadUsed := userStats["HTTP_READ_USED"]
		httpRequestsTotal := userStats["HTTP_REQUESTS_TOTAL"]
		err = gorest2.RedisMaster.HMSet("stats:"+projectId,
			"storage_used", storageUsed,
			"storage_total", storageTotal,
			"http_write_used", httpWriteUsed,
			"http_read_used", httpReadUsed,
			"http_requests_total", httpRequestsTotal).Err()
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
	return len(userStatsArray), nil

}

func updateJobStats() error {
	defaultDbo := gorest2.DboRegistry["default"]
	db, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	_, err = gosqljson.ExecDb(db, `UPDATE user_stats,(SELECT PROJECT_ID, COUNT(*) AS JOBS_USED FROM job GROUP BY PROJECT_ID) AS A 
		SET user_stats.JOBS_USED=A.JOBS_USED WHERE user_stats.PROJECT_ID=A.PROJECT_ID`)
	if err != nil {
		return err
	}
	return nil
}

func updateRIStats() error {
	defaultDbo := gorest2.DboRegistry["default"]
	db, err := defaultDbo.GetConn()
	if err != nil {
		return err
	}
	_, err = gosqljson.ExecDb(db, `UPDATE user_stats,(SELECT PROJECT_ID, COUNT(*) AS RI_USED FROM remote_interceptor GROUP BY PROJECT_ID) AS A 
		SET user_stats.RI_USED=A.RI_USED WHERE user_stats.PROJECT_ID=A.PROJECT_ID`)
	if err != nil {
		return err
	}
	return nil
}
