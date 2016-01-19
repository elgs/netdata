package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	_ "github.com/go-sql-driver/mysql"
	"github.com/satori/go.uuid"
	"gopkg.in/redis.v3"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"time"
)

var makeGetDbo = func(dbType string) func(id string) gorest2.DataOperator {
	return func(id string) gorest2.DataOperator {
		ret := gorest2.DboRegistry[id]
		if ret != nil {
			return ret
		}
		defaultDbo := gorest2.DboRegistry["default"]
		db, err := defaultDbo.GetConn()
		if err != nil {
			fmt.Println(err)
			return nil
		}
		query := `SELECT data_store.*, 
			CONCAT_WS('_','nd',project.PROJECT_KEY) AS DB,project.ID AS PROJECT_ID,project.PROJECT_KEY FROM project
			INNER JOIN data_store ON project.DATA_STORE_NAME=data_store.DATA_STORE_NAME WHERE project.ID=?`
		data, err := gosqljson.QueryDbToMap(db, "", query, id)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		if data == nil || len(data) == 0 {
			return nil
		}
		dboData := data[0]
		ds := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", dboData["PROJECT_KEY"], dboData["PROJECT_ID"],
			dboData["HOST"], dboData["PORT"], dboData["DB"])
		ret = NewDbo(ds, dbType)
		gorest2.DboRegistry[id] = ret
		return ret
	}
}

var grConfig gorest2.Gorest

var pushNode bool = false
var jobNode bool = false

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	input := args()
	grConfig = parseConfig(input[0])
	if grConfig == nil {
		return
	}

	redisMasterAddress := grConfig["redis_master_address"].(string)
	redisMasterPassword := grConfig["redis_master_password"].(string)
	gorest2.RedisMaster = redis.NewClient(&redis.Options{
		Addr:     redisMasterAddress,
		Password: redisMasterPassword,
	})
	_, err := gorest2.RedisMaster.Ping().Result()
	if err != nil {
		fmt.Println(err)
		return
	}

	redisLocalAddress := grConfig["redis_local_address"].(string)
	redisLocalPassword := grConfig["redis_local_password"].(string)
	gorest2.RedisLocal = redis.NewClient(&redis.Options{
		Addr:     redisLocalAddress,
		Password: redisLocalPassword,
	})
	_, err = gorest2.RedisLocal.Ping().Result()
	if err != nil {
		fmt.Println(err)
		return
	}

	ds := grConfig["data_source"].(string)
	dbType := grConfig["db_type"].(string)

	dbo := NewDbo(ds, dbType)

	gorest2.DboRegistry["default"] = dbo
	gorest2.GetDbo = makeGetDbo(dbType)

	pushNode = grConfig["push_node"].(bool)
	if pushNode {
		initCache()
	}
	jobNode = grConfig["job_node"].(bool)
	if jobNode {
		startJobs()
		_, err = loadRequestStats("")
		if err != nil {
			fmt.Println(err)
			return
		}
		err = updateStorageStats()
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	gorest2.RegisterHandler("/api", gorest2.RestFunc)
	gorest2.StartDaemons(dbo)

	grConfig.Serve()
}

func initCache() error {
	//	err := gorest2.RedisMaster.FlushDb().Err()
	//	if err != nil {
	//		return err
	//	}
	err := loadAllRemoteInterceptor()
	if err != nil {
		return err
	}
	return nil
}

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

func parseConfig(configFile string) gorest2.Gorest {
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println(configFile, "not found")
		return nil
	}
	var config gorest2.Gorest
	if err := json.Unmarshal(b, &config); err != nil {
		fmt.Println("Error parsing", configFile)
		return nil
	}
	return config
}

func args() []string {
	ret := []string{}
	if len(os.Args) <= 1 {
		ret = append(ret, "gorest.json")
	} else {
		ret = os.Args[1:]
	}
	return ret
}
