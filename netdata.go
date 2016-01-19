package main

import (
	"encoding/json"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/redis.v3"
	"io/ioutil"
	"os"
	"runtime"
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
