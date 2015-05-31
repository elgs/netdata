package main

import (
	"encoding/json"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"runtime"
)

var grConfig gorest2.Gorest

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	input := args()
	grConfig = parseConfig(input[0])
	if grConfig == nil {
		return
	}
	ds := grConfig["data_source"].(string)
	dbType := grConfig["db_type"].(string)

	dbo := &gorest2.MySqlDataOperator{
		Ds:     ds,
		DbType: dbType,
	}

	gorest2.DboRegistry["default"] = dbo
	gorest2.GetDbo = func(id string) gorest2.DataOperator {
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
		query := `SELECT data_store.*, REPLACE(project.ID,'-','') AS DB FROM project
		INNER JOIN data_store ON project.DATA_STORE_NAME=data_store.DATA_STORE_NAME
		WHERE project.ID=?`
		data, err := gosqljson.QueryDbToMap(db, "", query, id)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		if data == nil || len(data) == 0 {
			return nil
		}
		dboData := data[0]
		ds := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", dboData["USERNAME"], dboData["PASSWORD"],
			dboData["HOST"], dboData["PORT"], dboData["DB"])
		ret = &gorest2.MySqlDataOperator{
			Ds:     ds,
			DbType: "mysql",
		}
		gorest2.DboRegistry[id] = ret
		return ret
	}

	gorest2.RegisterHandler("/api", gorest2.RestFunc)
	gorest2.StartDaemons(dbo)

	grConfig.Serve()
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
