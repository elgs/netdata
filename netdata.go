package main

import (
	"encoding/json"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
)

var grConfig = &gorest2.Gorest{}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	input := args()
	config := parseConfig(input[0])
	if config == nil {
		return
	}
	ds := config["data_source"].(string)
	dbType := config["db_type"].(string)
	fbp := config["file_base_path"]
	u, err := user.Current()
	if err != nil {
		fmt.Println(err)
	}
	fileBasePath, err := filepath.Abs(u.HomeDir + string(os.PathSeparator) + "files")
	if err != nil {
		fmt.Println(err)
	}
	if fbp != nil {
		if !strings.HasPrefix(fbp.(string), string(os.PathSeparator)) {
			fileBasePath, err = filepath.Abs(u.HomeDir + string(os.PathSeparator) + fbp.(string))
			if err != nil {
				fmt.Println(err)
			}
		} else {
			fileBasePath, err = filepath.Abs(fbp.(string))
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	dbo := &gorest2.MySqlDataOperator{
		Ds:     ds,
		DbType: dbType,
	}

	grConfig.FileBasePath = fileBasePath

	if v, ok := config["enable_http"].(bool); ok {
		grConfig.EnableHttp = v
	}
	if v, ok := config["host_http"].(string); ok {
		grConfig.HostHttp = v
	}
	if v, ok := config["port_http"].(float64); ok {
		grConfig.PortHttp = uint16(v)
	}
	if v, ok := config["enable_https"].(bool); ok {
		grConfig.EnableHttps = v
	}
	if v, ok := config["host_https"].(string); ok {
		grConfig.HostHttps = v
	}
	if v, ok := config["port_https"].(float64); ok {
		grConfig.PortHttps = uint16(v)
	}
	if v, ok := config["cert_file_https"].(string); ok {
		grConfig.CertFileHttps = v
	}
	if v, ok := config["key_file_https"].(string); ok {
		grConfig.KeyFileHttps = v
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

func parseConfig(configFile string) map[string]interface{} {
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println(configFile, "not found")
		return nil
	}
	var config map[string]interface{}
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
