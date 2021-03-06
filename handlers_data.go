// handlers
package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elgs/gorest2"
	"github.com/elgs/gosplitargs"
	"github.com/elgs/gosqljson"
	"github.com/gorilla/websocket"
	"github.com/satori/go.uuid"
)

var wsMsgQueue = make(chan interface{}, 100)

func init() {

	var connections = make(map[*websocket.Conn]bool)
	var sendAll = func(msg interface{}) {
		for conn := range connections {
			if err := conn.WriteJSON(msg); err != nil {
				delete(connections, conn)
				return
			}
		}
	}
	gorest2.RegisterHandler("/sys/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()
		connections[conn] = true

		go func(c *websocket.Conn) {
			for {
				if _, _, err := c.NextReader(); err != nil {
					c.Close()
					break
				}
			}
		}(conn)

		for data := range wsMsgQueue {
			sendAll(data)
		}
	})

	gorest2.RegisterHandler("/download_csv", func(w http.ResponseWriter, r *http.Request) {
		sql := r.FormValue("sql")
		name := r.FormValue("name")

		projectId := r.FormValue("app_id")
		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid app."}`)
			return
		}

		if projectId == "default" {
			token := r.Header.Get("token")
			if !isDevToken(token) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				http.Error(w, `{"err":"Access denied."}`, http.StatusInternalServerError)
				return
			}
		}

		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		headers, data, err := gosqljson.QueryDbToArray(db, "", sql)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Disposition", "attachment; filename="+name+".csv")
		w.Header().Set("Content-Type", "application/octet-stream")

		writer := csv.NewWriter(w)
		writer.Write(headers)
		writer.WriteAll(data)
		writer.Flush()
	})

	gorest2.RegisterHandler("/upload_csv", func(w http.ResponseWriter, r *http.Request) {
		projectId := r.FormValue("app_id")
		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid app."}`)
			return
		}

		if projectId == "default" {
			token := r.Header.Get("token")
			if !isDevToken(token) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				http.Error(w, `{"err":"Access denied."}`, http.StatusInternalServerError)
				return
			}
		}

		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		csvData := r.FormValue("data")
		table := r.FormValue("table")
		reader := csv.NewReader(strings.NewReader(csvData))

		rawCSVdata, err := reader.ReadAll()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(rawCSVdata) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "Invalid data uploaded.")
			return
		}
		header := rawCSVdata[0]
		data := make(map[string]interface{})
		noID := false
		noCraetorId := false
		noCraetorCode := false
		noCraeteTime := false
		noUpdaterId := false
		noUpdaterCode := false
		noUpdateTime := false
		for i, row := range rawCSVdata {
			if i == 0 {
				//Skip header
				continue
			}
			for j, v := range header {
				data[v] = row[j]
			}
			if i == 1 {
				if _, ok := data["ID"]; !ok {
					noID = true
				}
				if _, ok := data["CREATOR_ID"]; !ok {
					noCraetorId = true
				}
				if _, ok := data["CREATOR_CODE"]; !ok {
					noCraetorCode = true
				}
				if _, ok := data["CREATE_TIME"]; !ok {
					noCraeteTime = true
				}
				if _, ok := data["UPDATER_ID"]; !ok {
					noUpdaterId = true
				}
				if _, ok := data["UPDATER_CODE"]; !ok {
					noUpdaterCode = true
				}
				if _, ok := data["UPDATE_TIME"]; !ok {
					noUpdateTime = true
				}
			}
			if noID {
				data["ID"] = strings.Replace(uuid.NewV4().String(), "-", "", -1)
			}
			if noCraetorId {
				data["CREATOR_ID"] = ""
			}
			if noCraetorCode {
				data["CREATOR_CODE"] = ""
			}
			if noCraeteTime {
				data["CREATE_TIME"] = time.Now().UTC()
			}
			if noUpdaterId {
				data["UPDATER_ID"] = ""
			}
			if noUpdaterCode {
				data["UPDATER_CODE"] = ""
			}
			if noUpdateTime {
				data["UPDATE_TIME"] = time.Now().UTC()
			}
			_, err := DbInsert(db, table, data, false, false)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		fmt.Fprint(w, "Data loaded.")
	})

	gorest2.RegisterHandler("/exec", func(w http.ResponseWriter, r *http.Request) {
		projectId := r.Header.Get("app_id")
		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid app."}`)
			return
		}

		if projectId == "default" {
			token := r.Header.Get("token")
			if !isDevToken(token) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				http.Error(w, `{"err":"Access denied."}`, http.StatusInternalServerError)
				return
			}
		}

		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		userSql := r.FormValue("sql")
		sqlNormalize(&userSql)
		sqls, err := gosplitargs.SplitArgs(userSql, ";", true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid sql."}`)
			return
		}

		if projectId == "default" {
			token := r.Header.Get("token")
			if !isDevToken(token) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				http.Error(w, `{"err":"Access denied."}`, http.StatusInternalServerError)
				return
			}
		}

		ms := make([]map[string]interface{}, 0, len(sqls))

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, sql := range sqls {
			if strings.TrimSpace(sql) == "" {
				continue
			}
			m := map[string]interface{}{}

			rowsAffected, err := exec(tx, sql)
			m["rowsAffected"] = rowsAffected

			if err != nil {
				tx.Rollback()
				m["err"] = err.Error()
				fmt.Println(err)
			}
			ms = append(ms, m)
		}
		tx.Commit()

		jsonData, err := json.Marshal(ms)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonString := string(jsonData)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprint(w, jsonString)
	})

	gorest2.RegisterHandler("/query", func(w http.ResponseWriter, r *http.Request) {
		sql := r.FormValue("sql")
		pageNumber, err := strconv.ParseInt(r.FormValue("page"), 10, 0)
		if err != nil || pageNumber < 1 {
			pageNumber = 1
		}
		pageSize, err := strconv.ParseInt(r.FormValue("limit"), 10, 0)
		if err != nil {
			pageSize = 1000
		}

		order := r.FormValue("sort")
		dir := r.FormValue("dir")
		mode := r.FormValue("mode") // header, data

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid app."}`)
			return
		}
		if projectId == "default" {
			token := r.Header.Get("token")
			if !isDevToken(token) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				http.Error(w, `{"err":"Access denied."}`, http.StatusInternalServerError)
				return
			}
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tx, err := db.Begin()

		m, err := query(tx, sql, pageNumber, pageSize, order, dir, mode)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else {
			tx.Commit()
		}

		jsonData, err := json.Marshal(m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonString := string(jsonData)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprint(w, jsonString)
	})

	gorest2.RegisterHandler("/query_all", func(w http.ResponseWriter, r *http.Request) {
		pageNumber, err := strconv.ParseInt(r.FormValue("page"), 10, 0)
		if err != nil || pageNumber < 1 {
			pageNumber = 1
		}
		pageSize, err := strconv.ParseInt(r.FormValue("limit"), 10, 0)
		if err != nil {
			pageSize = 1000
		}

		userSql := r.FormValue("sql")
		sqlNormalize(&userSql)
		sqls, err := gosplitargs.SplitArgs(userSql, ";", true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		order := r.FormValue("sort")
		dir := r.FormValue("dir")
		mode := r.FormValue("mode") // header, data
		ms := make([]map[string]interface{}, 0, len(sqls))
		projectId := r.Header.Get("app_id")
		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid app."}`)
			return
		}
		if projectId == "default" {
			token := r.Header.Get("token")
			if !isDevToken(token) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				http.Error(w, `{"err":"Access denied."}`, http.StatusInternalServerError)
				return
			}
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, sql := range sqls {
			if strings.TrimSpace(sql) == "" {
				continue
			}
			if isQuery(sql) {
				m, err := query(tx, sql, pageNumber, pageSize, order, dir, mode)
				if err != nil {
					tx.Rollback()
					m = map[string]interface{}{}
					m["err"] = err.Error()
					m["sql"] = sql
					fmt.Println(err)
					ms = append(ms, m)
					break
				}
				ms = append(ms, m)
			} else {
				m := map[string]interface{}{}
				rowsAffected, err := exec(tx, sql)
				if err != nil {
					tx.Rollback()
					m["err"] = err.Error()
					m["sql"] = sql
					fmt.Println(err)
					ms = append(ms, m)
					break
				}
				m["rowsAffected"] = rowsAffected
				m["sql"] = sql
				ms = append(ms, m)
			}
		}
		tx.Commit()
		jsonData, err := json.Marshal(ms)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		jsonString := string(jsonData)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprint(w, jsonString)
	})
}

func sqlNormalize(sql *string) {
	*sql = strings.TrimSpace(*sql)
	var ret string
	lines := strings.Split(*sql, "\n")
	for _, line := range lines {
		lineTrimmed := strings.TrimSpace(line)
		if lineTrimmed != "" && !strings.HasPrefix(lineTrimmed, "-- ") {
			ret += line + "\n"
		}
	}
	*sql = ret
}

func isQuery(sql string) bool {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(sqlUpper, "SELECT") ||
		strings.HasPrefix(sqlUpper, "SHOW") ||
		strings.HasPrefix(sqlUpper, "DESCRIBE") ||
		strings.HasPrefix(sqlUpper, "EXPLAIN") {
		return true
	}
	return false
}

type ByName [][]string

func (this ByName) Len() int {
	return len(this)
}
func (this ByName) Less(i, j int) bool {
	return this[i][len(this[i])-1] < this[j][len(this[j])-1]
}
func (this ByName) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func query(tx *sql.Tx, sql string, pageNumber int64, pageSize int64, order string, dir string, mode string) (map[string]interface{}, error) {
	sqlNormalize(&sql)

	m := make(map[string]interface{})

	if strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		if mode == "header" {
			expMap, err := gosqljson.QueryTxToMap(tx, "", `EXPLAIN `+sql)
			if err != nil {
				return nil, err
			}
			if len(expMap) == 1 {
				m["table"] = expMap[0]["table"]
			}

			headers, _, err := gosqljson.QueryTxToArray(tx, "", `SELECT * FROM (`+sql+`)a LIMIT 0`)
			if err != nil {
				return nil, err
			}
			m["headers"] = headers
			m["sql"] = sql
		} else {

			orderBy := ""
			if order != "" && dir != "" {
				orderBy = "ORDER BY " + order + " " + dir
			}

			headers, dataArray, err := gosqljson.QueryTxToArray(tx, "", `SELECT SQL_CALC_FOUND_ROWS * FROM (`+sql+`)a `+orderBy+` LIMIT ?,?`,
				(pageNumber-1)*pageSize, pageSize)
			if err != nil {
				return nil, err
			}

			totalRowsMap, err := gosqljson.QueryTxToMap(tx, "", `SELECT FOUND_ROWS()`)
			if err != nil {
				return nil, err
			}
			totalRows, err := strconv.ParseInt(totalRowsMap[0]["FOUND_ROWS()"], 10, 0)
			if err != nil {
				totalRows = 0
			}

			totalPages := int64(math.Ceil(float64(totalRows) / float64(pageSize)))

			if pageNumber > totalPages {
				pageNumber = totalPages
			}

			m["headers"] = headers
			m["data_array"] = dataArray
			m["total_rows"] = totalRows
			m["total_pages"] = totalPages
			m["page_number"] = pageNumber
			m["page_size"] = pageSize
			m["sql"] = sql
		}
	} else {
		headers, dataArray, err := gosqljson.QueryTxToArray(tx, "", sql)
		if err != nil {
			return nil, err
		}
		if mode == "header" {
			m["headers"] = headers
			m["sql"] = sql
		} else {
			if order != "" && dir != "" {
				sortIndex := -1
				for i, v := range headers {
					if v == order {
						sortIndex = i
						break
					}
				}
				if sortIndex == -1 {
					return nil, err
				} else {
					for _, data := range dataArray {
						data = append(data, data[sortIndex])
					}
					if dir == "ASC" {
						sort.Sort(ByName(dataArray))
					} else if dir == "DESC" {
						sort.Sort(sort.Reverse(ByName(dataArray)))
					}
					for _, data := range dataArray {
						data = data[:len(data)-1]
					}
				}
			}
			totalRows := len(dataArray)
			pageEndNumber := pageNumber * pageSize
			if pageEndNumber > int64(totalRows) {
				pageEndNumber = int64(totalRows)
			}
			totalPages := int64(math.Ceil(float64(totalRows) / float64(pageSize)))
			m["headers"] = headers
			m["data_array"] = dataArray[(pageNumber-1)*pageSize : pageEndNumber]
			m["total_rows"] = totalRows
			m["total_pages"] = totalPages
			m["page_number"] = pageNumber
			m["page_size"] = pageSize
			m["sql"] = sql
		}

	}
	return m, nil
}

func exec(tx *sql.Tx, sql string) (int64, error) {
	sqlNormalize(&sql)
	rowsAffected, err := gosqljson.ExecTx(tx, sql)
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func isDevToken(token string) bool {
	key := fmt.Sprint("dtoken:", token)
	roles := gorest2.RedisLocal.HGet(key, "ROLES").Val()
	roleArray := strings.Split(roles, ",")
	for _, role := range roleArray {
		if role == "dev" {
			return true
		}
	}
	return false
}
