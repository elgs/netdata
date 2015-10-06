// handlers
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosplitargs"
	"github.com/elgs/gosqljson"
	"github.com/gorilla/websocket"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
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
			fmt.Println(err)
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
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			fmt.Fprint(w, err.Error())
			return
		}
		headers, data, err := gosqljson.QueryDbToArray(db, "", sql)
		if err != nil {
			fmt.Fprint(w, err.Error())
			return
		}

		w.Header().Set("Content-Disposition", "attachment; filename="+name+".csv")
		w.Header().Set("Content-Type", "application/octet-stream")

		writer := csv.NewWriter(w)
		writer.Write(headers)
		writer.WriteAll(data)
		writer.Flush()
	})

	gorest2.RegisterHandler("/exec", func(w http.ResponseWriter, r *http.Request) {

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid app."}`)
			return
		}
		dbo := gorest2.GetDbo(projectId)

		sql := r.FormValue("sql")

		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid sql."}`)
			return
		}
		m := map[string]interface{}{}

		rowsAffected, err := exec(dbo, sql)
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
		}
		m["rowsAffected"] = rowsAffected
		jsonData, err := json.Marshal(m)
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
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
		dbo := gorest2.GetDbo(projectId)
		m, err := query(dbo, sql, pageNumber, pageSize, order, dir, mode)
		if err != nil {
			m["err"] = err.Error()
			fmt.Println(err)
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
		sqls := strings.Split(r.FormValue("sql"), ";")
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
		ms := make([]map[string]interface{}, 0, len(sqls))
		projectId := r.Header.Get("app_id")
		if projectId == "" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, `{"err":"Invalid app."}`)
			return
		}
		dbo := gorest2.GetDbo(projectId)
		for _, sql := range sqls {
			if strings.TrimSpace(sql) == "" {
				continue
			}
			if isQuery(sql) {
				m, err := query(dbo, sql, pageNumber, pageSize, order, dir, mode)
				if err != nil {
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
				rowsAffected, err := exec(dbo, sql)
				if err != nil {
					m["err"] = err.Error()
					m["sql"] = sql
					fmt.Println(err)
					ms = append(ms, m)
					break
				}
				m["rowsAffected"] = rowsAffected
				ms = append(ms, m)
			}
		}

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

func sqlCheck(sql *string) {
	*sql = strings.TrimSpace(*sql)
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

func query(dbo gorest2.DataOperator, sql string, pageNumber int64, pageSize int64, order string, dir string, mode string) (map[string]interface{}, error) {
	sqlCheck(&sql)

	db, err := dbo.GetConn()
	if err != nil {
		return nil, err
	}

	m := make(map[string]interface{})

	if strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		if mode == "header" {
			expMap, err := gosqljson.QueryDbToMap(db, "", `EXPLAIN `+sql)
			if err != nil {
				return nil, err
			}
			if len(expMap) == 1 {
				m["table"] = expMap[0]["table"]
			}

			headers, _, err := gosqljson.QueryDbToArray(db, "", `SELECT * FROM (`+sql+`)a LIMIT 0`)
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

			tx, err := db.Begin()
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
			tx.Commit()

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
		headers, dataArray, err := gosqljson.QueryDbToArray(db, "", sql)
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

func exec(dbo gorest2.DataOperator, sql string) ([]int64, error) {
	db, err := dbo.GetConn()
	if err != nil {
		return []int64{}, err
	}
	tx, err := db.Begin()
	if err != nil {
		return []int64{}, err
	}
	rowsAffectedArray := make([]int64, 0)
	sqls, err := gosplitargs.SplitArgs(sql, ";", true)
	for _, s := range sqls {
		sqlCheck(&s)
		if len(s) == 0 {
			continue
		}
		rowsAffected, err := gosqljson.ExecTx(tx, s)
		if err != nil {
			tx.Rollback()
			return rowsAffectedArray, err
		}
		rowsAffectedArray = append(rowsAffectedArray, rowsAffected)
	}
	tx.Commit()
	return rowsAffectedArray, nil
}
