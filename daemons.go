package main

import (
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/satori/go.uuid"
	"math/rand"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())

	gorest2.RegisterJob("dummy", &gorest2.Job{
		Cron: "0 0 * * * *",
		MakeAction: func(dbo gorest2.DataOperator) func() {
			return func() {
			}
		},
	})

	gorest2.RegisterJob("user_stats", &gorest2.Job{
		Cron: "0 * * * * *",
		MakeAction: func(dbo gorest2.DataOperator) func() {
			return func() {
				if !jobNode {
					return
				}

				db, err := dbo.GetConn()
				if err != nil {
					fmt.Println(err)
					return
				}
				projectArray, err := gosqljson.QueryDbToMap(db, "", "SELECT * FROM project WHERE STATUS=0")
				if err != nil {
					fmt.Println(err)
					return
				}
				for _, project := range projectArray {
					projectId := project["ID"]
					projectKey := project["PROJECT_KEY"]
					val, err := gorest2.RedisMaster.HGetAllMap("stats:" + projectId).Result()
					if err != nil {
						fmt.Println(err)
						return
					}
					if val == nil || len(val) == 0 { // not found in cache
						// insert ignore to user_stats
						userStats := map[string]interface{}{
							"ID":               strings.Replace(uuid.NewV4().String(), "-", "", -1),
							"PROJECT_ID":       projectId,
							"PROJECT_KEY":      projectKey,
							"STORAGE_USED":     0,
							"STORAGE_TOTAL":    1 << 30, // 1G
							"HTTP_WRITE_USED":  0,
							"HTTP_WRITE_TOTAL": 50000,
							"HTTP_READ_USED":   0,
							"HTTP_READ_TOTAL":  500000,
							"CREATOR_ID":       "",
							"CREATOR_CODE":     "",
							"CREATE_TIME":      time.Now().UTC(),
							"UPDATER_ID":       "",
							"UPDATER_CODE":     "",
							"UPDATE_TIME":      time.Now().UTC(),
						}

						_, err := DbInsert(db, "user_stats", userStats, true, false)
						if err != nil {
							fmt.Println(err)
							return
						}
						// load back to cache
						loadStats(projectId)
					} else { // found in cache
						// try to insert into user_stats
						httpWirte := val["http_write"]
						httpRead := val["http_read"]

						rowsAffected, err := gosqljson.ExecDb(db, "UPDATE user_stats SET HTTP_WRITE_USED=?, HTTP_READ_USED=? WHERE PROJECT_ID=?", httpWirte, httpRead, projectId)
						if err != nil {
							fmt.Println(err)
							return
						}
						if rowsAffected == 0 {
							//user_stats not found
							userStats := map[string]interface{}{
								"ID":               strings.Replace(uuid.NewV4().String(), "-", "", -1),
								"PROJECT_ID":       projectId,
								"PROJECT_KEY":      projectKey,
								"STORAGE_USED":     0,
								"STORAGE_TOTAL":    1 << 30, // 1G
								"HTTP_WRITE_USED":  httpWirte,
								"HTTP_WRITE_TOTAL": 50000,
								"HTTP_READ_USED":   httpRead,
								"HTTP_READ_TOTAL":  500000,
								"CREATOR_ID":       "",
								"CREATOR_CODE":     "",
								"CREATE_TIME":      time.Now().UTC(),
								"UPDATER_ID":       "",
								"UPDATER_CODE":     "",
								"UPDATE_TIME":      time.Now().UTC(),
							}
							_, err = DbInsert(db, "user_stats", userStats, true, false)
							if err != nil {
								fmt.Println(err)
								return
							}
						}
					}
					// remove orphans in users_stats
					_, err = gosqljson.ExecDb(db, "DELETE FROM user_stats WHERE PROJECT_ID NOT IN (SELECT ID FROM PROJECT)")
					if err != nil {
						fmt.Println(err)
						return
					}
				}
			}
		},
	})

	gorest2.RegisterJob("send_push_notifications", &gorest2.Job{
		Cron: "*/5 * * * * *",
		MakeAction: func(dbo gorest2.DataOperator) func() {
			return func() {
				if !pushNode {
					return
				}
				statusId := uuid.NewV4().String()
				db, err := dbo.GetConn()
				if err != nil {
					fmt.Println(err)
					return
				}
				rowAffected, err := gosqljson.ExecDb(db, `UPDATE push_notification SET STATUS=? WHERE STATUS=0 ORDER BY CREATE_TIME LIMIT 100`, statusId)
				if err != nil {
					fmt.Println(err)
					return
				}
				if rowAffected == 0 {
					return
				}
				data, err := gosqljson.QueryDbToMap(db, "", `SELECT * FROM push_notification WHERE STATUS=?`, statusId)
				if err != nil {
					fmt.Println(err)
					return
				}
				dataLen := len(data)
				good := make([]interface{}, 0, dataLen/2+1)
				bad := make([]interface{}, 0, dataLen/2+1)

				c := make(chan int, dataLen)
				for _, v := range data {
					go func(v map[string]string) {
						testUrl := strings.ToLower(v["url"])
						if strings.Contains(testUrl, "://localhost") ||
							strings.Contains(testUrl, "netdata.io") ||
							strings.Contains(testUrl, "://127.0.") {
							bad = append(bad, v["ID"])
							c <- 1
							return
						}
						_, statusCode, err := httpRequest(v["URL"], v["METHOD"], v["DATA"], 0)
						if err == nil && statusCode == 200 {
							good = append(good, v["ID"])
						} else {
							bad = append(bad, v["ID"])
						}
						c <- 1
					}(v)
				}
				for i := 0; i < dataLen; i++ {
					<-c
				}
				if len(good) > 0 {
					gosqljson.ExecDb(db, fmt.Sprintf(`UPDATE push_notification SET STATUS=-1, UPDATE_TIME=CONVERT_TZ(NOW(),'System','+0:0') WHERE ID IN(%v)`, GeneratePlaceholders(len(good))), good...)
				}
				if len(bad) > 0 {
					gosqljson.ExecDb(db, fmt.Sprintf(`UPDATE push_notification SET STATUS=1, UPDATE_TIME=CONVERT_TZ(NOW(),'System','+0:0') WHERE ID IN(%v)`, GeneratePlaceholders(len(bad))), bad...)
				}
			}
		},
	})

	//	gorest2.RegisterJob("invalidate_token", &gorest2.Job{
	//		Cron: fmt.Sprint(rand.Intn(60), " * * * * *"),
	//		MakeAction: func(dbo gorest2.DataOperator) func() {
	//			lastUpdateSince := ""
	//			return func() {
	//				db, err := dbo.GetConn()
	//				if err != nil {
	//					fmt.Println(err)
	//					return
	//				}

	//				if lastUpdateSince == "" {
	//					// minus the interval of this job (1 min) to eliminate the tokens changed before this job first run.
	//					changedTokens, err := gosqljson.QueryDbToMap(db, "upper",
	//						"SELECT UPDATE_TIME - INTERVAL 1 MINUTE AS UPDATE_TIME FROM token ORDER BY UPDATE_TIME DESC LIMIT 1")
	//					if err != nil {
	//						fmt.Println(err)
	//						return
	//					}
	//					if len(changedTokens) == 0 {
	//						return
	//					}
	//					lastChangedToken := changedTokens[0]
	//					lastUpdateSince = lastChangedToken["UPDATE_TIME"]
	//				} else {
	//					changedTokens, err := gosqljson.QueryDbToMap(db, "upper",
	//						"SELECT TOKEN,UPDATE_TIME FROM token WHERE UPDATE_TIME>? ORDER BY UPDATE_TIME DESC", lastUpdateSince)
	//					if err != nil {
	//						fmt.Println(err)
	//						return
	//					}
	//					for i, changedToken := range changedTokens {
	//						delete(projectTokenRegistry, changedToken["TOKEN"])
	//						if i == 0 {
	//							lastUpdateSince = changedToken["UPDATE_TIME"]
	//						}
	//					}
	//				}
	//			}
	//		},
	//	})

	//	gorest2.RegisterJob("invalidate_query", &gorest2.Job{
	//		Cron: fmt.Sprint(rand.Intn(60), " * * * * *"),
	//		MakeAction: func(dbo gorest2.DataOperator) func() {
	//			lastUpdateSince := ""
	//			return func() {
	//				db, err := dbo.GetConn()
	//				if err != nil {
	//					fmt.Println(err)
	//					return
	//				}

	//				if lastUpdateSince == "" {
	//					// minus the interval of this job (1 min) to eliminate the queries changed before this job first run.
	//					changedQueries, err := gosqljson.QueryDbToMap(db, "upper",
	//						"SELECT UPDATE_TIME - INTERVAL 1 MINUTE AS UPDATE_TIME FROM query ORDER BY UPDATE_TIME DESC LIMIT 1")
	//					if err != nil {
	//						fmt.Println(err)
	//						return
	//					}
	//					if len(changedQueries) == 0 {
	//						return
	//					}
	//					lastChangedQuery := changedQueries[0]
	//					lastUpdateSince = lastChangedQuery["UPDATE_TIME"]
	//				} else {
	//					changedQueries, err := gosqljson.QueryDbToMap(db, "upper",
	//						"SELECT PROJECT_ID,NAME,UPDATE_TIME FROM query WHERE UPDATE_TIME>? ORDER BY UPDATE_TIME DESC", lastUpdateSince)
	//					if err != nil {
	//						fmt.Println(err)
	//						return
	//					}
	//					for i, changedQuery := range changedQueries {
	//						queryName := changedQuery["NAME"]
	//						appId := changedQuery["PROJECT_ID"]
	//						dbo := gorest2.GetDbo(appId).(*NdDataOperator)
	//						delete(dbo.QueryRegistry, queryName)
	//						if i == 0 {
	//							lastUpdateSince = changedQuery["UPDATE_TIME"]
	//						}
	//					}
	//				}
	//			}
	//		},
	//	})

	//	gorest2.RegisterJob("update_remote_interceptor", &gorest2.Job{
	//		Cron: fmt.Sprint(rand.Intn(60), " * * * * *"),
	//		MakeAction: func(dbo gorest2.DataOperator) func() {
	//			lastUpdateSince := ""
	//			return func() {
	//				db, err := dbo.GetConn()
	//				if err != nil {
	//					fmt.Println(err)
	//					return
	//				}

	//				if lastUpdateSince == "" {
	//					// minus the interval of this job (1 min) to eliminate the remote_interceptor changed before this job first run.
	//					changedRIs, err := gosqljson.QueryDbToMap(db, "upper",
	//						"SELECT UPDATE_TIME - INTERVAL 1 MINUTE AS UPDATE_TIME FROM remote_interceptor ORDER BY UPDATE_TIME DESC LIMIT 1")
	//					if err != nil {
	//						fmt.Println(err)
	//						return
	//					}
	//					if len(changedRIs) == 0 {
	//						return
	//					}
	//					lastChangedToken := changedRIs[0]
	//					lastUpdateSince = lastChangedToken["UPDATE_TIME"]
	//				} else {
	//					changedRIs, err := gosqljson.QueryDbToMap(db, "upper",
	//						"SELECT * FROM remote_interceptor WHERE UPDATE_TIME>? ORDER BY UPDATE_TIME DESC", lastUpdateSince)
	//					if err != nil {
	//						fmt.Println(err)
	//						return
	//					}
	//					for i, changedRI := range changedRIs {
	//						projectId := changedRI["PROJECT_ID"]
	//						target := changedRI["TARGET"]
	//						method := changedRI["METHOD"]
	//						url := changedRI["URL"]
	//						theType := changedRI["TYPE"]
	//						actionType := changedRI["ACTION_TYPE"]
	//						ri := &RemoteInterceptorDefinition{
	//							ProjectId:  projectId,
	//							Target:     target,
	//							Type:       theType,
	//							ActionType: actionType,
	//							Method:     method,
	//							Url:        url,
	//						}
	//						RemoteInterceptorRegistry[fmt.Sprint(projectId, target, theType, actionType)] = ri
	//						if i == 0 {
	//							lastUpdateSince = changedRI["UPDATE_TIME"]
	//						}
	//					}
	//				}
	//			}
	//		},
	//	})
}

//func httpRequest(url string, method string, data string, apiTokenId string, apiTokenKey string) ([]byte, error) {
//	defer func() {
//		if err := recover(); err != nil {
//			fmt.Println(err)
//		}
//	}()

//	tr := &http.Transport{
//		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
//	}
//	client := &http.Client{Transport: tr}
//	req, err := http.NewRequest(method, url, bytes.NewBuffer([]byte(data)))
//	if err != nil {
//		return nil, err
//	}
//	if apiTokenId != "" {
//		req.Header.Add("api_token_id", apiTokenId)
//	}
//	if apiTokenKey != "" {
//		req.Header.Add("api_token_key", apiTokenKey)
//	}

//	res, err := client.Do(req)
//	if err != nil {
//		return nil, err
//	}
//	body, err := ioutil.ReadAll(res.Body)
//	defer res.Body.Close()
//	defer tr.CloseIdleConnections()

//	return body, err
//}
