package main

import (
	"strings"
	//	"bytes"
	//	"crypto/tls"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	//	"io/ioutil"
	"github.com/satori/go.uuid"
	"math/rand"
	//	"net/http"
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

	gorest2.RegisterJob("send_push_notifications", &gorest2.Job{
		Cron: "*/5 * * * * *",
		MakeAction: func(dbo gorest2.DataOperator) func() {
			return func() {
				if !mainNode {
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
						_, _, err = httpRequest(v["URL"], v["METHOD"], v["DATA"])
						if err == nil {
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
					gosqljson.ExecDb(db, fmt.Sprintf(`UPDATE push_notification SET STATUS=-1 WHERE ID IN(%v)`, GeneratePlaceholders(len(good))), good...)
				}
				if len(bad) > 0 {
					gosqljson.ExecDb(db, fmt.Sprintf(`UPDATE push_notification SET STATUS=1 WHERE ID IN(%v)`, GeneratePlaceholders(len(bad))), bad...)
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
