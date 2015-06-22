package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"io/ioutil"
	"math/rand"
	"net/http"
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

	gorest2.RegisterJob("invalidate_token", &gorest2.Job{
		Cron: fmt.Sprint(rand.Intn(60), " * * * * *"),
		MakeAction: func(dbo gorest2.DataOperator) func() {
			lastUpdateSince := ""
			return func() {
				db, err := dbo.GetConn()
				if err != nil {
					fmt.Println(err)
					return
				}

				if lastUpdateSince == "" {
					// minus the interval of this job (1 min) to eliminate the tokens changed before this job first run.
					changedTokens, err := gosqljson.QueryDbToMap(db, "upper",
						"SELECT UPDATE_TIME - INTERVAL 1 MINUTE AS UPDATE_TIME FROM token ORDER BY UPDATE_TIME DESC LIMIT 1")
					if err != nil {
						fmt.Println(err)
						return
					}
					lastChangedToken := changedTokens[0]
					lastUpdateSince = lastChangedToken["UPDATE_TIME"]
				} else {
					changedTokens, err := gosqljson.QueryDbToMap(db, "upper",
						"SELECT TOKEN,UPDATE_TIME FROM token WHERE UPDATE_TIME>? ORDER BY UPDATE_TIME DESC", lastUpdateSince)
					if err != nil {
						fmt.Println(err)
						return
					}
					for i, changedToken := range changedTokens {
						delete(projectTokenRegistry, changedToken["TOKEN"])
						if i == 0 {
							lastUpdateSince = changedToken["UPDATE_TIME"]
						}
					}
				}
			}
		},
	})

	gorest2.RegisterJob("invalidate_query", &gorest2.Job{
		Cron: fmt.Sprint(rand.Intn(60), " * * * * *"),
		MakeAction: func(dbo gorest2.DataOperator) func() {
			lastUpdateSince := ""
			return func() {
				db, err := dbo.GetConn()
				if err != nil {
					fmt.Println(err)
					return
				}

				if lastUpdateSince == "" {
					// minus the interval of this job (1 min) to eliminate the queries changed before this job first run.
					changedQueries, err := gosqljson.QueryDbToMap(db, "upper",
						"SELECT UPDATE_TIME - INTERVAL 1 MINUTE AS UPDATE_TIME FROM query ORDER BY UPDATE_TIME DESC LIMIT 1")
					if err != nil {
						fmt.Println(err)
						return
					}
					lastChangedQuery := changedQueries[0]
					lastUpdateSince = lastChangedQuery["UPDATE_TIME"]
				} else {
					changedQueries, err := gosqljson.QueryDbToMap(db, "upper",
						"SELECT PROJECT_ID,NAME,UPDATE_TIME FROM query WHERE UPDATE_TIME>? ORDER BY UPDATE_TIME DESC", lastUpdateSince)
					if err != nil {
						fmt.Println(err)
						return
					}
					for i, changedQuery := range changedQueries {
						queryName := changedQuery["NAME"]
						appId := changedQuery["PROJECT_ID"]
						dbo := gorest2.GetDbo(appId).(*NdDataOperator)
						delete(dbo.QueryRegistry, queryName)
						if i == 0 {
							lastUpdateSince = changedQuery["UPDATE_TIME"]
						}
					}
				}
			}
		},
	})
}

func httpRequest(url string, method string, data string, apiTokenId string, apiTokenKey string) ([]byte, error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(method, url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		return nil, err
	}
	if apiTokenId != "" {
		req.Header.Add("api_token_id", apiTokenId)
	}
	if apiTokenKey != "" {
		req.Header.Add("api_token_key", apiTokenKey)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	defer tr.CloseIdleConnections()

	return body, err
}
