package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/satori/go.uuid"
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

	// api node -> db
	gorest2.RegisterJob("update_user_request_stats", &gorest2.Job{
		Cron: fmt.Sprint(rand.Intn(60), " * * * * *"),
		MakeAction: func(dbo gorest2.DataOperator) func() {
			return func() {
				db, err := dbo.GetConn()
				if err != nil {
					fmt.Println(err)
					return
				}

				for k, v := range gorest2.RequestReads {
					_, err := gosqljson.ExecDb(db,
						`UPDATE user_stats SET HTTP_READ_USED=HTTP_READ_USED+?,UPDATE_TIME=? WHERE PROJECT_ID=?`,
						v, time.Now().UTC(), k)
					if err != nil {
						fmt.Println(err)
					}
					gorest2.RequestReads[k] = 0
				}
				for k, v := range gorest2.RequestWrites {
					_, err := gosqljson.ExecDb(db,
						`UPDATE user_stats SET HTTP_WRITE_USED=HTTP_WRITE_USED+?,UPDATE_TIME=? WHERE PROJECT_ID=?`,
						v, time.Now().UTC(), k)
					if err != nil {
						fmt.Println(err)
					}
					gorest2.RequestWrites[k] = 0
				}
			}
		},
	})

	// update db and cache
	gorest2.RegisterJob("load_user_stats_to_cache", &gorest2.Job{
		Cron: "0 * * * * *",
		MakeAction: func(dbo gorest2.DataOperator) func() {
			return func() {
				if !jobNode {
					return
				}
				_, err := loadRequestStats("")
				if err != nil {
					fmt.Println(err)
				}
				err = updateStorageStats()
				if err != nil {
					fmt.Println(err)
				}
				err = updateJobStats()
				if err != nil {
					fmt.Println(err)
				}
				err = updateRIStats()
				if err != nil {
					fmt.Println(err)
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
				rowAffected, err := gosqljson.ExecDb(db, `UPDATE push_notification SET STATUS=? WHERE STATUS=0 ORDER BY CREATE_TIME`, statusId)
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
}
