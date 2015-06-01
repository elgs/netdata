// handlers
package main

import (
	"fmt"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/oschwald/geoip2-golang"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

func init() {

	gorest2.RegisterHandler("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RemoteAddr, "127.0.0.1:") {
			defer func() {
				os.Exit(0)
			}()
		} else {
			fmt.Fprintln(w, "Attack!!!")
		}
	})

	gorest2.RegisterHandler("/get_server", func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			fmt.Fprintf(w, "netdata.io:2015")
			return
		}
		serverAddr := GetNearestServer(host)
		fmt.Fprintf(w, serverAddr)
	})
}

func GetNearestServer(ipStr string) string {
	fmt.Println("client ip:", ipStr)
	if ipStr == "127.0.0.1" || ipStr == "localhost" {
		return "127.0.0.1:1103"
	}

	ret := "netdata.io:2015"
	ip := net.ParseIP(ipStr)

	cityDb, err := geoip2.Open("/root/netdata.io/GeoLite2-City.mmdb")
	if err != nil {
		return ret
	}
	defer cityDb.Close()

	record, err := cityDb.City(ip)
	if err != nil {
		return ret
	}

	city := record.City.Names["en"]
	countryCode := record.Country.IsoCode

	dbo := gorest2.DboRegistry["default"]
	defaultDb, err := dbo.GetConn()
	if err != nil {
		return ret
	}
	serverData, err := gosqljson.QueryDbToMap(defaultDb, "",
		"SELECT SERVER_NAME, SERVER_PORT, REGION FROM server WHERE STATUS='0' AND COUNTRY=?", countryCode)
	if err != nil || len(serverData) == 0 {
		return ret
	}
	if len(serverData) == 1 {
		server := serverData[0]
		ret = fmt.Sprintf("%s:%d", server["SERVER_NAME"], server["SERVER_IP"])
		fmt.Println(ret)
		return ret
	}
	for _, server := range serverData {
		if city == server["REGION"] {
			ret = fmt.Sprintf("%s:%d", server["SERVER_NAME"], server["SERVER_IP"])
			fmt.Println(ret)
			return ret
		}
	}
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.Intn(len(serverData))
	server := serverData[r]
	ret = fmt.Sprintf("%s:%d", server["SERVER_NAME"], server["SERVER_IP"])
	fmt.Println(ret)
	return ret
}
