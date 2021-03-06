// handlers
package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/oschwald/geoip2-golang"
)

func init() {

	gorest2.RegisterHandler("/sys/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "pong")
	})

	gorest2.RegisterHandler("/sys/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RemoteAddr, "127.0.0.1:") {
			defer func() {
				os.Exit(0)
			}()
		} else {
			fmt.Fprintln(w, "Attack!!!")
		}
	})

	gorest2.RegisterHandler("/sys/get_server", func(w http.ResponseWriter, r *http.Request) {
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
	//	fmt.Println("client ip:", ipStr)
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
	continent := record.Continent.Names["en"]
	var state string
	if len(record.Subdivisions) > 0 {
		state = record.Subdivisions[0].Names["en"]
	}
	//	fmt.Println(strings.Join([]string{city, state, countryCode, continent}, ","))

	dbo := gorest2.DboRegistry["default"]
	defaultDb, err := dbo.GetConn()
	if err != nil {
		return ret
	}
	serverData, err := gosqljson.QueryDbToMap(defaultDb, "",
		`SELECT SERVER_NAME, SERVER_PORT, REGION, COUNTRY, PRIORITY 
		FROM server WHERE STATUS='0' AND (COUNTRY=? OR SUPER_REGION=?)`,
		countryCode, continent)
	if err != nil || len(serverData) == 0 {
		return ret
	}
	//	fmt.Println(serverData)
	if len(serverData) == 1 {
		server := serverData[0]
		ret = fmt.Sprintf("%s:%s", server["SERVER_NAME"], server["SERVER_PORT"])
		//		fmt.Println(ret)
		return ret
	}
	for _, server := range serverData {
		region := server["REGION"]
		country := server["COUNTRY"]
		if (city == region || state == region) && countryCode == country {
			ret = fmt.Sprintf("%s:%s", server["SERVER_NAME"], server["SERVER_PORT"])
			//			fmt.Println(ret)
			return ret
		}
	}
	for _, server := range serverData {
		country := server["COUNTRY"]
		if countryCode == country {
			ret = fmt.Sprintf("%s:%s", server["SERVER_NAME"], server["SERVER_PORT"])
			//			fmt.Println(ret)
			return ret
		}
	}
	regionalHubs := make([]map[string]string, 0, len(serverData))
	for _, server := range serverData {
		priority, err := strconv.Atoi(server["PRIORITY"])
		if err != nil {
			continue
		}
		if priority <= 0 {
			continue
		}
		regionalHubs = append(regionalHubs, server)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.Intn(len(regionalHubs))
	server := regionalHubs[r]
	ret = fmt.Sprintf("%s:%s", server["SERVER_NAME"], server["SERVER_PORT"])
	//	fmt.Println(ret)
	return ret
}
