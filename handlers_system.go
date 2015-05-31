// handlers
package main

import (
	"fmt"
	"github.com/elgs/gorest2"
	"net/http"
	"os"
	"strings"
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
}
