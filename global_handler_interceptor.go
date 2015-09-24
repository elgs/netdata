// global_handler_interceptor
package main

import (
	"fmt"
	"github.com/elgs/gorest2"
	"net/http"
	"strings"
)

func init() {
	gorest2.GlobalHandlerInterceptorRegistry = append(gorest2.GlobalHandlerInterceptorRegistry, &GlobalHandlerInterceptor{})
}

type GlobalHandlerInterceptor struct {
	*gorest2.DefaultHandlerInterceptor
}

func (this *GlobalHandlerInterceptor) BeforeHandle(w http.ResponseWriter, r *http.Request) (bool, error) {
	fmt.Println("Before handling: ", r.URL.Path)
	if strings.HasPrefix(r.URL.Path, "/sys/") || strings.HasPrefix(r.URL.Path, "/auth/") {
		return true, nil
	} else {
		projectId := r.Header.Get("app_id")
		token := r.Header.Get("token")
		if projectId == "default" {
			// for admin, check role

		} else {
			// for apps, check user token
			return checkProjectToken(projectId, token, "*", "rwx")
		}
	}
	return true, nil
}
func (this *GlobalHandlerInterceptor) AfterHandle(w http.ResponseWriter, r *http.Request) error {
	//	fmt.Println("After handling: ", r.URL.Path)
	return nil
}
