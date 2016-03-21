// global_handler_interceptor
package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/elgs/gorest2"
)

func init() {
	gorest2.GlobalHandlerInterceptorRegistry = append(gorest2.GlobalHandlerInterceptorRegistry, &GlobalHandlerInterceptor{})
}

type GlobalHandlerInterceptor struct {
	*gorest2.DefaultHandlerInterceptor
}

func (this *GlobalHandlerInterceptor) BeforeHandle(w http.ResponseWriter, r *http.Request) (bool, error) {
	//	fmt.Println("Before handling: ", r.URL.Path)
	if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/sys/") || strings.HasPrefix(r.URL.Path, "/auth/") {
		return true, nil
	} else {
		projectId := r.Header.Get("app_id")
		token := r.Header.Get("token")
		if projectId == "" {
			projectId = r.FormValue("app_id")
			token = r.FormValue("token")
		}
		if projectId == "default" {
			// for admin, check role
			allow, _, err := checkDefaultToken(token, r.URL.Path)
			if !allow {
				fmt.Println("auth failed:", r.URL.Path)
			}
			return allow, err
		} else {
			// for apps, check user token
			allow, err := checkProjectToken(map[string]interface{}{
				"app_id": projectId,
				"token":  token,
			}, "*", "rwx")
			if !allow {
				fmt.Println("auth failed:", r.URL.Path)
			}
			return allow, err
		}
	}
	return true, nil
}
func (this *GlobalHandlerInterceptor) AfterHandle(w http.ResponseWriter, r *http.Request) error {
	//	fmt.Println("After handling: ", r.URL.Path)
	return nil
}
