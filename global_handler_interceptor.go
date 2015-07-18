// global_handler_interceptor
package main

import (
	"fmt"
	"github.com/elgs/gorest2"
	"net/http"
)

func init() {
	gorest2.GlobalHandlerInterceptorRegistry = append(gorest2.GlobalHandlerInterceptorRegistry, &GlobalHandlerInterceptor{})
}

type GlobalHandlerInterceptor struct {
	*gorest2.DefaultHandlerInterceptor
}

func (this *GlobalHandlerInterceptor) BeforeHandle(w http.ResponseWriter, r *http.Request) (bool, error) {
	fmt.Println("Before handling: ", r.URL.Path)
	return true, nil
}
func (this *GlobalHandlerInterceptor) AfterHandle(w http.ResponseWriter, r *http.Request) error {
	fmt.Println("After handling: ", r.URL.Path)
	return nil
}
