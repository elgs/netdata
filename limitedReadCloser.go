// LimitedReadCloser
package main

import (
	"errors"
	"io"
)

type LimitedReadCloser struct {
	io.ReadCloser
	N int64
}

func (l *LimitedReadCloser) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, errors.New("http: response body too large")
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.ReadCloser.Read(p)
	l.N -= int64(n)
	return
}