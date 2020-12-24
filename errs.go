package ecu

import (
	"fmt"
	"runtime"
	"strings"
)

type Errs struct{
	file string
	line int
	errs []interface{}
}

func (e Errs) Error() string {
	l := []interface{}{e.file, "[", e.line, "]:"}
	l = append(l, e.errs...)
	return fmt.Sprint(l...)
}

var errs = Errs{}


func(Errs)New(errs... interface{})error{
	e := &Errs{}
	e.errs = errs
	if _, file, line, ok := runtime.Caller(1); ok {
		e.file = func(name string)string{
			if pos := strings.LastIndex(name, "/"); pos >= 0 {
				return name[pos+1:]
			} else {
				return name
			}
		}(file)
		e.line = line
	}
	return e
}
func (Errs)Println(v ...interface{}) {
	empty := true
	for _, i := range v {
		if i != nil {
			empty = false
			break
		}
	}
	if empty {
		return
	}

	if _, file, line, ok := runtime.Caller(1); ok {
		fmt.Printf("%s->%d ",func(name string)string{
			if pos := strings.LastIndex(name, "/"); pos >= 0 {
				return name[pos+1:]
			} else {
				return name
			}
		}(file) , line)
	}
	fmt.Println(v...)
}
func (Errs)Logic() error {
	e := &Errs{errs: []interface{}{"internal logic error!"}}
	if _, file, line, ok := runtime.Caller(1); ok {
		e.file = func(name string)string{
			if pos := strings.LastIndex(name, "/"); pos >= 0 {
				return name[pos+1:]
			} else {
				return name
			}
		}(file)
		e.line = line
	}
	return e
}