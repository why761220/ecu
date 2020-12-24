package ecu

import (
	"strconv"
	"testing"
)

func Test2(t *testing.T) {

	a := map[string]map[string]interface{}{}
	a["1"] = map[string]interface{}{}
	v := a["1"]
	v["2"] = 2
	v = a["1"]
	t.Log(v)
	v["3"] = 3
	for i := 0; i < 100000; i++ {
		v[strconv.FormatInt(int64(i), 10)] = i
	}
	t.Log(a)
}
