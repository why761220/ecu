package ecu

import (
	"testing"
	"time"
)

func Test_FReq(t *testing.T) {
	freq := make([]FReq, 0, 10)
	freq = append(freq, FReq{Unit: "m", Offset: 13})

	var start time.Time
	start = time.Unix(57600, 0) //time.Parse(time.RFC3339Nano, "2000-01-01T00:00:00+08:00")
	//start = time.Unix(0, int64(-time.Hour*8))
	//time.Parse(time.RFC3339Nano, "2020-01-04T02:00:00.1+08:00")
	if ti := time.Unix(1601511226, 0); !ti.IsZero() {
		t.Log(start.String())
		t.Log(ti.String())
		t.Log(DoFReq(ti, start, "day", 1, false).String())
		t.Log(DoFReq(ti, start, "day", 1, true).String())
		t.Log(DoFReq(ti, start, "day", 1, false).Unix())
		t.Log(DoFReq(ti, start, "day", 1, true).Unix())
	} else {

	}
}
func Test_Locate(t *testing.T) {
	if ti, err := time.Parse("2006-01-02T15:04:05Z07:00", "2020-01-04T02:00:00+08:00"); err != nil {
		t.Log(err)
	} else {
		t.Log(ti.UnixNano())
	}
	if ti, err := time.ParseInLocation("2006-01-02T15:04:05Z07:00", "2020-01-04T02:00:00+08:00", time.Local); err != nil {
		t.Log(err)
	} else {
		t.Log(ti.UnixNano())
	}

}
