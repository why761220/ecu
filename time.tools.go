package ecu

import (

	"regexp"
	"strconv"
	"time"
)

type FReq struct {
	Unit   string `json:"unit,omitempty"`
	Offset int    `json:"offset,omitempty"`
	Start  string `json:"start,omitempty"`
	Closed bool   `json:"closed,omitempty"`
	start  time.Time
}

var defFReq = &FReq{start: time.Unix(946656000, 0)} //2000-01-01T00:00:00+08:00
func (r *FReq) init(fmt string) *FReq {
	var err error
	if r.start, err = ParseTime(r.Start, fmt); err != nil {
		r.start = time.Unix(946656000, 0)
	}
	if r.Offset <= 0 {
		r.Offset = 1
	}
	return r
}

/*
	case "ns", "nanosecond":
	case "us", "microsecond":
	case "ms", "millisecond":
	case "s", "second":
	case "mi", "minute":
	case "h", "hour":
	case "d", "day":
	case "m", "month":
	case "y", "year":
*/
func ParseTime(s string, fmt string) (ti time.Time, err error) {
	var i int64
	switch fmt {
	case "ns", "nanosecond":
		if i, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		} else {
			return time.Unix(0, i), nil
		}
	case "us", "microsecond":
		if i, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		} else {
			return time.Unix(0, i*int64(time.Microsecond)), nil
		}
	case "ms", "millisecond":
		if i, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		} else {
			return time.Unix(0, i*int64(time.Millisecond)), nil
		}
	case "s", "second":
		if i, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		} else {
			return time.Unix(0, i*int64(time.Second)), nil
		}
	case "mi", "minute":
		if i, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		} else {
			return time.Unix(0, i*int64(time.Minute)), nil
		}
	case "h", "hour":
		if i, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		} else {
			return time.Unix(0, i*int64(time.Hour)), nil
		}
	case "d", "day":
		if i, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		} else {
			return time.Unix(0, i*int64(time.Hour)*24), nil
		}
	default:
		if fmt == "" {
			fmt = time.RFC3339Nano
		}
		if ti, e := time.ParseInLocation(fmt, s, time.Local); e != nil {
			return ti, errs.New(e)
		} else {
			return ti, nil
		}
	}
}
func FormatTime(ti int64, fmt string) interface{} {
	switch fmt {
	case "ns", "nanosecond":
		return ti
	case "us", "microsecond":
		return ti / int64(time.Microsecond)
	case "ms", "millisecond":
		return ti / int64(time.Millisecond)
	case "s", "second":
		return ti / int64(time.Second)
	case "mi", "minute":
		return ti / int64(time.Minute)
	case "h", "hour":
		return ti / int64(time.Hour)
	case "d", "day":
		return ti / int64(time.Hour*24)
	default:
		if fmt == "" {
			fmt = time.RFC3339Nano
		}
		return time.Unix(0, ti).Format(fmt)
	}
}
func ParseFreq(s string) (ret FReq, err error) {
	var offset int64
	reg1 := regexp.MustCompile(`\d{1,2}d|\d{1,2}h|\d{1,2}mi|\d{1,2}s|\d{1,3}ms|\d{1,3}ns`)
	reg2 := regexp.MustCompile(`(\d+)|(d|h|mi|ms|ns|s)`)
	tss := reg1.FindAllString(s, -1)
	for _, ts := range tss {
		if ret.Unit != "" {
			err = errs.New("not support month or year mixed operation!")
		}
		t := reg2.FindAllString(ts, -1)
		if offset, err = strconv.ParseInt(t[0], 10, 64); err != nil {
			return
		}
		switch t[1] {
		case "ns", "nanosecond":
			ret.Offset = ret.Offset + int(offset)
		case "us", "microsecond":
			ret.Offset = ret.Offset + int(offset)*int(time.Microsecond)
		case "ms", "millisecond":
			ret.Offset = ret.Offset + int(offset)*int(time.Microsecond)
		case "s", "second":
			ret.Offset = ret.Offset + int(offset)*int(time.Second)
		case "mi", "minute":
			ret.Offset = ret.Offset + int(offset)*int(time.Minute)
		case "h", "hour":
			ret.Offset = ret.Offset + int(offset)*int(time.Hour)
		case "d", "day":
			ret.Offset = ret.Offset + int(offset)*int(time.Hour*24)
		case "m", "month":
			ret.Unit, ret.Offset = t[1], int(offset)
		case "y", "year":
			ret.Unit, ret.Offset = t[1], int(offset)
		}
	}
	if ret.Unit == "" {
		ret.Unit = "ns"
	}
	return
}
func CompareDay(s, e time.Time) (ret int) {
	if e.Day() > s.Day() {
		ret = 1
	} else if e.Day() < s.Day() {
		ret = -1
	} else if e.Month() > s.Month() {
		ret = 1
	} else if e.Month() < s.Month() {
		ret = -1
	} else if e.Day() > s.Day() {
		ret = 1
	} else if e.Day() < s.Day() {
		ret = -1
	} else if e.Hour() > s.Hour() {
		ret = 1
	} else if e.Hour() < s.Hour() {
		ret = -1
	} else if e.Minute() > s.Minute() {
		ret = 1
	} else if e.Minute() < s.Minute() {
		ret = -1
	} else if e.Second() > s.Second() {
		ret = 1
	} else if e.Second() < s.Second() {
		ret = -1
	} else if e.Nanosecond() > s.Nanosecond() {
		ret = 1
	} else if e.Nanosecond() < s.Nanosecond() {
		ret = -1
	}
	return
}

func CompareMonth(s, e time.Time) (ret int) {
	if e.Month() > s.Month() {
		ret = 1
	} else if e.Month() < s.Month() {
		ret = -1
	} else if e.Day() > s.Day() {
		ret = 1
	} else if e.Day() < s.Day() {
		ret = -1
	} else if e.Month() > s.Month() {
		ret = 1
	} else if e.Month() < s.Month() {
		ret = -1
	} else if e.Day() > s.Day() {
		ret = 1
	} else if e.Day() < s.Day() {
		ret = -1
	} else if e.Hour() > s.Hour() {
		ret = 1
	} else if e.Hour() < s.Hour() {
		ret = -1
	} else if e.Minute() > s.Minute() {
		ret = 1
	} else if e.Minute() < s.Minute() {
		ret = -1
	} else if e.Second() > s.Second() {
		ret = 1
	} else if e.Second() < s.Second() {
		ret = -1
	} else if e.Nanosecond() > s.Nanosecond() {
		ret = 1
	} else if e.Nanosecond() < s.Nanosecond() {
		ret = -1
	}
	return
}
func DoFReq2(start, s, e time.Time, closed, Reversed bool, d time.Duration, offset int) time.Time {
	var m, n int64
	sm, sn := s.UnixNano()/(int64(d)), s.UnixNano()%(int64(d))
	em, en := e.UnixNano()/(int64(d)), e.UnixNano()%(int64(d))
	m = em - sm
	m, n = m/int64(offset), m%int64(offset)
	if n > 0 {
		if closed != Reversed {
			m = m + 1
		}
	} else if n == 0 {
		if en > sn {
			if closed != Reversed {
				m = m + 1
			}
		} else if en < sn {
			if closed == Reversed {
				m = m - 1
			}
		}
	}
	m = m * int64(offset) * int64(d)
	if Reversed {
		m = -m
	}
	return start.Add(time.Duration(m))
}
func DoFReq(ti, start time.Time, unit string, offset int, closed bool) time.Time {
	var reversed bool
	var s, e time.Time
	var m, n, t, year, month int
	if unit == "" {
		return ti
	} else if ti.Equal(start) {
		return ti
	} else if reversed = ti.Before(start); reversed {
		e, s = start, ti
	} else {
		s, e = start, ti
	}
	switch unit {
	case "ns", "nanosecond":
		return DoFReq2(start, s, e, closed, reversed, time.Nanosecond, offset)
	case "us", "microsecond":
		return DoFReq2(start, s, e, closed, reversed, time.Microsecond, offset)
	case "ms", "millisecond":
		return DoFReq2(start, s, e, closed, reversed, time.Millisecond, offset)
	case "s", "second":
		return DoFReq2(start, s, e, closed, reversed, time.Second, offset)
	case "mi", "minute":
		return DoFReq2(start, s, e, closed, reversed, time.Minute, offset)
	case "h", "hour":
		return DoFReq2(start, s, e, closed, reversed, time.Hour, offset)
	case "d", "day":
		return DoFReq2(start, s, e, closed, reversed, time.Hour*24, offset)
	case "m", "month":
		m = (e.Year()-s.Year())*12 + int(e.Month()) - int(s.Month())
		m, n = m/offset, m%offset
		if n > 0 {
			if closed != reversed {
				m = m + 1
			}
		} else if n == 0 {
			if t = CompareDay(s, e); t > 0 {
				if closed != reversed {
					m = m + 1
				}
			} else if t < 0 {
				if closed == reversed {
					m = m - 1
				}
			}
		}
		m = m * offset
		year, month = m/12, m%12
		if reversed {
			year, month = -year, -month
		}
		return time.Date(start.Year()+year, start.Month()+time.Month(month), start.Day(), start.Hour(), start.Minute(), start.Second(), start.Nanosecond(), time.Local)
	case "y", "year":
		m = e.Year() - s.Year()
		if m, n = m/offset, m%offset; n > 0 {
			if closed != reversed {
				m = m + 1
			}
		} else if n == 0 {
			if t = CompareMonth(s, e); t > 0 {
				if closed != reversed {
					m = m + 1
				}
			} else if t < 0 {
				if closed == reversed {
					m = m - 1
				}
			}
		}
		year = m * offset
		if reversed {
			year = -year
		}
		return time.Date(start.Year()+year, start.Month(), start.Day(), start.Hour(), start.Minute(), start.Second(), start.Nanosecond(), time.Local)
	}
	return ti
}
