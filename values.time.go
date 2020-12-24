package ecu

import (

	"bufio"
	"errors"
	"io"
	"math"
	"strconv"
	"time"
)

type timeValue int64

func (ti timeValue) Format(fmt string) string {
	switch fmt {
	case "ns", "nanosecond":
		return strconv.FormatInt(int64(ti), 10)
	case "us", "microsecond":
		return strconv.FormatInt(int64(ti)/int64(time.Microsecond), 10)
	case "ms", "millisecond":
		return strconv.FormatInt(int64(ti)/int64(time.Millisecond), 10)
	case "s", "second":
		return strconv.FormatInt(int64(ti)/int64(time.Second), 10)
	case "mi", "minute":
		return strconv.FormatInt(int64(ti)/int64(time.Minute), 10)
	case "h", "hour":
		return strconv.FormatInt(int64(ti)/int64(time.Hour), 10)
	case "d", "day":
		return strconv.FormatInt(int64(ti)/int64(time.Hour*24), 10)
	default:
		if fmt == "" {
			return time.Unix(0, int64(ti)).Format(time.RFC3339Nano)
		} else {
			return time.Unix(0, int64(ti)).Format(fmt)
		}
	}
}

func (t timeValue) Type() DataType {
	return TypeTime
}
func (t timeValue) Base() interface{} {
	if t.Nil() {
		return nil
	}
	return time.Unix(0, int64(t))
}
func (t timeValue) Nil() bool {
	return t == 0
}

func (t timeValue) Bool() bool {
	panic("implement me")
}

func (t timeValue) Int() int64 {
	return int64(t)
}

func (t timeValue) Float() float64 {
	return float64(t) / 1000000000.0
}

func (t timeValue) Str() string {
	return time.Unix(0, int64(t)).Format(time.RFC3339Nano)
}

func (t timeValue) int1() int64 {
	return t.Int()
}
func (t timeValue) int2() int64 {
	panic("implement me")
}
func (t timeValue) float1() float64 {
	return t.Float()
}
func (t timeValue) float2() float64 {
	panic("implement me")
}

func (t timeValue) any2() interface{} {
	panic("implement me")
}

type timeValues struct {
	values
	slices  [][]int64
	periods struct {
		s, e int64
	}
	origin time.Time
}

func (this timeValues) ToSlice(rows Positions) interface{} {
	ret := make([]int64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Int())
		return true
	})
	return ret
}

func (this timeValues) Periods() (int64, int64) {
	return this.periods.s, this.periods.e
}
func (this timeValues) init() (Values, error) {
	return this, nil
}

func (this timeValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	if add {
		if slices, size, err := loadInts(this.alloc, this.slices, r, max); err == nil {
			this.slices, this.Size = slices, this.Size+size
			return this, nil
		} else {
			this.slices = slices
			return this.Resize(this.Size), err
		}
	} else if slices, size, err := loadInts(this.alloc, nil, r, max); err == nil {
		for _, vs := range this.slices {
			this.release(vs)
		}
		this.slices, this.Size = slices, size
		return this, nil
	} else {
		for _, vs := range slices {
			this.release(vs)
		}
		return this, nil
	}
}

func (this timeValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	} else {
		return storeInts(this.slices, w)
	}
}

func (timeValues) Type() DataType {
	return TypeTime
}
func (timeValues) alloc(l int) []int64 {
	return intSlicePool.Get().([]int64)[0:l]
}
func (timeValues) release(v []int64) {
	intSlicePool.Put(v)
}
func (timeValues) nil() int64 {
	return 0
}
func (this timeValues) value(v int64) Value {
	if v == this.nil() {
		return Nil
	} else {
		return timeValue(v)
	}
}
func (this timeValues) Resize(size int) Values {
	if size <= 0 {
		for i := range this.slices {
			this.release(this.slices[i])
		}
		this.slices, this.Size = this.slices[0:0], 0
		return this
	}
	m, n := mn(size)
	if m2 := len(this.slices) - 1; m2 < m {
		if m2 >= 0 {
			slices := this.slices[m2]
			for i := len(slices); i < VarsSliceSize; i++ {
				slices = append(slices, this.nil())
			}
			this.slices[m2] = slices
		}
		for ; m2 < m-1; m2++ {
			slices := this.alloc(VarsSliceSize)
			for i := 0; i < VarsSliceSize; i++ {
				slices[i] = this.nil()
			}
			this.slices = append(this.slices, slices)
		}
		slices := this.alloc(n)
		for i := 0; i < n; i++ {
			slices[i] = this.nil()
		}
		this.slices = append(this.slices, slices)
	} else {
		if m2 > m {
			for _, slices := range this.slices[m+1 : m2+1] {
				this.release(slices)
			}
			this.slices = this.slices[0 : m+1]
		}
		slices := this.slices[m]
		if n2 := len(slices); n2 > n {
			slices = slices[0:n]
		} else if n2 < n {
			for ; n2 < n; n2++ {
				slices = append(slices, this.nil())
			}
		}
		this.slices[m] = slices
	}
	this.Size = size
	return this
}
func (this timeValues) add(v int64) Values {
	if m := len(this.slices); m <= 0 {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else if slices := this.slices[m-1]; len(slices) >= VarsSliceSize {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else {
		this.slices[m-1] = append(slices, v)
	}
	this.Size = this.Size + 1
	if this.periods.s == 0 || v < this.periods.s {
		this.periods.s = v
	}
	if v > this.periods.e {
		this.periods.e = v
	}
	return this
}
func (this timeValues) set(pos int, v int64) (Values, Cancel, error) {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			old := slices[n]
			slices[n] = v
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errors.New("index out of array")
}
func (this timeValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil {
		return this.add(0), nil
	} else {
		switch v.Type() & TypeMask {
		case TypeInt:
			return this.add(v.Int()), nil
		case TypeTime:
			return this.add(v.Int()), nil
		case TypeStr:
			if ti, err := ParseTime(v.Str(), this.Fmt); err != nil {
				return this, err
			} else {
				return this.add(ti.UnixNano()), nil
			}
		default:
			return this, errs.New("internal logic error!")
		}
	}
}

func (this timeValues) GetFrom(getter Getter) (v Value, err error) {
	var ti time.Time
	var sec, nsec float64
	if v, err = getter(this.Name); err != nil {
		return
	} else if v == nil && this.Field != "" {
		if v, err = getter(this.Field); err != nil {
			return
		}
	}
	if v == nil || v.Type() == TypeNil {
		v = timeValue(0)
	}
	switch v.Type() & TypeMask {
	case TypeTime:
		ti = time.Unix(0, v.Int())
	case TypeInt:
		switch this.Fmt {
		case "ns", "nanosecond":
			ti = time.Unix(0, v.Int())
		case "us", "microsecond":
			ti = time.Unix(0, v.Int()*1000)
		case "ms", "millisecond":
			ti = time.Unix(0, v.Int()*1000000)
		case "s", "second":
			ti = time.Unix(v.Int(), 0)
		case "mi", "minute":
			ti = time.Unix(v.Int()*60, 0)
		case "h", "hour":
			ti = time.Unix(v.Int()*60*60, 0)
		case "d", "day":
			ti = time.Unix(v.Int()*60*60*24, 0)
		case "m", "month":
			return nil, errs.New("not supported month parse!")
		case "y", "year":
			ti = time.Date(int(v.Int()), time.January, 0, 0, 0, 0, 0, time.Local)
		default:
			ti = time.Unix(0, v.Int())
		}
	case TypeFloat:
		sec, nsec = math.Modf(v.Float())
		ti = time.Unix(int64(sec), int64(nsec*1000*1000*1000))
	case TypeStr:
		if ti, err = ParseTime(v.Str(), this.Fmt); err != nil {
			return
		}
	default:
		return nil, errors.New("type is error")
	}
	return timeValue(DoFReq(ti, this.FReq.start, this.FReq.Unit, this.FReq.Offset, this.FReq.Closed).UnixNano()), nil
}
func (this timeValues) Set(pos int, v Value, _ bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this.set(pos, this.nil())
	} else if v.Type() == TypeTime {
		return this.set(pos, v.Int())
	} else {
		return this, nil, errors.New("type is error")
	}
}

func (this timeValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if n >= 0 && n < len(this.slices[m]) {
			return this.value(this.slices[m][n])
		}
	}
	return Nil
}
