package ecu

import (

	"bufio"
	"encoding/binary"
	"io"
	"strconv"
)

type (
	intAvgValue struct {
		total int64
		count int64
	}
	intAvgValues struct {
		values
		slices [][]intAvgValue
	}
)

func (this intAvgValues) ToSlice(rows Positions) interface{} {
	ret := make([]int64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Int())
		return true
	})
	return ret
}

func (this intAvgValue) Format(fmt string) string {
	if this.Nil() {
		return ""
	}
	return FmtInt(this.Int(), fmt)
}

func (this intAvgValues) init() (Values, error) {
	return this, nil
}
func loadIntAvgs(alloc func(int) []intAvgValue, old [][]intAvgValue, r io.Reader, max int) (slices [][]intAvgValue, size int, err error) {
	var total, count int64
	var bs [16]byte
	m := len(old) - 1
	slices = old
	if m < 0 {
		m, slices = 0, append(make([][]intAvgValue, 0, 32), alloc(0))
	}
	for size = 0; size < max; size++ {
		if _, err = io.ReadFull(r, bs[:]); err != nil {
			return slices, size, err
		}
		total = int64(binary.BigEndian.Uint64(bs[0:8]))
		count = int64(binary.BigEndian.Uint64(bs[8:16]))
		if len(slices[m]) >= VarsSliceSize {
			m, slices = m+1, append(slices, alloc(0))
		}
		slices[m] = append(slices[m], intAvgValue{total: total, count: count})
	}
	return slices, size, err
}
func (this intAvgValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	if add {
		if slices, size, err := loadIntAvgs(this.alloc, this.slices, r, max); err == nil {
			this.slices, this.Size = slices, this.Size+size
			return this, nil
		} else {
			this.slices = slices
			return this.Resize(this.Size), err
		}
	} else if slices, size, err := loadIntAvgs(this.alloc, nil, r, max); err == nil {
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

func (this intAvgValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	}
	buf := make([]byte, 8)
	for _, slices := range this.slices {
		for i := range slices {
			binary.BigEndian.PutUint64(buf, uint64(slices[i].total))
			if _, err = w.Write(buf); err != nil {
				return
			}
			binary.BigEndian.PutUint64(buf, uint64(slices[i].count))
			if _, err = w.Write(buf); err != nil {
				return
			}
		}
	}
	return
}

func (i intAvgValue) Type() DataType {
	return TypeIntAvg
}

func (i intAvgValue) Base() interface{} {
	if i.Nil() {
		return nil
	}
	return i.Int()
}

func (i intAvgValue) Nil() bool {
	return i.count == 0
}

func (i intAvgValue) Bool() bool {
	panic("implement me")
}

func (i intAvgValue) Int() int64 {
	if i.Nil() {
		return 0
	}
	return i.total / i.count
}
func (i intAvgValue) int1() int64 {
	return i.total
}
func (i intAvgValue) int2() int64 {
	return i.count
}
func (i intAvgValue) Float() float64 {
	return float64(i.Int())
}
func (i intAvgValue) float1() float64 {
	return float64(i.int1())
}

func (i intAvgValue) float2() float64 {
	return float64(i.int2())
}
func (i intAvgValue) Str() string {
	return strconv.FormatInt(i.Int(), 10)
}

func (i intAvgValue) any2() interface{} {
	panic("implement me")
}

func (intAvgValues) Type() DataType {
	return TypeIntAvg
}
func (this intAvgValues) alloc(l int) []intAvgValue {
	return intAvgSlicePool.Get().([]intAvgValue)[0:l]
}
func (this intAvgValues) release(v []intAvgValue) {
	intAvgSlicePool.Put(v)
}
func (intAvgValues) nil() intAvgValue {
	return intAvgValue{}
}
func (this intAvgValues) value(v intAvgValue) Value {
	if v.count == 0 {
		return nil
	} else {
		return v
	}
}
func (this intAvgValues) Resize(size int) Values {
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

func (this intAvgValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if n >= 0 && n < len(this.slices[m]) {
			return this.value(this.slices[m][n])
		}
	}
	return Nil
}
func (this intAvgValues) add(v intAvgValue) Values {
	if m := len(this.slices); m <= 0 {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else if slices := this.slices[m-1]; len(slices) >= VarsSliceSize {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else {
		this.slices[m-1] = append(slices, v)
	}
	this.Size = this.Size + 1
	return this
}
func (this intAvgValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeMask {
		return this.add(intAvgValue{}), nil
	} else {
		switch v.Type() {
		case TypeInt, TypeTime:
			return this.add(intAvgValue{total: v.int1(), count: 1}), nil
		case TypeFloat:
			return this.add(intAvgValue{total: v.Int(), count: 1}), nil
		case TypeIntAvg:
			return this.add(intAvgValue{total: v.int1(), count: v.int2()}), nil
		case TypeFloatAvg:
			if this.IsForbid() {
				return this, errs.New("forbid type convert!")
			} else {
				return this.tof().Add(getter)
			}
		case TypeStr:
			if i, err := strconv.ParseInt(v.Str(), 10, 64); err == nil {
				return this.add(intAvgValue{total: i, count: 1}), nil
			} else if this.IsForbid() {
				return this, errs.New("forbid type convert!")
			} else if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.tof().add(floatAvgValue{total: f, count: 1})
			} else {
				return this, errs.New(err)
			}
		}
		return this, errs.New("wrong data type!")
	}
}
func (this *intAvgValues) set(agg bool, pos int, total, count int64) (Values, Cancel, error) {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			old := slices[n]
			if agg {
				slices[n] = intAvgValue{total: old.total + total, count: old.count + count}
			} else {
				slices[n] = intAvgValue{total: total, count: count}
			}
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errs.New("index of array!")
}
func (this intAvgValues) Set(pos int, v Value, merge bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this, func() {}, nil
	} else {
		switch v.Type() {
		case TypeInt, TypeTime:
			return this.set(merge, pos, v.int1(), 1)
		case TypeFloat:
			if this.IsForbid() {
				return this, func() {}, errs.New("forbid type convert!")
			} else {
				return this.tof().Set(pos, v, merge)
			}
		case TypeIntAvg:
			return this.set(merge, pos, v.int1(), v.int2())
		case TypeFloatAvg:
			if this.IsForbid() {
				return this, func() {}, errs.New("forbid type convert!")
			} else {
				return this.tof().set(merge, pos, v.float1(), v.float2())
			}
		default:
			if i, err := strconv.ParseInt(v.Str(), 10, 64); err == nil {
				return this.set(merge, pos, i, 1)
			} else if this.IsForbid() {
				return this, func() {}, errs.New("forbid type convert!")
			} else if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.tof().set(merge, pos, f, 1)
			} else {
				return this, func() {}, errs.New(err)
			}
		}
	}
}

func (this intAvgValues) tof() (ret floatAvgValues) {
	ret.values = this.values
	ret.values.DataType = TypeFloatAvg
	ret.slices = make([][]floatAvgValue, 0, len(this.slices))
	for _, src := range this.slices {
		dst := ret.alloc(len(src))
		for i, l := 0, len(src); i < l; i++ {
			if src[i] == this.nil() {
				dst[i] = ret.nil()
			} else {
				dst[i] = floatAvgValue{total: float64(src[i].total), count: float64(src[i].count)}
			}
		}
		ret.slices = append(ret.slices, dst)
	}
	return
}
