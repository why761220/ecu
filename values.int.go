package ecu

import (

	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
)

type intValue int64

func FmtInt(i int64, fmt string) string {
	switch fmt {
	case "8", "oct":
		return strconv.FormatInt(i, 8)
	case "16", "hex":
		return strconv.FormatInt(i, 16)
	default:
		return strconv.FormatInt(i, 10)
	}
}
func (i intValue) Format(fmt string) string {
	if i.Nil() {
		return ""
	}
	return FmtInt(i.Int(), fmt)
}

func (i intValue) Type() DataType {
	return TypeInt
}

func (i intValue) Base() interface{} {
	if i.Nil() {
		return nil
	}
	return i.Int()
}
func (i intValue) Nil() bool {
	return i == math.MinInt64
}
func (i intValue) Bool() bool {
	return i == 1
}
func (i intValue) Float() float64 {
	return float64(i.Int())
}
func (i intValue) Int() int64 {
	return int64(i)
}
func (i intValue) Str() string {
	if i.Nil() {
		return ""
	} else {
		return strconv.FormatInt(i.Int(), 10)
	}
}

func (i intValue) int1() int64 {
	return i.Int()
}
func (i intValue) int2() int64 {
	panic("implement me")
}
func (i intValue) float1() float64 {
	return i.Float()
}

func (i intValue) float2() float64 {
	panic("implement me")
}

func (i intValue) any2() interface{} {
	panic("implement me")
}

type intValues struct {
	values
	slices [][]int64
}

func (this intValues) ToSlice(rows Positions) interface{} {
	ret := make([]int64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Int())
		return true
	})
	return ret
}

func (this intValues) init() (Values, error) {
	return this, nil
}
func storeInts(slices [][]int64, w io.Writer) (err error) {
	bs := make([]byte, 8)
	for _, s := range slices {
		for i := range s {
			binary.BigEndian.PutUint64(bs, uint64(s[i]))
			if _, err = w.Write(bs); err != nil {
				return
			}
		}
	}
	return
}
func loadInts(alloc func(int) []int64, old [][]int64, r io.Reader, max int) (slices [][]int64, size int, err error) {

	var m int
	var bs [8]byte
	if slices, m = old, len(old)-1; m < 0 {
		m, slices = 0, append(make([][]int64, 0, 32), alloc(0))
	}
	for size = 0; size < max; size++ {
		if _, err = io.ReadFull(r, bs[:]); err != nil {
			return
		}
		if len(slices[m]) >= VarsSliceSize {
			m, slices = m+1, append(slices, alloc(0))
		}
		slices[m] = append(slices[m], int64(binary.BigEndian.Uint64(bs[:])))
	}
	return
}

func (this intValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
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

func (this intValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	} else {
		return storeInts(this.slices, w)
	}
}

func (intValues) Type() DataType {
	return TypeInt
}
func (intValues) alloc(l int) []int64 {
	return intSlicePool.Get().([]int64)[0:l]
}
func (intValues) release(v []int64) {
	intSlicePool.Put(v)
}
func (intValues) nil() int64 {
	return math.MinInt64
}
func (this intValues) value(v int64) Value {
	if v == this.nil() {
		return Nil
	} else {
		return intValue(v)
	}
}
func (this intValues) Resize(size int) Values {
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

func (this intValues) i2f() (ret *floatValues) {
	ret = &floatValues{values: this.values}
	ret.slices = make([][]float64, len(this.slices))
	for i, src := range this.slices {
		dst := ret.alloc(len(src))
		for j, v := range src {
			if v == this.nil() {
				dst[j] = ret.nil()
			} else {
				dst[j] = float64(v)
			}
		}
		ret.slices[i] = dst
		this.release(src)
	}
	this.Size, this.slices = 0, this.slices[0:0]
	return
}
func (this intValues) i2s() (ret strValues) {
	ret.values = this.values
	ret.slices = make([][]string, len(this.slices))
	for i, src := range this.slices {
		dst := ret.alloc(len(src))
		for j, v := range src {
			if v == this.nil() {
				dst[j] = ret.nil()
			} else {
				dst[j] = strconv.FormatInt(v, 10)
			}
		}
		ret.slices[i] = dst
		this.release(src)
	}
	this.Size, this.slices = 0, nil
	return
}

func (this intValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(this.nil()), nil
	} else {
		switch v.Type() & TypeMask {
		case TypeInt:
			return this.add(v.Int()), nil
		case TypeFloat:
			if this.IsForbid() {
				return this, errs.New("forbid type convert!")
			} else {
				return this.i2f().Add(getter)
			}
		case TypeStr:
			if i, err := strconv.ParseInt(v.Str(), 10, 64); err == nil {
				return this.add(i), nil
			} else if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.i2f().add(f), nil
			} else if this.IsForbid() {
				return this, errs.New("forbid type convert!")
			} else {
				return this.i2s().add(v.Str()), nil
			}
		default:
			return this, errs.New("internal logic error!")
		}
	}
}
func (this intValues) Set(pos int, v Value, _ bool) (Values, Cancel, error) {
	if v == nil || v.Nil() {
		return this.set(pos, this.nil())
	} else {
		switch v.Type() {
		case TypeInt:
			return this.set(pos, v.Int())
		case TypeFloat:
			return this.i2f().set(pos, v.Float())
		case TypeStr:
			if i, err := strconv.ParseInt(v.Str(), 10, 64); err == nil {
				return this.set(pos, i)
			} else if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.i2f().set(pos, f)
			} else if this.IsForbid() {
				return this, func() {}, errs.New("forbid type convert!")
			} else {
				return this.i2s().set(pos, v.Str())
			}
		default:
			return this, func() {}, errors.New("type is error")
		}
	}
}

func (this intValues) add(v int64) Values {
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
func (this intValues) set(pos int, v int64) (Values, Cancel, error) {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			old := slices[n]
			slices[n] = v
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errors.New("index out of array")
}
func (this intValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if n >= 0 && n < len(this.slices[m]) {
			return this.value(this.slices[m][n])
		}
	}
	return Nil
}
