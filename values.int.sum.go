package ecu

import (

	"bufio"
	"io"
	"strconv"
)

type intSumValue int64

func (this intSumValue) Format(fmt string) string {
	if this.Nil() {
		return ""
	}
	return FmtInt(this.Int(), fmt)
}

type intSumValues struct {
	values
	slices [][]int64
}

func (this intSumValues) ToSlice(rows Positions) interface{} {
	ret := make([]int64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Int())
		return true
	})
	return this.value(this.nil())
}

func (this intSumValues) init() (Values, error) {
	return this, nil
}

func (this intSumValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
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

func (this intSumValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	} else {
		return storeInts(this.slices, w)
	}
}

func (intSumValue) Type() DataType {
	return TypeIntSum
}

func (i intSumValue) Base() interface{} {
	if i.Nil() {
		return nil
	}
	return i.Int()
}

func (i intSumValue) Nil() bool {
	return false
}

func (i intSumValue) Bool() bool {
	panic("implement me")
}

func (i intSumValue) Int() int64 {
	return int64(i)
}

func (i intSumValue) int1() int64 {
	return i.Int()
}

func (i intSumValue) int2() int64 {
	panic("implement me")
}

func (i intSumValue) Float() float64 {
	return float64(i.Int())
}

func (i intSumValue) float1() float64 {
	return i.Float()
}

func (i intSumValue) float2() float64 {
	panic("implement me")
}

func (i intSumValue) Str() string {
	return strconv.FormatInt(i.Int(), 10)
}

func (i intSumValue) any2() interface{} {
	panic("implement me")
}

func (intSumValues) Type() DataType {
	return TypeIntSum
}
func (intSumValues) alloc(l int) []int64 {
	return intSlicePool.Get().([]int64)[0:l]
}
func (intSumValues) release(v []int64) {
	intSlicePool.Put(v)
}
func (intSumValues) nil() int64 {
	return 0
}
func (this intSumValues) value(v int64) Value {
	return intSumValue(v)
}
func (this intSumValues) Resize(size int) Values {
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
func (this intSumValues) add(v int64) Values {
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
func (this intSumValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(this.nil()), nil
	} else {
		switch v.Type() & TypeMask {
		case TypeInt, TypeTime:
			return this.add(v.int1()), nil
		case TypeFloat:
			if this.IsForbid() {
				return this, errs.New("forbid type convert!")
			} else {
				return this.tof().Add(getter)
			}
		case TypeStr:
			if i, err := strconv.ParseInt(v.Str(), 10, 64); err == nil {
				return this.add(i), nil
			} else if this.IsForbid() {
				return this, errs.New("forbid type convert!")
			} else if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.tof().add(f), nil
			} else {
				return this, errs.New(err)
			}
		}
		return this, errs.New("wrong data type!")
	}
}
func (this intSumValues) set(merge bool, pos int, v int64) (Values, Cancel, error) {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			old := slices[n]
			if merge {
				if old == this.nil() {
					slices[n] = v
				} else {
					slices[n] = old + v
				}
			} else {
				slices[n] = v
			}
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errs.New("index out of array!")
}
func (this intSumValues) Set(pos int, v Value, merge bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this.set(merge, pos, 0)
	}
	switch v.Type() & TypeMask {
	case TypeInt, TypeTime:
		return this.set(merge, pos, v.Int())
	case TypeFloat:
		if this.IsForbid() {
			return this, func() {}, errs.New("forbid type convert!")
		} else {
			return this.tof().set(merge, pos, v.Float())
		}
	case TypeStr:
		if i, err := strconv.ParseInt(v.Str(), 10, 64); err == nil {
			return this.set(merge, pos, i)
		} else if this.IsForbid() {
			return this, func() {}, errs.New("forbid type convert!")
		} else if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
			return this.tof().set(merge, pos, f)
		} else {
			return this, func() {}, errs.New(err)
		}
	}
	return this, nil, errs.New("data type error!")
}

func (this intSumValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if n >= 0 && n < len(this.slices[m]) {
			return this.value(this.slices[m][n])
		}
	}
	return Nil
}

func (this intSumValues) tof() (ret floatSumValues) {
	ret.values = this.values
	ret.values.DataType = TypeFloatSum
	ret.slices = make([][]float64, 0, len(this.slices))
	for _, src := range this.slices {
		dst := ret.alloc(len(src))
		for i, l := 0, len(src); i < l; i++ {
			if src[i] == this.nil() {
				dst[i] = ret.nil()
			} else {
				dst[i] = float64(src[i])
			}
		}
		ret.slices = append(ret.slices, dst)
	}
	return
}
