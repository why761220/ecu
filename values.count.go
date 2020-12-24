package ecu

import (
	"bufio"
	"io"
	"strconv"
)

type (
	countValue  int64
	countValues struct {
		values
		slices [][]int64
	}
)

func (this countValues) ToSlice(rows Positions) interface{} {
	ret := make([]int64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Int())
		return true
	})
	return ret
}

func (c countValue) Type() DataType {
	return TypeCount
}

func (c countValue) Base() interface{} {
	return c.Int()
}

func (c countValue) Nil() bool {
	return false
}

func (c countValue) Bool() bool {
	panic("implement me")
}

func (c countValue) Int() int64 {
	return int64(c)
}

func (c countValue) Float() float64 {
	return float64(c.Int())
}
func (c countValue) float1() float64 {
	return c.Float()
}
func (c countValue) Str() string {
	return strconv.FormatInt(c.Int(), 10)
}

func (c countValue) int2() int64 {
	panic("implement me")
}
func (c countValue) float2() float64 {
	panic("implement me")
}
func (c countValue) int1() int64 {
	return c.Int()
}

func (c countValue) any2() interface{} {
	panic("implement me")
}
func (this countValue) Format(fmt string) string {
	if this.Nil() {
		return ""
	}
	return FmtInt(this.Int(), fmt)
}

func (this countValues) init() (Values, error) {
	return this, nil
}

func (this countValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
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

func (this countValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	} else {
		return storeInts(this.slices, w)
	}
}

func (countValues) Type() DataType {
	return TypeCount
}
func (countValues) alloc(l int) []int64 {
	return intSlicePool.Get().([]int64)[0:l]
}
func (countValues) release(v []int64) {
	intSlicePool.Put(v)
}
func (countValues) nil() int64 {
	return 0
}
func (this countValues) Resize(size int) Values {
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

func (this countValues) Get(pos int) Value {
	if m, n := mn(pos); m < len(this.slices) {
		if slices := this.slices[m]; n < len(slices) {
			return countValue(slices[n])
		}
	}
	return Nil
}
func (this countValues) set(merge bool, pos int, v int64) (Values, Cancel, error) {
	if m, n := mn(pos); m < len(this.slices) {
		if slices := this.slices[m]; n < len(slices) {
			old := slices[n]
			if merge {
				slices[n] = old + v
			} else {
				slices[n] = v
			}
			return this, func() { slices[n] = old }, nil
		}
	}
	return nil, func() {}, errs.New("wrong data type!")
}

func (this countValues) GetFrom(getter Getter) (v Value, err error) {
	if v, err = getter(this.Name); err == nil && v != nil && v.Type() == TypeCount {
		return v, nil
	} else if this.Field == "" {
		return countValue(1), nil
	} else if v, err = getter(this.Field); err != nil {
		return nil, err
	} else if v != nil && v.Type() != TypeNil {
		return countValue(1), nil
	} else {
		return countValue(0), nil
	}
}
func (this countValues) Set(pos int, v Value, merge bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this, func() {}, nil
	} else if v.Type() == TypeCount {
		return this.set(merge, pos, v.Int())
	} else {
		return this.set(merge, pos, 1)
	}
}
func (this countValues) add(v int64) Values {
	if m := len(this.slices); m == 0 {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else if slices := this.slices[m-1]; len(slices) >= VarsSliceSize {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else {
		this.slices[m-1] = append(slices, v)
	}
	this.Size = this.Size + 1
	return this
}
func (this countValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(0), nil
	} else if v.Type() == TypeCount {
		return this.add(v.Int()), nil
	} else {
		return this.add(1), nil
	}
}
