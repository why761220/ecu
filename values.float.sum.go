package ecu

import (

	"bufio"
	"io"
	"math"
	"strconv"
)

type floatSumValue float64

func (this floatSumValue) Format(fmt string) string {
	if this.Nil() {
		return ""
	}
	return FmtFloat(this.Float(), fmt)
}

type floatSumValues struct {
	values
	slices [][]float64
}

func (this floatSumValues) ToSlice(rows Positions) interface{} {
	ret := make([]float64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Float())
		return true
	})
	return ret
}

func (this floatSumValues) init() (Values, error) {
	return this, nil
}

func (this floatSumValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	if add {
		if slices, size, err := loadFloats(this.alloc, this.slices, r, max); err == nil {
			this.slices, this.Size = slices, this.Size+size
			return this, nil
		} else {
			this.slices = slices
			return this.Resize(this.Size), err
		}
	} else if slices, size, err := loadFloats(this.alloc, nil, r, max); err == nil {
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

func (this floatSumValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	} else {
		return storeFloats(this.slices, w)
	}
}

func (f floatSumValue) Type() DataType {
	return TypeFloatSum
}

func (f floatSumValue) Base() interface{} {
	if f.Nil() {
		return nil
	}
	return f.Float()
}

func (f floatSumValue) Nil() bool {
	return f == math.SmallestNonzeroFloat64
}

func (f floatSumValue) Bool() bool {
	panic("implement me")
}

func (f floatSumValue) Int() int64 {
	panic("implement me")
}

func (f floatSumValue) int1() int64 {
	panic("implement me")
}

func (f floatSumValue) int2() int64 {
	panic("implement me")
}

func (f floatSumValue) Float() float64 {
	return float64(f)
}

func (f floatSumValue) float1() float64 {
	return f.Float()
}

func (f floatSumValue) float2() float64 {
	panic("implement me")
}

func (f floatSumValue) Str() string {
	return strconv.FormatFloat(float64(f), 'f', 4, 64)
}

func (f floatSumValue) any2() interface{} {
	panic("implement me")
}

func (floatSumValues) Type() DataType {
	return TypeFloatSum
}
func (this floatSumValues) alloc(l int) []float64 {
	return floatSlicePool.Get().([]float64)[0:l]
}
func (this floatSumValues) release(v []float64) {
	floatSlicePool.Put(v)
}
func (floatSumValues) nil() float64 {
	return math.SmallestNonzeroFloat64
}
func (this floatSumValues) value(v float64) Value {
	return floatSumValue(v)
}
func (this floatSumValues) Resize(size int) Values {
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
func (this floatSumValues) add(v float64) Values {
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

func (this floatSumValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(this.nil()), nil
	} else {
		switch v.Type() & TypeMask {
		case TypeInt, TypeFloat, TypeTime:
			return this.add(v.float1()), nil
		case TypeStr:
			if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.add(f), nil
			} else {
				return this, errs.New(err)
			}
		}
		return this, errs.New("wrong data type!")
	}
}
func (this floatSumValues) set(merge bool, pos int, v float64) (Values, Cancel, error) {
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
func (this floatSumValues) Set(pos int, v Value, merge bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this.set(merge, pos, 0)
	}
	switch v.Type() & TypeMask {
	case TypeInt, TypeFloat, TypeTime:
		return this.set(merge, pos, v.Float())
	case TypeStr:
		if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
			return this.set(merge, pos, f)
		}
	}
	return this, nil, errs.New("data type error!")
}

func (this floatSumValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if n >= 0 && n < len(this.slices[m]) {
			return this.value(this.slices[m][n])
		}
	}
	return Nil
}
