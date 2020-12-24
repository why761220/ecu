package ecu

import (

	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
)

type floatValue float64

func FmtFloat(f float64, fmt string) string {
	if math.IsNaN(f) {
		return ""
	}
	switch fmt {
	case "f0":
		return strconv.FormatFloat(f, 'f', 0, 64)
	case "f1":
		return strconv.FormatFloat(f, 'f', 1, 64)
	case "f2":
		return strconv.FormatFloat(f, 'f', 2, 64)
	case "f3":
		return strconv.FormatFloat(f, 'f', 3, 64)
	case "f4":
		return strconv.FormatFloat(f, 'f', 4, 64)
	case "f5":
		return strconv.FormatFloat(f, 'f', 5, 64)
	case "f6":
		return strconv.FormatFloat(f, 'f', 6, 64)
	default:
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
}
func (f floatValue) Format(fmt string) string {
	return FmtFloat(f.Float(), fmt)
}

func (f floatValue) Type() DataType {
	return TypeFloat
}

func (f floatValue) Base() interface{} {
	if f.Nil() {
		return nil
	}
	return f.Float()
}

func (f floatValue) Nil() bool {
	return math.IsNaN(float64(f))
}

func (f floatValue) Bool() bool {
	return f == 1
}

func (f floatValue) Int() int64 {
	return int64(f)
}
func (f floatValue) Float() float64 {
	return float64(f)
}
func (f floatValue) Str() string {
	if f.Nil() {
		return ""
	}
	return strconv.FormatFloat(float64(f), 'f', 4, 64)
}

func (f floatValue) int1() int64 {
	return f.Int()
}
func (f floatValue) int2() int64 {
	panic("implement me")
}
func (f floatValue) float1() float64 {
	return f.Float()
}

func (f floatValue) float2() float64 {
	panic("implement me")
}

func (f floatValue) any2() interface{} {
	panic("implement me")
}

type floatValues struct {
	values
	slices [][]float64
}

func (this floatValues) ToSlice(rows Positions) interface{} {
	ret := make([]float64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Float())
		return true
	})
	return ret
}

func (this floatValues) init() (Values, error) {
	return this, nil
}

func storeFloats(slices [][]float64, w io.Writer) (err error) {
	bs := make([]byte, 8)
	for _, s := range slices {
		for _, f := range s {
			binary.BigEndian.PutUint64(bs, math.Float64bits(f))
			if _, err = w.Write(bs); err != nil {
				return
			}
		}
	}
	return
}
func loadFloats(alloc func(int) []float64, old [][]float64, r io.Reader, max int) (slices [][]float64, size int, err error) {
	var bs [8]byte
	m := len(old) - 1
	slices = old
	if m < 0 {
		m, slices = 0, append(make([][]float64, 0, 32), alloc(0))
	}
	for size = 0; size < max; size++ {
		if _, err = io.ReadFull(r, bs[:]); err != nil {
			return
		}
		if len(slices[m]) >= VarsSliceSize {
			m, slices = m+1, append(slices, alloc(0))
		}
		slices[m] = append(slices[m], math.Float64frombits(binary.BigEndian.Uint64(bs[:])))
	}
	return
}

func (this floatValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
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

func (this floatValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	} else {
		return storeFloats(this.slices, w)
	}
}

func (floatValues) Type() DataType {
	return TypeFloat
}

///////////////////////////////////////////////////////////////////////////////
func (this floatValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(this.nil()), nil
	} else {
		switch v.Type() & TypeMask {
		case TypeInt, TypeFloat, TypeTime:
			return this.add(v.Float()), nil
		case TypeStr:
			if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.add(f), nil
			}
		}
		return this, errs.New("data type error!")
	}

}
func (this floatValues) Set(pos int, v Value, _ bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this.set(pos, this.nil())
	} else {
		switch v.Type() & TypeMask {
		case TypeInt, TypeFloat, TypeTime:
			return this.set(pos, v.Float())
		case TypeStr:
			if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.set(pos, f)
			}
		}
		return this, nil, errs.New("data type error!")
	}
}

///////////////////////////////////////////////////////////////////////////////

func (this floatValues) newIndices() floatIndex {
	return make(floatIndex)
}
func (this floatValues) alloc(l int) []float64 {
	return floatSlicePool.Get().([]float64)[0:l]
}
func (this floatValues) release(v []float64) {
	floatSlicePool.Put(v)
}
func (floatValues) nil() float64 {
	return math.NaN()
}
func (this floatValues) value(v float64) Value {
	if v == this.nil() {
		return Nil
	} else {
		return floatValue(v)
	}
}

func (this floatValues) Resize(size int) Values {
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

func (this floatValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			return this.value(slices[n])
		}
	}
	return Nil
}

func (this floatValues) add(v float64) Values {
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

func (this floatValues) set(pos int, v float64) (Values, Cancel, error) {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			old := slices[n]
			slices[n] = v
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errors.New("index out of array")
}
