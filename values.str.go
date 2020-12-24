package ecu

import (

	"bufio"
	"errors"
	"io"
)

type strValue string

func (s strValue) Format(string) string {
	return string(s)
}

func (s strValue) Type() DataType {
	return TypeStr
}

func (s strValue) Base() interface{} {
	if s.Nil() {
		return nil
	}
	return s.Str()
}

func (s strValue) Nil() bool {
	return s == ""
}

func (s strValue) Bool() bool {
	panic("implement me")
}
func (s strValue) Str() string {
	return string(s)
}
func (s strValue) Int() int64 {
	panic("implement me")
}
func (s strValue) int1() int64 {
	panic("implement me")
}
func (s strValue) int2() int64 {
	panic("implement me")
}
func (s strValue) Float() float64 {
	panic("implement me")
}
func (s strValue) float1() float64 {
	panic("implement me")
}
func (s strValue) float2() float64 {
	panic("implement me")
}
func (s strValue) any2() interface{} {
	panic("implement me")
}

type strValues struct {
	values
	slices [][]string
}

func (this strValues) ToSlice(rows Positions) interface{} {
	ret := make([]string, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Str())
		return true
	})
	return ret
}

func (this strValues) init() (Values, error) {
	return this, nil
}
func loadStrs(alloc func(int) []string, old [][]string, r *bufio.Reader, max int) (slices [][]string, size int, err error) {
	var bs []byte
	m := len(old) - 1
	slices = old
	if m < 0 {
		m, slices = 0, append(make([][]string, 0, 32), alloc(0))
	}
	for size = 0; size < max; size++ {
		if bs, _, err = r.ReadLine(); err != nil {
			return
		}
		if len(slices[m]) >= VarsSliceSize {
			m, slices = m+1, append(slices, alloc(0))
		}
		slices[m] = append(slices[m], string(bs))
	}
	return
}
func (this strValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	if add {
		if slices, size, err := loadStrs(this.alloc, this.slices, r, max); err == nil {
			this.slices, this.Size = slices, this.Size+size
			return this, nil
		} else {
			this.slices = slices
			return this.Resize(this.Size), err
		}
	} else if slices, size, err := loadStrs(this.alloc, nil, r, max); err == nil {
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

func (this strValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	}
	for _, slices := range this.slices {
		for i := range slices {
			if _, err = w.Write([]byte(slices[i])); err != nil {
				return
			}
			if _, err = w.Write([]byte{'\n'}); err != nil {
				return
			}
		}
	}
	return
}

func (strValues) Type() DataType {
	return TypeStr
}
func (strValues) alloc(l int) []string {
	return strSlicePool.Get().([]string)[0:l]
}
func (strValues) release(v []string) {
	strSlicePool.Put(v)
}
func (strValues) nil() string {
	return ""
}
func (this strValues) value(v string) Value {
	if v == this.nil() {
		return Nil
	} else {
		return strValue(v)
	}
}
func (this strValues) Resize(size int) Values {
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

func (this strValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(this.nil()), nil
	} else if v.Type() == TypeStr {
		return this.add(v.Str()), nil
	} else {
		return this, errs.New("internal logic error!")
	}
}

func (this strValues) Set(pos int, v Value, _ bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this.set(pos, this.nil())
	} else {
		return this.set(pos, v.Str())
	}
}

func (this strValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			return this.value(slices[n])
		}
	}
	return Nil
}

func (this strValues) add(v string) Values {
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

func (this strValues) set(pos int, v string) (Values, Cancel, error) {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			old := slices[n]
			slices[n] = v
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errors.New("index out of array")
}
