package ecu

import (

	"bufio"
	"bytes"
	"io"
	"strconv"
)

type distinctValue map[string]struct{}

func (d distinctValue) Type() DataType {
	return TypeDistinct
}

func (d distinctValue) Base() interface{} {
	return d.Int()
}

func (d distinctValue) Nil() bool {
	return len(d) == 0
}

func (d distinctValue) Bool() bool {
	panic("implement me")
}

func (d distinctValue) Int() int64 {
	return int64(len(d))
}

func (d distinctValue) Float() float64 {
	return float64(d.Int())
}

func (d distinctValue) Str() string {
	return strconv.FormatInt(d.Int(), 10)
}

func (d distinctValue) int1() int64 {
	return d.Int()
}

func (d distinctValue) int2() int64 {
	panic("implement me")
}

func (d distinctValue) float1() float64 {
	return d.Float()
}

func (d distinctValue) float2() float64 {
	panic("implement me")
}

func (d distinctValue) any2() interface{} {
	panic("implement me")
}

func (d distinctValue) Format(fmt string) string {
	return FmtInt(d.Int(), fmt)
}

type distinctValues struct {
	values
	slices [][]distinctValue
}

func (this distinctValues) ToSlice(rows Positions) interface{} {
	ret := make([]int64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Int())
		return true
	})
	return ret
}

func (this distinctValues) init() (Values, error) {
	return this, nil
}
func loadDistincts(alloc func(int) []distinctValue, old [][]distinctValue, r *bufio.Reader, max int) (slices [][]distinctValue, size int, err error) {
	var bs []byte
	m := len(old) - 1
	slices = old
	if m < 0 {
		m, slices = 0, append(make([][]distinctValue, 0, 32), alloc(0))
	}
	for size = 0; size < max; size++ {
		if bs, _, err = r.ReadLine(); err != nil {
			return
		}
		if len(slices[m]) >= VarsSliceSize {
			m, slices = m+1, append(slices, alloc(0))
		}
		vs, i := map[string]struct{}{}, 0
		for {
			if t := bytes.IndexByte(bs[i:], ','); t > 0 {
				vs[string(bs[i:i+t])] = struct{}{}
				i = i + t + 1
			} else {
				vs[string(bs[i:])] = struct{}{}
				break
			}
		}
		slices[m] = append(slices[m], vs)
	}
	return
}
func storeDistincts(slices [][]distinctValue, w io.Writer) (err error) {
	var first bool
	for _, slices := range slices {
		for _, vs := range slices {
			first = true
			for k := range vs {
				if first {
					first = false
				} else if _, err = w.Write([]byte{','}); err != nil {
					return
				}
				if _, err = w.Write([]byte(k)); err != nil {
					return
				}
			}
			if _, err = w.Write([]byte{'\n'}); err != nil {
				return
			}
		}
	}
	return
}
func (this distinctValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	if add {
		if slices, size, err := loadDistincts(this.alloc, this.slices, r, max); err == nil {
			this.slices, this.Size = slices, this.Size+size
			return this, nil
		} else {
			this.slices = slices
			return this.Resize(this.Size), err
		}
	} else if slices, size, err := loadDistincts(this.alloc, nil, r, max); err == nil {
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

func (this distinctValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	} else {
		return storeDistincts(this.slices, w)
	}
}

func (distinctValues) Type() DataType {
	return TypeDistinct
}
func (distinctValues) alloc(l int) []distinctValue {
	return distinctSlicePool.Get().([]distinctValue)[0:l]
}
func (distinctValues) release(v []distinctValue) {
	distinctSlicePool.Put(v)
}
func (distinctValues) nil() distinctValue {
	return map[string]struct{}{}
}
func (this distinctValues) Resize(size int) Values {
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

func (this distinctValues) Get(pos int) Value {
	if m, n := mn(pos); m < len(this.slices) {
		if slices := this.slices[m]; n < len(slices) {
			return slices[n]
		}
	}
	return Nil
}
func (this distinctValues) set(pos int, v string) (Values, Cancel, error) {
	if m, n := mn(pos); m < len(this.slices) {
		if slices := this.slices[m]; n < len(slices) {
			if _, ok := slices[n][v]; !ok {
				slices[n][v] = struct{}{}
				return this, func() { delete(slices[n], v) }, nil
			} else {
				return this, func() {}, nil
			}
		}
	}
	return nil, func() {}, errs.New("wrong data type!")
}
func (this distinctValues) set2(merge bool, pos int, vs map[string]struct{}) (Values, Cancel, error) {
	if len(vs) == 0 {
		return this, func() {}, nil
	}
	if m, n := mn(pos); m < len(this.slices) {
		if slices := this.slices[m]; n < len(slices) {
			old := slices[n]
			if merge {
				nv := make(map[string]struct{})
				for k := range old {
					nv[k] = struct{}{}
				}
				for k := range vs {
					nv[k] = struct{}{}
				}
				slices[n] = nv
			} else {
				slices[n] = vs
			}
			return this, func() { slices[n] = old }, nil
		}
	}
	return nil, func() {}, errs.New("wrong data type!")
}
func (this distinctValues) Set(pos int, v Value, merge bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this, func() {}, nil
	} else if v.Type() == TypeDistinct {
		return this.set2(merge, pos, v.(distinctValue))
	} else {
		switch v.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat, TypeStr:
			return this.set(pos, v.Str())
		default:
			return this, func() {}, errs.New("wrong data type!")
		}
	}
}
func (this distinctValues) add(v map[string]struct{}) Values {
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
func (this distinctValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(map[string]struct{}{}), nil
	} else if v.Type() == TypeDistinct {
		vs := make(map[string]struct{})
		for k := range v.(distinctValue) {
			vs[k] = struct{}{}
		}
		return this.add(vs), nil
	} else {
		switch v.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat, TypeStr:
			return this.add(map[string]struct{}{v.Str(): {}}), nil
		default:
			return this, errs.New("wrong data type!")
		}
	}
}
