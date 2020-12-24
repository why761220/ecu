package ecu

import (

	"bufio"
	"encoding/binary"
	"io"
	"math"
	"strconv"
)

type (
	floatAvgValue struct {
		total float64
		count float64
	}
	floatAvgValues struct {
		values
		slices [][]floatAvgValue
	}
)

func (this floatAvgValues) ToSlice(rows Positions) interface{} {
	ret := make([]float64, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Float())
		return true
	})
	return ret
}

func (f floatAvgValue) Format(fmt string) string {
	return FmtFloat(f.Float(), fmt)
}

func (this floatAvgValues) init() (Values, error) {
	return this, nil
}

func loadFloatAvgs(alloc func(int) []floatAvgValue, old [][]floatAvgValue, r io.Reader, max int) (slices [][]floatAvgValue, size int, err error) {
	var total, count float64
	var bs [16]byte
	m := len(old) - 1
	slices = old
	if m < 0 {
		m, slices = 0, append(make([][]floatAvgValue, 0, 32), alloc(0))
	}
	for size = 0; size < max; size++ {
		if _, err = io.ReadFull(r, bs[:]); err != nil {
			return slices, size, err
		}
		total = math.Float64frombits(binary.BigEndian.Uint64(bs[0:8]))
		count = math.Float64frombits(binary.BigEndian.Uint64(bs[8:16]))
		if len(slices[m]) >= VarsSliceSize {
			m, slices = m+1, append(slices, alloc(0))
		}
		slices[m] = append(slices[m], floatAvgValue{total: total, count: count})
	}
	return slices, size, err
}
func (this floatAvgValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	if add {
		if slices, size, err := loadFloatAvgs(this.alloc, this.slices, r, max); err == nil {
			this.slices, this.Size = slices, this.Size+size
			return this, nil
		} else {
			this.slices = slices
			return this.Resize(this.Size), err
		}
	} else if slices, size, err := loadFloatAvgs(this.alloc, nil, r, max); err == nil {
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

func (this floatAvgValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	}
	buf := make([]byte, 8)
	for _, slices := range this.slices {
		for i := range slices {
			binary.BigEndian.PutUint64(buf, math.Float64bits(slices[i].total))
			if _, err = w.Write(buf); err != nil {
				return
			}
			binary.BigEndian.PutUint64(buf, math.Float64bits(slices[i].count))
			if _, err = w.Write(buf); err != nil {
				return
			}
		}
	}
	return
}

func (f floatAvgValue) Type() DataType {
	return TypeFloatAvg
}

func (f floatAvgValue) Base() interface{} {
	if f.Nil() {
		return nil
	} else {
		return f.Float()
	}
}

func (f floatAvgValue) Nil() bool {
	return f.count == 0
}

func (f floatAvgValue) Bool() bool {
	panic("implement me")
}

func (f floatAvgValue) Int() int64 {
	return int64(f.Float())
}
func (f floatAvgValue) int1() int64 {
	return int64(f.total)
}

func (f floatAvgValue) int2() int64 {
	return int64(f.count)
}
func (f floatAvgValue) Float() float64 {
	if f.count == 0 {
		return 0
	} else {
		return f.total / f.count
	}
}
func (f floatAvgValue) float1() float64 {
	return f.total
}
func (f floatAvgValue) float2() float64 {
	return f.count
}
func (f floatAvgValue) Str() string {
	if f.Nil() {
		return ""
	} else {
		return strconv.FormatFloat(f.Float(), 'f', 4, 64)
	}
}

func (f floatAvgValue) any2() interface{} {
	panic("implement me")
}

func (floatAvgValues) alloc(l int) []floatAvgValue {
	return floatAvgSlicePool.Get().([]floatAvgValue)[0:l]
}
func (floatAvgValues) release(v []floatAvgValue) {
	floatAvgSlicePool.Put(v)
}
func (floatAvgValues) nil() floatAvgValue {
	return floatAvgValue{}
}
func (this floatAvgValues) Resize(size int) Values {
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

func (this floatAvgValues) Get(pos int) Value {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if n >= 0 && n < len(this.slices[m]) {
			return this.slices[m][n]
		}
	}
	return Nil
}
func (this floatAvgValues) add(v floatAvgValue) (floatAvgValues, error) {
	if m := len(this.slices); m <= 0 {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else if slices := this.slices[m-1]; len(slices) >= VarsSliceSize {
		this.slices = append(this.slices, append(this.alloc(0), v))
	} else {
		this.slices[m-1] = append(slices, v)
	}
	this.Size = this.Size + 1
	return this, nil
}
func (this floatAvgValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(floatAvgValue{})
	} else {
		switch v.Type() & TypeMask {
		case TypeInt, TypeFloat:
			return this.add(floatAvgValue{total: v.Float(), count: 1})
		case TypeIntAvg, TypeFloatAvg:
			return this.add(floatAvgValue{total: v.float1(), count: v.float2()})
		case TypeStr:
			if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
				return this.add(floatAvgValue{total: f, count: 1})
			}
		}
		return this, errs.New("wrong data type!")
	}

}
func (this floatAvgValues) set(agg bool, pos int, total, count float64) (Values, Cancel, error) {
	if m, n := mn(pos); m >= 0 && m < len(this.slices) {
		if slices := this.slices[m]; n >= 0 && n < len(slices) {
			old := slices[n]
			if agg {
				slices[n] = floatAvgValue{total: old.total + total, count: old.count + count}
			} else {
				slices[n] = floatAvgValue{total: total, count: count}
			}
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errs.New("index out of array!")
}
func (this floatAvgValues) Set(pos int, v Value, merge bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this.set(merge, pos, 0, 0)
	}
	switch v.Type() {
	case TypeInt, TypeFloat:
		return this.set(merge, pos, v.Float(), 1)
	case TypeIntAvg, TypeFloatAvg:
		return this.set(merge, pos, v.float1(), v.float2())
	case TypeStr:
		if f, err := strconv.ParseFloat(v.Str(), 64); err == nil {
			return this.set(merge, pos, f, 1)
		}
	}
	return this, nil, errs.New("wrong data type!")
}
