package ecu

import (

	"bufio"
	"io"
	"strconv"
)

type (
	boolValue  byte
	boolValues struct {
		values
		slices [][]byte
	}
)

func (this boolValues) ToSlice(rows Positions) interface{} {
	ret := make([]bool, 0, rows.Len())
	rows.Range(func(pos int) bool {
		ret = append(ret, this.Get(pos).Bool())
		return true
	})
	return ret
}

func (v boolValue) Format(string) string {
	if v.Nil() {
		return ""
	} else {
		return strconv.FormatBool(v.Bool())
	}
}

func (this boolValues) init() (Values, error) {
	return this, nil
}
func loadBytes(alloc func(int) []byte, old [][]byte, r io.Reader, max int) (slices [][]byte, size int, err error) {
	var t, m, n, i, j int
	if m = len(old) - 1; m >= 0 {
		slices, i = old, len(slices[m])
		if n = VarsSliceSize - i; n < max {
			j = i + n
		} else {
			j = i + max
		}
		if t, err = io.ReadFull(r, slices[m][i:j]); err != nil {
			return
		} else {
			size = t
		}
		m = m + 1
	} else {
		m, slices = 0, append(make([][]byte, 0, 32), alloc(0))
	}

	for size < max {
		if n = max - size; n > VarsSliceSize {
			n = VarsSliceSize
		}
		slices, m = append(slices, alloc(n)), m+1
		if t, err = io.ReadFull(r, slices[m]); err != nil {
			return
		} else {
			size += t
		}
	}
	return
}
func (this boolValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	if add {
		if slices, size, err := loadBytes(this.alloc, this.slices, r, max); err == nil {
			this.slices, this.Size = slices, this.Size+size
			return this, nil
		} else {
			this.slices = slices
			return this.Resize(this.Size), err
		}
	} else if slices, size, err := loadBytes(this.alloc, nil, r, max); err == nil {
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

func (this boolValues) Save(w io.Writer) (err error) {
	if err = this.json(w); err != nil {
		return
	}
	for _, slices := range this.slices {
		if _, err = w.Write(slices); err != nil {
			return
		}
	}
	return
}

func (boolValue) Type() DataType {
	return TypeBool
}

func (v boolValue) Base() interface{} {
	if v.Nil() {
		return nil
	}
	return v.Bool()
}

func (v boolValue) Nil() bool {
	return v != 2
}

func (v boolValue) Bool() bool {
	return v == 1
}

func (v boolValue) Int() int64 {
	return int64(v)
}

func (v boolValue) Float() float64 {
	panic("implement me")
}

func (v boolValue) Str() string {
	if v.Nil() {
		return ""
	} else {
		return strconv.FormatBool(v.Bool())
	}
}
func (v boolValue) int1() int64 {
	panic("implement me")
}
func (v boolValue) int2() int64 {
	panic("implement me")
}
func (v boolValue) float1() float64 {
	panic("implement me")
}
func (v boolValue) float2() float64 {
	panic("implement me")
}

func (v boolValue) any2() interface{} {
	panic("implement me")
}

func (boolValues) Type() DataType {
	return TypeBool
}
func (boolValues) alloc(l int) []byte {
	return boolSlicePool.Get().([]byte)[0:l]
}
func (boolValues) release(v []byte) {
	boolSlicePool.Put(v)
}
func (boolValues) nil() byte {
	return 2
}
func (this boolValues) Resize(size int) Values {
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

func (this boolValues) add(v byte) boolValues {
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
func (this boolValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		return this.add(this.nil()), nil
	} else if v.Type() == TypeBool {
		return this.add(byte(v.Int())), nil
	} else if v.Type() == TypeStr {
		return this.b2s().add(v.Str()), nil
	} else {
		return this, errs.New("wrong data type!")
	}
}
func (this boolValues) b2s() (ret strValues) {
	ret.values = this.values
	ret.slices = make([][]string, len(this.slices))
	for i, src := range this.slices {
		dst := ret.alloc(len(src))
		for j, v := range src {
			if v == this.nil() {
				dst[j] = ret.nil()
			} else if v == 1 {
				dst[j] = "true"
			} else {
				dst[j] = "false"
			}
		}
		ret.slices[i] = dst
		this.release(src)
	}
	this.Size, this.slices = 0, nil
	return

}
func (this boolValues) Set(pos int, v Value, _ bool) (Values, Cancel, error) {
	if v == nil || v.Type() == TypeNil {
		return this.merge(pos, this.nil())
	} else if v.Type() == TypeBool {
		return this.merge(pos, byte(v.Int()))
	} else {
		return this, nil, errs.New("wrong data type!")
	}
}
func (this boolValues) Get(pos int) Value {
	if m, n := mn(pos); m < len(this.slices) {
		if slices := this.slices[m]; n < len(slices) {
			return boolValue(slices[n])
		}
	}
	return Nil
}

func (this boolValues) merge(pos int, v byte) (Values, Cancel, error) {
	if m, n := mn(pos); m < len(this.slices) {
		if slices := this.slices[m]; n < len(slices) {
			old := slices[n]
			slices[n] = v
			return this, func() { slices[n] = old }, nil
		}
	}
	return this, nil, errs.New("wrong data type!")
}
