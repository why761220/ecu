package ecu

import (
	"bufio"
	"io"
)

type nilValue struct{}

var Nil = nilValue{}

func (nilValue) Type() DataType {
	return TypeNil
}
func (nilValue) Format(string) string {
	return ""
}
func (nilValue) Base() interface{} {
	return nil
}

func (nilValue) Nil() bool {
	return true
}

func (nilValue) Bool() bool {
	panic("implement me")
}

func (n nilValue) Int() int64 {
	return 0
}

func (n nilValue) Float() float64 {
	panic("implement me")
}

func (n nilValue) Str() string {
	return "nil"
}

func (n nilValue) int1() int64 {
	panic("implement me")
}

func (n nilValue) int2() int64 {
	panic("implement me")
}

func (n nilValue) float1() float64 {
	panic("implement me")
}

func (n nilValue) float2() float64 {
	panic("implement me")
}

func (n nilValue) any2() interface{} {
	panic("implement me")
}

type nilValues struct {
	values
}

func (this nilValues) ToSlice(rows Positions) interface{} {
	return make([]interface{}, rows.Len())
}

func (this nilValues) init() (Values, error) {
	return this, nil
}

func (this nilValues) load(r *bufio.Reader, max int, add bool) (Values, error) {
	return this, nil
}

func (this nilValues) Save(w io.Writer) error {
	return this.json(w)
}

func (nilValues) Type() DataType {
	return TypeNil
}
func (this nilValues) Add(getter Getter) (Values, error) {
	if v, err := this.GetFrom(getter); err != nil {
		return this, err
	} else if v == nil || v.Type() == TypeNil {
		this.Size++
		return this, nil
	} else {
		this.DataType = v.Type()
		return newValues(this.values).Add(getter)
	}
}
func (this nilValues) Set(pos int, v Value, merge bool) (Values, Cancel, error) {
	if v == nil {
		return this, func() {}, nil
	} else {
		this.DataType = v.Type()
		return newValues(this.values).Set(pos, v, merge)
	}
}

func (this nilValues) Resize(size int) Values {
	this.Size = size
	return this
}

func (this nilValues) Get(int) Value {
	return Nil
}
