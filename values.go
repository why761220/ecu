package ecu

import (

	"bufio"
	"encoding/json"
	"io"
	"strings"
	"time"
)

type DataType int

func (d *DataType) UnmarshalJSON(v []byte) error {
	switch strings.ToLower(string(v)) {
	case `"nil"`:
		*d = TypeNil
	case `"bool"`:
		*d = TypeBool
	case `"int"`:
		*d = TypeInt
	case `"float"`:
		*d = TypeFloat
	case `"str"`:
		*d = TypeStr
	case `"time"`:
		*d = TypeTime
	case `"intavg"`, `"avg"`:
		*d = TypeIntAvg
	case `"floatavg"`:
		*d = TypeFloatAvg
	case `"count"`:
		*d = TypeCount
	case `"floatmax"`:
		*d = TypeFloatMax
	case `"floatmin"`:
		*d = TypeFloatMin
	case `"intmin"`, `"min"`:
		*d = TypeIntMin
	case `"intmax"`, `"max"`:
		*d = TypeIntMax
	case `"floatsum"`:
		*d = TypeFloatSum
	case `"intsum"`, `"sum"`:
		*d = TypeIntSum
	case `"distinct"`:
		*d = TypeDistinct
	default:
		*d = TypeNil
	}
	return nil
}

func (d DataType) MarshalJSON() ([]byte, error) {
	switch d {
	case TypeNil:
		return []byte(`"nil"`), nil
	case TypeBool:
		return []byte(`"bool"`), nil
	case TypeInt:
		return []byte(`"int"`), nil
	case TypeFloat:
		return []byte(`"float"`), nil
	case TypeStr:
		return []byte(`"str"`), nil
	case TypeTime:
		return []byte(`"time"`), nil
	case TypeIntAvg:
		return []byte(`"intavg"`), nil
	case TypeFloatAvg:
		return []byte(`"floatavg"`), nil
	case TypeCount:
		return []byte(`"count"`), nil
	case TypeFloatMax:
		return []byte(`"floatmax"`), nil
	case TypeFloatMin:
		return []byte(`"floatmin"`), nil
	case TypeIntMin:
		return []byte(`"intmin"`), nil
	case TypeIntMax:
		return []byte(`"intmax"`), nil
	case TypeFloatSum:
		return []byte(`"floatsum"`), nil
	case TypeIntSum:
		return []byte(`"intsum"`), nil
	case TypeDistinct:
		return []byte(`"distinct"`), nil
	default:
		return nil, errs.New("unkown data type!")
	}
}

const TypeMask = 255 //0b11111111
const (
	IsNil   = 0
	IsBool  = 1 << 1
	IsInt   = 1 << 2
	IsFloat = 1 << 3
	IsStr   = 1 << 4
	IsTime  = 1 << 5
)
const (
	IsAgg      = 1 << 8
	IsMax      = 1 << 9
	IsMin      = 1 << 10
	IsSum      = 1 << 11
	IsAvg      = 1 << 12
	IsCount    = 1 << 13
	IsDistinct = 1 << 14
)
const (
	TypeNil      DataType = IsNil
	TypeBool              = IsBool
	TypeInt               = IsInt
	TypeFloat             = IsFloat
	TypeStr               = IsStr
	TypeTime              = IsTime
	TypeIntAvg            = IsInt | IsAgg | IsAvg
	TypeFloatAvg          = IsFloat | IsAgg | IsAvg
	TypeCount             = IsInt | IsAgg | IsCount
	TypeFloatMax          = IsFloat | IsAgg | IsMax
	TypeFloatMin          = IsFloat | IsAgg | IsMin
	TypeIntMin            = IsInt | IsAgg | IsMin
	TypeIntMax            = IsInt | IsAgg | IsMax
	TypeFloatSum          = IsFloat | IsAgg | IsSum
	TypeIntSum            = IsInt | IsAgg | IsSum
	TypeDistinct          = IsInt | IsAgg | IsDistinct
)

type (
	Value interface {
		Type() DataType
		Base() interface{}
		Nil() bool
		Bool() bool
		Int() int64
		Float() float64
		Str() string
		Format(fmt string) string
		int1() int64
		int2() int64
		float1() float64
		float2() float64
		any2() interface{}
	}
	Values interface {
		Type() DataType
		GetName() string
		Count() int
		IsForbid() bool
		IsIndex() bool
		GetKind() ColType
		Format() string
		GetFrom(getter Getter) (Value, error)
		Periods() (int64, int64)
		////////////
		Get(pos int) Value
		Add(getter Getter) (Values, error)
		Set(pos int, v Value, merge bool) (Values, Cancel, error)
		Resize(size int) Values
		Save(w io.Writer) error
		////////////////
		load(r *bufio.Reader, max int, add bool) (Values, error)
		init() (Values, error)
		base() values
		ToSlice(rows Positions) interface{}
	}
)
type Cancel = func()
type values struct {
	DataType DataType `json:"type,omitempty"`
	Kind     ColType  `json:"kind,omitempty"`
	Name     string   `json:"name,omitempty"`
	Field    string   `json:"field,omitempty"`
	Comment  string   `json:"comment,omitempty"`
	Index    bool     `json:"index,omitempty"`
	Forbid   bool     `json:"forbid,omitempty"`
	Prefix   string   `json:"prefix,omitempty"`
	FReq     *FReq    `json:"freq,omitempty"`
	Size     int      `json:"size,omitempty"`
	Fmt      string   `json:"fmt,omitempty"`
	encoder  string   `json:"encoder,omitempty"`
}

func (this values) Format() string {
	return this.Fmt
}
func (this values) base() values {
	return this
}
func (this values) Type() DataType {
	return this.DataType
}
func (this values) IsForbid() bool {
	return this.Forbid
}
func (this values) IsIndex() bool {
	return this.Index
}
func (this values) GetKind() ColType {
	return this.Kind
}

func (this values) json(w io.Writer) error {
	return json.NewEncoder(w).Encode(this)
}
func (this values) GetFrom(getter Getter) (v Value, err error) {
	if v, err = getter(this.Name); err != nil {
		return
	} else if v == nil && this.Field != "" {
		return getter(this.Field)
	} else {
		return
	}
}
func (this values) GetName() string {
	return this.Name
}
func (this values) Count() int {
	return this.Size
}
func (this values) Periods() (int64, int64) {
	return 0, 0
}

func mn(pos int) (int, int) {
	return pos / VarsSliceSize, pos % VarsSliceSize
}

func newValues(base values) Values {
	base.Size = 0
	if base.FReq == nil {
		base.FReq = defFReq
	} else {
		base.FReq = base.FReq.init(base.Fmt)
	}
	switch base.DataType {
	case TypeBool:
		return boolValues{values: base}
	case TypeInt:
		return intValues{values: base}
	case TypeFloat:
		return floatValues{values: base}
	case TypeStr:
		return strValues{values: base}
	case TypeTime:
		return timeValues{values: base}
	case TypeIntAvg:
		return intAvgValues{values: base}
	case TypeFloatAvg:
		return floatAvgValues{values: base}
	case TypeCount:
		return countValues{values: base}
	case TypeFloatMax:
		return floatMaxValues{values: base}
	case TypeFloatMin:
		return floatMinValues{values: base}
	case TypeIntMax:
		return intMaxValues{values: base}
	case TypeIntMin:
		return intMinValues{values: base}
	case TypeFloatSum:
		return floatSumValues{values: base}
	case TypeIntSum:
		return intSumValues{values: base}
	case TypeDistinct:
		return distinctValues{values: base}
	default:
		return nilValues{values: base}
	}
}

func NewValues(name string, field string, dt DataType, size int, fmt string) Values {
	return newValues(values{Name: name, Field: field, DataType: dt, Size: 0, Fmt: fmt}).Resize(size)
}

func NewValue(v interface{}) (Value, error) {
	switch v := v.(type) {
	case nil:
		return Nil, nil
	case bool:
		if v {
			return boolValue(1), nil
		} else {
			return boolValue(0), nil
		}
	case time.Time:
		return timeValue(v.UnixNano()), nil
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return intValue(i), nil
		} else if f, err := v.Float64(); err == nil {
			return floatValue(f), nil
		} else {
			return strValue(v), nil
		}
	case int8:
		return intValue(v), nil
	case uint8:
		return intValue(v), nil
	case int16:
		return intValue(v), nil
	case uint16:
		return intValue(v), nil
	case int32:
		return intValue(v), nil
	case uint32:
		return intValue(v), nil
	case int:
		return intValue(v), nil
	case uint:
		return intValue(v), nil
	case int64:
		return intValue(v), nil
	case uint64:
		return intValue(v), nil
	case float32:
		return floatValue(v), nil
	case float64:
		return floatValue(v), nil
	case string:
		return strValue(v), nil
	case Value:
		return v, nil
	default:
		return nil, errs.New("type is error!")
	}
}
