package ecu

import (

	"strconv"
)

const (
	OperInvalid OperType = iota
	Const
	Field
	SelectField
	All
	Sum
	Max
	Min
	Avg
	Count
	DistinctCount
	Multiple
	Divide
	Plus
	Minus
	Equal
	NotEqual
	Greater
	Lesser
	GreaterEqual
	LesserEqual
	Brackets
	IsNull
	IsNotNull
	Like
	In
	NotIn
	Between
	NotBetween
	Not
	And
	Or
	Timestamp
	ToFloat
	ToInt
	RecordValue
	IfNull
	Now
	LastRecord
	FirstRecord
)

type (
	OperType uint32
	Getter   func(name string) (Value, error)
	Setter   func(name string, value Value)

	Expr struct {
		Oper   OperType
		Args   []*Expr
		Prefix string
		Field  string
		Const  Value
		sel    *SelExpr
		err    error `json:"-"`
	}
	f0 func() (Value, error)
	f1 func(a Value) (Value, error)
	f2 func(a, b Value) (Value, error)
	f3 func(a, b, c Value) (Value, error)
	fn func(vs ...Value) (Value, error)
)

func (this Expr) Invalid() bool {
	panic("implement me")
}

func (this Expr) String() string {
	panic("implement me")
}
func (this Expr) Bool(getter Getter) (bool, error) {
	if v, err := this.DoExpr(getter, Nil); err != nil {
		return false, err
	} else if v == nil || v.Type() == TypeNil {
		return false, nil
	} else if (v.Type() & TypeMask) == TypeBool {
		return v.Bool(), err
	} else {
		return false, errs.New("return value is not bool!")
	}
}
func (this Expr) DoExpr(getter Getter, old Value) (Value, error) {
	switch this.Oper {
	case Const:
		return this.Const, nil
	case Field:
		if this.Field == "*" {
			panic("implement me")
		} else {
			return getter(this.Field)
		}
	case Multiple:
		return this.f2(getter, old, this.multiple)
	case Divide:
		return this.f2(getter, old, this.divide)
	case Plus:
		return this.f2(getter, old, this.plus)
	case Minus:
		return this.f2(getter, old, this.minus)
	case Equal:
		return this.f2(getter, old, this.equal)
	case NotEqual:
		return this.f2(getter, old, this.notEqual)
	case Greater:
		return this.f2(getter, old, this.greater)
	case Lesser:
		return this.f2(getter, old, this.lesser)
	case GreaterEqual:
		return this.f2(getter, old, this.greaterEqual)
	case LesserEqual:
		return this.f2(getter, old, this.LesserEqual)
	case Brackets:
		return this.f1(getter, old, this.brackets)
	case IsNull:
		return this.f1(getter, old, this.isNull)
	case IsNotNull:
		return this.f1(getter, old, this.isNotNull)
	case Like:
		return this.f1(getter, old, this.like)
	case In:
		return this.fn(getter, old, this.in)
	case NotIn:
		return this.fn(getter, old, this.notIn)
	case Between:
		return this.f3(getter, old, this.between)
	case NotBetween:
		return this.f3(getter, old, this.notBetween)
	case Not:
		return this.f1(getter, old, this.not)
	case And:
		return this.f2(getter, old, this.and)
	case Or:
		return this.f2(getter, old, this.or)
	case Timestamp:
		return this.f1(getter, old, this.timestamp)
	case ToFloat:
		return this.f1(getter, old, this.toFloat)
	case ToInt:
		return this.f1(getter, old, this.toInt)
	default:
		return nil, errs.New("oper not supported!")
	}
}
func (this Expr) f0(f f0) (Value, error) {
	return f()
}
func (this Expr) f1(getter Getter, old Value, f f1) (Value, error) {
	if a, ok1, err := this.GetArg(0, getter, old); err != nil {
		return old, err
	} else if ok1 {
		return f(a)
	} else {
		return old, errs.New("internal logic error!")
	}
}
func (this Expr) f2(getter Getter, old Value, f f2) (Value, error) {
	if a, ok1, err := this.GetArg(0, getter, old); err != nil {
		return old, err
	} else if b, ok2, err := this.GetArg(1, getter, old); err != nil {
		return old, err
	} else if ok1 && ok2 {
		return f(a, b)
	} else {
		return old, errs.New("internal logic error!")
	}
}
func (this Expr) f3(getter Getter, old Value, f f3) (Value, error) {
	if a, ok1, err := this.GetArg(0, getter, old); err != nil {
		return old, err
	} else if b, ok2, err := this.GetArg(1, getter, old); err != nil {
		return old, err
	} else if c, ok3, err := this.GetArg(2, getter, old); err != nil {
		return old, err
	} else if ok1 && ok2 && ok3 {
		return f(a, b, c)
	} else {
		return old, errs.New("internal logic error!")
	}
}
func (this Expr) fn(getter Getter, old Value, f fn) (Value, error) {
	var err error
	args := make([]Value, len(this.Args))
	for i, e := range this.Args {
		if args[i], err = e.DoExpr(getter, old); err != nil {
			return nil, err
		}
	}
	return f(args...)
}
func (this Expr) GetArg(index int, getter Getter, old Value) (Value, bool, error) {
	if index < len(this.Args) {
		r, e := this.Args[index].DoExpr(getter, old)
		return r, true, e
	} else {
		return nil, false, nil
	}
}

func s2i(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
func s2f(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
func b2v(b bool) boolValue {
	if b {
		return boolValue(1)
	} else {
		return boolValue(0)
	}
}
func s2b(s string) (bool, error) {
	return strconv.ParseBool(s)
}
