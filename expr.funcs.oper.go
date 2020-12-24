package ecu

import (

	"strconv"
)

func (this Expr) multiple(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt:
		switch b.Type() {
		case TypeInt:
			return intValue(a.int1() * b.int1()), nil
		case TypeFloat:
			return floatValue(a.float1() * b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return intValue(a.int1() * i), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() * f), nil
			}
		}
	case TypeFloat:
		switch b.Type() {
		case TypeInt:
			return floatValue(a.float1() * b.float1()), nil
		case TypeFloat:
			return floatValue(a.float1() * b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return floatValue(a.float1() * float64(i)), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() * f), nil
			}
		}
	case TypeStr:
		if i1, err := strconv.ParseInt(a.Str(), 10, 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return intValue(i1 * b.int1()), nil
			case TypeFloat:
				return floatValue(float64(i1) * b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return intValue(i1 * i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(float64(i1) * f2), nil
				}
			}
		} else if f1, err := strconv.ParseFloat(a.Str(), 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return floatValue(f1 * b.float1()), nil
			case TypeFloat:
				return floatValue(f1 * b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return floatValue(f1 * float64(i2)), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(f1 * f2), nil
				}
			}
		} else {
			return nil, err
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) divide(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt:
		switch b.Type() {
		case TypeInt:
			return intValue(a.int1() / b.int1()), nil
		case TypeFloat:
			return floatValue(a.float1() / b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return intValue(a.int1() / i), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() / f), nil
			}
		}
	case TypeFloat:
		switch b.Type() {
		case TypeInt:
			return floatValue(a.float1() / b.float1()), nil
		case TypeFloat:
			return floatValue(a.float1() / b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return floatValue(a.float1() / float64(i)), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() / f), nil
			}
		}
	case TypeStr:
		if i1, err := strconv.ParseInt(a.Str(), 10, 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return intValue(i1 / b.int1()), nil
			case TypeFloat:
				return floatValue(float64(i1) / b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return intValue(i1 / i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(float64(i1) / f2), nil
				}
			}
		} else if f1, err := strconv.ParseFloat(a.Str(), 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return floatValue(f1 / b.float1()), nil
			case TypeFloat:
				return floatValue(f1 / b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return floatValue(f1 / float64(i2)), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(f1 / f2), nil
				}
			}
		} else {
			return nil, err
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) plus(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt:
		switch b.Type() {
		case TypeInt:
			return intValue(a.int1() + b.int1()), nil
		case TypeFloat:
			return floatValue(a.float1() + b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return intValue(a.int1() + i), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() + f), nil
			}
		}
	case TypeFloat:
		switch b.Type() {
		case TypeInt:
			return floatValue(a.float1() + b.float1()), nil
		case TypeFloat:
			return floatValue(a.float1() + b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return floatValue(a.float1() + float64(i)), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() + f), nil
			}
		}
	case TypeStr:
		if i1, err := strconv.ParseInt(a.Str(), 10, 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return intValue(i1 + b.int1()), nil
			case TypeFloat:
				return floatValue(float64(i1) + b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return intValue(i1 + i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(float64(i1) + f2), nil
				}
			}
		} else if f1, err := strconv.ParseFloat(a.Str(), 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return floatValue(f1 + b.float1()), nil
			case TypeFloat:
				return floatValue(f1 + b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return floatValue(f1 + float64(i2)), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(f1 + f2), nil
				}
			}
		} else {
			return nil, err
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) minus(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt:
		switch b.Type() {
		case TypeInt:
			return intValue(a.int1() - b.int1()), nil
		case TypeFloat:
			return floatValue(a.float1() - b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return intValue(a.int1() - i), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() - f), nil
			}
		}
	case TypeFloat:
		switch b.Type() {
		case TypeInt:
			return floatValue(a.float1() - b.float1()), nil
		case TypeFloat:
			return floatValue(a.float1() - b.float1()), nil
		case TypeStr:
			if i, err := s2i(b.Str()); err == nil {
				return floatValue(a.float1() - float64(i)), nil
			} else if f, err := s2f(b.Str()); err == nil {
				return floatValue(a.float1() - f), nil
			}
		}
	case TypeStr:
		if i1, err := strconv.ParseInt(a.Str(), 10, 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return intValue(i1 - b.int1()), nil
			case TypeFloat:
				return floatValue(float64(i1) - b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return intValue(i1 - i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(float64(i1) - f2), nil
				}
			}
		} else if f1, err := strconv.ParseFloat(a.Str(), 64); err == nil {
			switch b.Type() {
			case TypeInt:
				return floatValue(f1 - b.float1()), nil
			case TypeFloat:
				return floatValue(f1 - b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return floatValue(f1 - float64(i2)), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return floatValue(f1 - f2), nil
				}
			}
		} else {
			return nil, err
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) equal(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeBool:
		switch b.Type() & TypeMask {
		case TypeBool:
			return b2v(a.Bool() == b.Bool()), nil
		case TypeStr:
			if b1, err := s2b(a.Str()); err == nil {
				return b2v(b1 == b.Bool()), nil
			}
		}
	case TypeInt, TypeTime:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime:
			return b2v(a.int1() == b.int1()), nil
		case TypeFloat:
			return b2v(a.float1() == b.float1()), nil
		case TypeStr:
			if i2, err := s2i(b.Str()); err == nil {
				return b2v(a.int1() == i2), nil
			} else if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() == f2), nil
			}
		}
	case TypeFloat:
		switch b.Type() & TypeMask {
		case TypeInt, TypeFloat:
			return b2v(a.float1() == b.float1()), nil
		case TypeStr:
			if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() == f2), nil
			}
		}
	case TypeStr:
		switch b.Type() & TypeMask {
		case TypeBool:
			if a1, err := s2b(a.Str()); err == nil {
				return b2v(a1 == b.Bool()), nil
			} else {
				return b2v(false), errs.New(err)
			}
		case TypeInt, TypeTime:
			if a1, err := s2i(a.Str()); err == nil {
				return b2v(a1 == b.Int()), nil
			} else {
				return b2v(false), errs.New(err)
			}
		case TypeFloat:
			if a1, err := s2f(a.Str()); err == nil {
				return b2v(a1 == b.Float()), nil
			} else {
				return b2v(false), errs.New(err)
			}
		case TypeStr:
			return b2v(a.Str() == b.Str()), nil
		}
	}
	return nil, errs.New(a, b, "data type is error!")
}
func (this Expr) notEqual(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeBool:
		switch b.Type() & TypeMask {
		case TypeBool:
			return b2v(a.Bool() != b.Bool()), nil
		case TypeStr:
			if b1, err := s2b(a.Str()); err == nil {
				return b2v(b1 != b.Bool()), nil
			}
		}
	case TypeInt, TypeTime:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime:
			return b2v(a.int1() != b.int1()), nil
		case TypeFloat:
			return b2v(a.float1() != b.float1()), nil
		case TypeStr:
			if i2, err := s2i(b.Str()); err == nil {
				return b2v(a.int1() != i2), nil
			} else if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() != f2), nil
			}
		}
	case TypeFloat:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat:
			return b2v(a.float1() != b.float1()), nil
		case TypeStr:
			if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() != f2), nil
			}
		}
	case TypeStr:
		switch b.Type() & TypeMask {
		case TypeStr:
			return b2v(a.Str() != b.Str()), nil
		case TypeBool:
			if vb, err := strconv.ParseBool(a.Str()); err == nil {
				return b2v(vb != b.Bool()), nil
			} else {
				return b2v(true), nil
			}
		case TypeInt, TypeTime:
			if vi, err := strconv.ParseInt(a.Str(), 10, 64); err == nil {
				return b2v(vi != b.Int()), nil
			} else {
				return b2v(true), nil
			}
		case TypeFloat:
			if vf, err := strconv.ParseFloat(a.Str(), 64); err == nil {
				return b2v(vf != b.Float()), nil
			} else {
				return b2v(true), nil
			}
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) greater(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt, TypeTime:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime:
			return b2v(a.int1() > b.int1()), nil
		case TypeFloat:
			return b2v(a.float1() > b.float1()), nil
		case TypeStr:
			if i2, err := s2i(b.Str()); err == nil {
				return b2v(a.int1() > i2), nil
			} else if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() > f2), nil
			}
		}
	case TypeFloat:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat:
			return b2v(a.float1() > b.float1()), nil
		case TypeStr:
			if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() > f2), nil
			}
		}
	case TypeStr:
		if i1, err := s2i(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime:
				return b2v(i1 > b.int1()), nil
			case TypeFloat:
				return b2v(float64(i1) > b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return b2v(i1 > i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return b2v(float64(i1) > f2), nil
				}
			}
		} else if f1, err := s2f(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime, TypeFloat:
				return b2v(f1 > b.float1()), nil
			case TypeStr:
				if f2, err := s2f(b.Str()); err == nil {
					return b2v(f1 > f2), nil
				}
			}
		} else {
			return nil, errs.New(err.Error())
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) lesser(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt, TypeTime:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime:
			return b2v(a.int1() < b.int1()), nil
		case TypeFloat:
			return b2v(a.float1() < b.float1()), nil
		case TypeStr:
			if i2, err := s2i(b.Str()); err == nil {
				return b2v(a.int1() < i2), nil
			} else if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() < f2), nil
			}
		}
	case TypeFloat:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat:
			return b2v(a.float1() < b.float1()), nil
		case TypeStr:
			if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() < f2), nil
			}
		}
	case TypeStr:
		if i1, err := s2i(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime:
				return b2v(i1 < b.int1()), nil
			case TypeFloat:
				return b2v(float64(i1) < b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return b2v(i1 < i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return b2v(float64(i1) < f2), nil
				}
			}
		} else if f1, err := s2f(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime, TypeFloat:
				return b2v(f1 < b.float1()), nil
			case TypeStr:
				if f2, err := s2f(b.Str()); err == nil {
					return b2v(f1 < f2), nil
				}
			}
		} else {
			return nil, errs.New(err.Error())
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) greaterEqual(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt, TypeTime:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime:
			return b2v(a.int1() >= b.int1()), nil
		case TypeFloat:
			return b2v(a.float1() >= b.float1()), nil
		case TypeStr:
			if i2, err := s2i(b.Str()); err == nil {
				return b2v(a.int1() >= i2), nil
			} else if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() >= f2), nil
			}
		}
	case TypeFloat:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat:
			return b2v(a.float1() >= b.float1()), nil
		case TypeStr:
			if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() >= f2), nil
			}
		}
	case TypeStr:
		if i1, err := s2i(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime:
				return b2v(i1 >= b.int1()), nil
			case TypeFloat:
				return b2v(float64(i1) >= b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return b2v(i1 >= i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return b2v(float64(i1) >= f2), nil
				}
			}
		} else if f1, err := s2f(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime, TypeFloat:
				return b2v(f1 >= b.float1()), nil
			case TypeStr:
				if f2, err := s2f(b.Str()); err == nil {
					return b2v(f1 >= f2), nil
				}
			}
		} else {
			return nil, errs.New(err.Error())
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) LesserEqual(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt, TypeTime:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime:
			return b2v(a.int1() <= b.int1()), nil
		case TypeFloat:
			return b2v(a.float1() <= b.float1()), nil
		case TypeStr:
			if i2, err := s2i(b.Str()); err == nil {
				return b2v(a.int1() <= i2), nil
			} else if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() <= f2), nil
			}
		}
	case TypeFloat:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat:
			return b2v(a.float1() <= b.float1()), nil
		case TypeStr:
			if f2, err := s2f(b.Str()); err == nil {
				return b2v(a.float1() <= f2), nil
			}
		}
	case TypeStr:
		if i1, err := s2i(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime:
				return b2v(i1 <= b.int1()), nil
			case TypeFloat:
				return b2v(float64(i1) <= b.float1()), nil
			case TypeStr:
				if i2, err := s2i(b.Str()); err == nil {
					return b2v(i1 <= i2), nil
				} else if f2, err := s2f(b.Str()); err == nil {
					return b2v(float64(i1) <= f2), nil
				}
			}
		} else if f1, err := s2f(a.Str()); err == nil {
			switch b.Type() & TypeMask {
			case TypeInt, TypeTime, TypeFloat:
				return b2v(f1 <= b.float1()), nil
			case TypeStr:
				if f2, err := s2f(b.Str()); err == nil {
					return b2v(f1 <= f2), nil
				}
			}
		} else {
			return nil, errs.New(err.Error())
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) brackets(a Value) (Value, error) {
	return a, nil
}
func (this Expr) isNull(v Value) (Value, error) {
	return b2v(v == nil || v == Nil), nil
}
func (this Expr) isNotNull(v Value) (Value, error) {
	return b2v(v != nil && v != Nil), nil
}
func (this Expr) like(v Value) (Value, error) {
	return Nil, nil
}
func (this Expr) in(vs ...Value) (Value, error) {
	if len(vs) <= 1 {
		return b2v(false), nil
	}
	first, others := vs[0], vs[1:]
	for i := range others {
		if b, err := this.equal(first, others[i]); b == Nil || err != nil {
			return Nil, err
		} else if b.Bool() {
			return b, nil
		}
	}
	return b2v(false), nil
}
func (this Expr) notIn(vs ...Value) (Value, error) {
	if len(vs) <= 1 {
		return b2v(true), nil
	}
	first, others := vs[0], vs[1:]
	for i := range others {
		if b, err := this.equal(first, others[i]); b == nil || err != nil {
			return nil, err
		} else if !b.Bool() {
			return b, nil
		}
	}
	return b2v(true), nil
}
func (this Expr) between(a, b, c Value) (Value, error) {
	if a == nil || b == nil || c == nil || a == Nil || b == Nil || c == Nil {
		return Nil, nil
	}
	switch a.Type() & TypeMask {
	case TypeInt, TypeTime:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime:
			switch c.Type() & TypeMask {
			case TypeInt, TypeTime:
				return b2v(a.int1() >= b.int1() && a.int1() < c.int1()), nil
			case TypeFloat:
				return b2v(a.float1() >= b.float1() && a.float1() < c.float1()), nil
			case TypeStr:
				if i3, err := s2i(c.Str()); err == nil {
					return b2v(a.int1() >= b.int1() && a.int1() < i3), nil
				} else if f3, err := s2f(c.Str()); err == nil {
					return b2v(a.float1() >= b.float1() && a.float1() < f3), nil
				}
			}
		case TypeFloat:
			switch c.Type() & TypeMask {
			case TypeInt, TypeTime, TypeFloat:
				return b2v(a.float1() >= b.float1() && a.float1() < c.float1()), nil
			case TypeStr:
				if f3, err := s2f(c.Str()); err == nil {
					return b2v(a.float1() >= b.float1() && a.float1() < f3), nil
				}
			}
		case TypeStr:
			if i2, err := s2i(b.Str()); err == nil {
				return this.between(a, intValue(i2), c)
			} else if f2, err := s2f(b.Str()); err == nil {
				return this.between(a, floatValue(f2), c)
			} else {
				return nil, errs.New(err.Error())
			}
		}
	case TypeFloat:
		switch b.Type() & TypeMask {
		case TypeInt, TypeTime, TypeFloat:
			switch c.Type() & TypeMask {
			case TypeInt, TypeTime, TypeFloat:
				return b2v(a.float1() >= b.float1() && a.float1() < c.float1()), nil
			case TypeStr:
				if f3, err := s2f(c.Str()); err == nil {
					return b2v(a.float1() >= b.float1() && a.float1() < f3), nil
				}
			}
		case TypeStr:
			if f2, err := s2f(b.Str()); err == nil {
				switch c.Type() & TypeMask {
				case TypeInt, TypeTime, TypeFloat:
					return b2v(a.float1() >= f2 && f2 < c.float1()), nil
				case TypeStr:
					if f3, err := s2f(c.Str()); err == nil {
						return b2v(a.float1() >= f2 && a.float1() < f3), nil
					}
				}
			}
		}
	case TypeStr:
		if i1, err := s2i(a.Str()); err == nil {
			return this.between(intValue(i1), b, c)
		} else if f1, err := s2f(a.Str()); err == nil {
			return this.between(floatValue(f1), b, c)
		} else {
			return nil, errs.New(err.Error())
		}
	}
	return nil, errs.New("data type is error!")
}
func (this Expr) notBetween(a, b, c Value) (Value, error) {
	if ret, err := this.between(a, b, c); err != nil {
		return ret, err
	} else if ret.Type() == TypeBool {
		return b2v(!ret.Bool()), nil
	} else {
		return nil, errs.New("internal logic error!")
	}
}
func (this Expr) not(a Value) (Value, error) {
	if a == nil || a == Nil {
		return Nil, nil
	} else if a.Type() == TypeBool {
		return b2v(!a.Bool()), nil
	} else {
		return nil, errs.New("data type is error!")
	}
}
func (this Expr) and(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil {
		return Nil, nil
	} else if a.Type() == TypeBool && b.Type() == TypeBool {
		return b2v(a.Bool() && b.Bool()), nil
	} else {
		return nil, errs.New(a, b, "data type is error!")
	}
}
func (this Expr) or(a, b Value) (Value, error) {
	if a == nil || b == nil || a == Nil || b == Nil {
		return Nil, nil
	} else if a.Type() == TypeBool && b.Type() == TypeBool {
		return b2v(a.Bool() || b.Bool()), nil
	} else {
		return nil, errs.New("data type is error!")
	}
}
