package ecu

import (

	"sort"
)

func BmiAnd(r UnOrderPositions, bs ...UnOrderPositions) UnOrderPositions {
	sort.Slice(bs, func(i, j int) bool {
		return len(bs[i]) < len(bs[j])
	})
	for i := range bs {
		if i == 0 {
			for p := range bs[i] {
				r[p] = struct{}{}
			}
		} else {
			for p := range r {
				if _, ok := bs[i][p]; !ok {
					delete(r, p)
				}
				if len(r) == 0 {
					return r
				}
			}
		}
	}
	return r
}
func BmiOR(a, b UnOrderPositions) UnOrderPositions {
	return a
}
func BmiOper(oper OperType, ret, a, b UnOrderPositions) (UnOrderPositions, error) {
	switch oper {
	case And:
		return BmiAnd(a, b), nil
	case Or:
		return BmiOR(a, b), nil
	default:
		return nil, errs.New("internal logice error!")
	}
}
func (this *Expr) Query(ret UnOrderPositions, bmiGet func(ret UnOrderPositions, oper OperType, name string, values ...Value) (UnOrderPositions, error),
	bmiEach func(oper OperType, e *Expr, ret, b UnOrderPositions) (UnOrderPositions, error)) (e *Expr, b UnOrderPositions, err error) {
	switch this.Oper {
	case Equal, NotEqual:
		if this.Args[0].Oper == Field && this.Args[1].Oper == Const {
			if b, err = bmiGet(ret, this.Oper, this.Args[0].Field, this.Args[1].Const); err != nil || b != nil {
				return
			}
		} else if this.Args[1].Oper == Field && this.Args[0].Oper == Const {
			if b, err = bmiGet(ret, this.Oper, this.Args[1].Field, this.Args[0].Const); err != nil || b != nil {
				return
			}
		}
	case Between, NotBetween:
		if this.Args[0].Oper == Field && this.Args[1].Oper == Const && this.Args[2].Oper == Const {
			if b, err = bmiGet(ret, this.Oper, this.Args[0].Field, this.Args[1].Const, this.Args[2].Const); err != nil || b != nil {
				return
			}
		}
	case In, NotIn:
		if this.Args[0].Oper == Field {
			args := make([]Value, 0, len(this.Args))
			for _, arg := range this.Args[1:] {
				if arg.Oper == Const {
					args = append(args, arg.Const)
				}
			}
			if len(args) == (len(this.Args) - 1) {
				if b, err = bmiGet(ret, this.Oper, this.Args[0].Field, args...); err != nil || b != nil {
					return
				}
			}
		}
	case And, Or:
		var e1, e2 *Expr
		b1, b2 := make(UnOrderPositions), make(UnOrderPositions)
		if e1, b1, err = this.Args[0].Query(b1, bmiGet, bmiEach); err != nil {
			return
		} else if e2, b2, err = this.Args[1].Query(b2, bmiGet, bmiEach); err != nil {
			return
		} else if b1 != nil && b2 != nil {
			b, err = BmiOper(this.Oper, ret, b1, b2)
			return
		} else if e1 != nil && b2 != nil {
			b, err = bmiEach(this.Oper, e1, ret, b2)
			return
		} else if e2 != nil && b1 != nil {
			b, err = bmiEach(this.Oper, e2, ret, b1)
			return
		}
	}
	return this, nil, nil
}
