package ecu

import (

	"sort"
)

type Outer func(iter func(out func(name string, value Value) error) error) error

func (this DataFrame) bmiGet(ret UnOrderPositions, oper OperType, name string, vars ...Value) (UnOrderPositions, error) {
	var indexes Indexes
	var ok bool
	var col int
	if len(this.names) == 0 || len(this.indices) == 0 {
		return nil, nil
	}
	if col, ok = this.names[name]; !ok {
		return nil, errs.New("field is not exit!")
	} else if indexes, ok = this.indices[name]; !ok {
		indexes = NewIndexes(this.vars[col].Type())
	}
	indexes = indexes.Build(this.vars[col], vars...)
	this.indices[name] = indexes
	switch oper {
	case Equal:
		return ret.OR(indexes.Get(vars[0], nil)), nil
	case NotEqual:
		return indexes.Get(vars[0], nil).Reverse(ret, this.count), nil
	case IsNull:
		return indexes.Get(nil, nil), nil
	case IsNotNull:
		return indexes.Get(nil, nil).Reverse(ret, this.count), nil
	case In:
		for _, v := range vars {
			ret = ret.OR(indexes.Get(v, nil))
		}
		return ret, nil
	case NotIn:
		for _, v := range vars {
			ret = ret.OR(indexes.Get(v, nil))
		}
		return ret.Reverse(nil, this.count), nil
	}
	return nil, nil
}

func (this DataFrame) bmiEach(oper OperType, e *Expr, a, b UnOrderPositions) (UnOrderPositions, error) {
	switch oper {
	case And:
		t := make(UnOrderPositions)
		for i := range b {
			if ok, err := e.Bool(func(name string) (value Value, err error) {
				if col, ok := this.names[name]; ok {
					return this.vars[col].Get(i), nil
				} else {
					return nil, nil
				}
			}); err != nil {
				return nil, err
			} else if ok {
				t[i] = struct{}{}
			}
		}
		return a.And(t), nil
	case Or:
		for i := range b {
			if ok, err := e.Bool(func(name string) (value Value, err error) {
				if col, ok := this.names[name]; ok {
					return this.vars[col].Get(i), nil
				} else {
					return nil, nil
				}
			}); err != nil {
				return nil, err
			} else if ok {
				a = a.Set(i)
			}
		}
		return a, nil
	default:
		return nil, errs.Logic()
	}
}
func (this DataFrame) Sort(pos OrderPositions, sorts []Sort, limit1, limit2 int) (OrderPositions, error) {
	var vs Values
	var v1, v2 Value
	if len(pos) == 0 || len(sorts) == 0 {
		return pos, nil
	}
	cols := make([]int, len(sorts))
	for i := range sorts {
		if col, ok := this.names[sorts[i].Name]; ok {
			cols[i] = col
		} else {
			return pos, errs.New("name out of fields!")
		}
	}

	sort.Slice(pos, func(i, j int) bool {
		for m := range sorts {
			vs = this.vars[cols[m]]
			v1, v2 = vs.Get(pos[i]), vs.Get(pos[j])
			switch vs.Type() & TypeMask {
			case TypeBool:
				if v1.Bool() == v2.Bool() {
					continue
				} else {
					return v1.Bool()
				}
			case TypeInt, TypeTime:
				if v1.Int() < v2.Int() {
					return !sorts[m].Desc
				} else if v1.Int() > v2.Int() {
					return sorts[m].Desc
				}
			case TypeFloat:
				if v1.Float() < v2.Float() {
					return !sorts[m].Desc
				} else if v1.Float() > v2.Float() {
					return sorts[m].Desc
				}
			case TypeStr:
				if v1.Str() < v2.Str() {
					return !sorts[m].Desc
				} else if v1.Str() > v2.Str() {
					return sorts[m].Desc
				}
			}
		}
		return false
	})
	if limit1 >= 0 && limit1 < len(pos) {
		pos = pos[limit1:]
	}
	if limit2 >= 0 && limit2 < len(pos) {
		pos = pos[:limit2]
	}
	return pos, nil
}

func (this DataFrame) query(in UnOrderPositions, e *Expr) (b UnOrderPositions, err error) {
	var ok bool
	if e, b, err = e.Query(in, this.bmiGet, this.bmiEach); err != nil {
		return
	} else if e != nil {
		for i, l := 0, this.count; i < l; i++ {
			if ok, err = e.Bool(func(name string) (value Value, err error) {
				if col, ok := this.names[name]; ok {
					return this.vars[col].Get(i), nil
				} else {
					return nil, nil
				}
			}); err != nil {
				return
			} else if ok {
				if b == nil {
					b = make(UnOrderPositions)
				}
				b = b.Set(i)
			}
		}
	}
	return
}
func (this DataFrame) Out(pos OrderPositions, names map[string]string, outer Outer) (err error) {
	if len(names) > 0 {
		for _, p := range pos {
			if err = outer(func(out func(name string, value Value) error) (err error) {
				for k1, k2 := range names {
					if col, ok := this.names[k1]; ok {
						if err = out(k2, this.vars[col].Get(p)); err != nil {
							return
						}
					}
				}
				return
			}); err != nil {
				return
			}
		}
	} else {
		for _, p := range pos {
			if err = outer(func(out func(name string, value Value) error) (err error) {
				for col := range this.vars {
					if err = out(this.vars[col].GetName(), this.vars[col].Get(p)); err != nil {
						return
					}
				}
				return
			}); err != nil {
				return
			}
		}
	}
	return
}

func (this DataFrame) SelectBak(e *Expr, start, end int64) (Positions, error) {
	var ok bool
	var err error
	if e == nil {
		if start != 0 && end != 0 && this.ti >= 0 {
			ti := this.vars[this.ti]
			var b OrderPositions
			for i, l := 0, this.count; i < l; i++ {
				if t := ti.Get(i).Int(); t >= start && t < end {
					if b == nil {
						b = make(OrderPositions, 0, this.count)
					}
					b = b.Add(i)
				}
			}
			return b, nil
		} else {
			return AllPositions(this.count), nil
		}
	}
	r := make(UnOrderPositions)
	if e, r, err = e.Query(r, this.bmiGet, this.bmiEach); err != nil {
		return nil, err
	} else if e == nil {
		if start != 0 && end != 0 && this.ti >= 0 {
			var ret OrderPositions
			ti := this.vars[this.ti]
			r.Range(func(pos int) bool {
				if t := ti.Get(pos).Int(); t >= start && t < end {
					if ret == nil {
						ret = make(OrderPositions, 0, len(r))
					}
					ret = ret.Add(pos)
				}
				return true
			})
			return ret, nil
		} else {
			return r, nil
		}
	}
	if r == nil {
		r = make(UnOrderPositions)
	}
	if start != 0 && end != 0 && this.ti >= 0 {
		ti := this.vars[this.ti]
		for i, l := 0, this.count; i < l; i++ {
			if t := ti.Get(i).Int(); t >= start && t < end {
				if ok, err = e.Bool(func(name string) (value Value, err error) {
					if col, ok := this.names[name]; ok {
						return this.vars[col].Get(i), nil
					} else {
						return nil, nil
					}
				}); err != nil {
					return nil, err
				} else if ok {
					r = r.Set(i)
				}
			}
		}
	} else {
		for i, l := 0, this.count; i < l; i++ {
			if ok, err = e.Bool(func(name string) (value Value, err error) {
				if col, ok := this.names[name]; ok {
					return this.vars[col].Get(i), nil
				} else {
					return nil, nil
				}
			}); err != nil {
				return nil, err
			} else if ok {
				r = r.Set(i)
			}
		}
	}

	return r, nil
}
func (this DataFrame) Select(e *Expr, start, end int64, sorts []Sort, limit1, limit2 int) (Positions, error) {
	var ok bool
	var err error
	if e == nil {
		if start != 0 && end != 0 && this.ti >= 0 {
			ti := this.vars[this.ti]
			var b OrderPositions
			for i, l := 0, this.count; i < l; i++ {
				if t := ti.Get(i).Int(); t >= start && t < end {
					if b == nil {
						b = make(OrderPositions, 0, this.count)
					}
					b = b.Add(i)
				}
			}
			if sorts != nil {
				return this.Sort(b, sorts, limit1, limit2)
			} else {
				return b, nil
			}
		} else if sorts != nil {
			return this.Sort(NewOrderPositions(this.count), sorts, limit1, limit2)
		} else {
			return AllPositions(this.count), nil
		}
	}
	r := make(UnOrderPositions)
	if e, r, err = e.Query(r, this.bmiGet, this.bmiEach); err != nil {
		return nil, err
	} else if e == nil {
		if start != 0 && end != 0 && this.ti >= 0 {
			var ret OrderPositions
			ti := this.vars[this.ti]
			r.Range(func(pos int) bool {
				if t := ti.Get(pos).Int(); t >= start && t < end {
					if ret == nil {
						ret = make(OrderPositions, 0, len(r))
					}
					ret = ret.Add(pos)
				}
				return true
			})
			if sorts != nil {
				return this.Sort(ret, sorts, limit1, limit2)
			} else {
				return ret, nil
			}
		} else if sorts != nil {
			return this.Sort(r.To(make(OrderPositions, 0, r.Len())), sorts, limit1, limit2)
		} else {
			return r, nil
		}
	}
	var b OrderPositions
	if start != 0 && end != 0 && this.ti >= 0 {
		ti := this.vars[this.ti]
		for i, l := 0, this.count; i < l; i++ {
			if t := ti.Get(i).Int(); t >= start && t < end {
				if ok, err = e.Bool(func(name string) (value Value, err error) {
					if col, ok := this.names[name]; ok {
						return this.vars[col].Get(i), nil
					} else {
						return nil, errs.New(name + " field not exist!")
					}
				}); err != nil {
					return nil, err
				} else if ok {
					if b == nil {
						b = make(OrderPositions, 0, l)
					}
					b = b.Add(i)
				}
			}
		}
	} else {
		for i, l := 0, this.count; i < l; i++ {
			if ok, err = e.Bool(func(name string) (value Value, err error) {
				if col, ok := this.names[name]; ok {
					return this.vars[col].Get(i), nil
				} else {
					return nil, errs.New(name + " field not exist!")
				}
			}); err != nil {
				return nil, err
			} else if ok {
				if b == nil {
					b = make(OrderPositions, 0, l)
				}
				b = b.Add(i)
			}
		}
	}
	if sorts != nil {
		return this.Sort(b, sorts, limit1, limit2)
	} else {
		return b, nil
	}
}
