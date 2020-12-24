package ecu

type Indexes interface {
	Get(value Value, values Values) UnOrderPositions
	Build(values Values, vars ...Value) Indexes
	Type() DataType
}

func NewIndexes(dt DataType) Indexes {
	switch dt & TypeMask {
	case TypeInt, TypeTime:
		return make(intIndex)
	case TypeFloat:
		return make(floatIndex)
	case TypeStr:
		return make(strIndex)
	}
	return nil
}

type intIndex map[int64]UnOrderPositions

func (intIndex) Type() DataType {
	return TypeInt
}

type floatIndex map[float64]UnOrderPositions

func (floatIndex) Type() DataType {
	return TypeFloat
}

type strIndex map[string]UnOrderPositions

func (strIndex) Type() DataType {
	return TypeStr
}

func (this floatIndex) Get(value Value, values Values) UnOrderPositions {
	a := value.Float()
	if p, ok := this[a]; ok {
		return p
	} else if values == nil {
		return nil
	}
	pos := make(UnOrderPositions)
	for i, l := 0, values.Count(); i < l; i++ {
		if b := values.Get(i).Float(); a == b {
			pos = pos.Set(i)
		}
	}
	this[a] = pos
	return pos
}

func (this floatIndex) Build(values Values, vars ...Value) Indexes {
	if len(vars) > 0 {
		for i, l := 0, values.Count(); i < l; i++ {
			a := values.Get(i).Float()
			for _, b := range vars {
				if a == b.Float() {
					if pos, ok := this[a]; ok {
						this[a] = pos.Set(i)
					} else {
						this[a] = make(UnOrderPositions).Set(i)
					}
				}
			}
		}
	} else {
		for i, l := 0, values.Count(); i < l; i++ {
			a := values.Get(i).Float()
			if pos, ok := this[a]; ok {
				this[a] = pos.Set(i)
			} else {
				this[a] = make(UnOrderPositions).Set(i)
			}
		}
	}
	return this
}

func (this strIndex) Get(value Value, values Values) UnOrderPositions {
	if p, ok := this[value.Str()]; ok {
		return p
	} else {
		return nil
	}
}

func (this strIndex) Build(values Values, vars ...Value) Indexes {
	if len(vars) > 0 {
		for i, l := 0, values.Count(); i < l; i++ {
			a := values.Get(i).Str()
			for _, b := range vars {
				if a == b.Str() {
					if pos, ok := this[a]; ok {
						this[a] = pos.Set(i)
					} else {
						this[a] = make(UnOrderPositions).Set(i)
					}
				}
			}
		}
	} else {
		for i, l := 0, values.Count(); i < l; i++ {
			a := values.Get(i).Str()
			if pos, ok := this[a]; ok {
				this[a] = pos.Set(i)
			} else {
				this[a] = make(UnOrderPositions).Set(i)
			}
		}
	}
	return this
}

type Positions interface {
	Range(ret func(pos int) bool)
	Len() int
	To(OrderPositions) OrderPositions
}
type UnOrderPositions map[int]struct{}
type OrderPositions []int
type AllPositions int

func (a AllPositions) Range(ret func(pos int) bool) {
	for i := 0; i < int(a); i++ {
		if !ret(i) {
			return
		}
	}
}

func (a AllPositions) Len() int {
	return int(a)
}
func (a AllPositions) To(pos OrderPositions) OrderPositions {
	l := a.Len()
	if pos == nil {
		pos = make(OrderPositions, 0, l)
	} else {
		pos = pos[0:0]
	}
	for i, l := 0, a.Len(); i < l; i++ {
		pos = append(pos, i)
	}
	return pos
}
func NewOrderPositions(size int) OrderPositions {
	ret := make([]int, size)
	for i := 0; i < size; i++ {
		ret[i] = i
	}
	return ret
}
func (a OrderPositions) Range(ret func(pos int) bool) {
	for i := range a {
		if !ret(a[i]) {
			break
		}
	}
}

func (a OrderPositions) Len() int {
	return len(a)
}
func (a OrderPositions) To(OrderPositions) OrderPositions {
	return a
}

func (a OrderPositions) Add(pos int) OrderPositions {
	return append(a, pos)
}
func (a UnOrderPositions) Len() int {
	return len(a)
}
func (a UnOrderPositions) Range(ret func(pos int) bool) {
	for pos := range a {
		if !ret(pos) {
			break
		}
	}
}
func (a UnOrderPositions) To(pos OrderPositions) OrderPositions {
	if pos == nil {
		pos = make(OrderPositions, 0, len(a))
	} else {
		pos = pos[0:0]
	}
	for i := range a {
		pos = append(pos, i)
	}
	return pos
}
func (a UnOrderPositions) And(b UnOrderPositions) UnOrderPositions {
	for i := range a {
		if _, ok := b[i]; !ok {
			delete(a, i)
		}
	}
	return a
}
func (a UnOrderPositions) OR(b UnOrderPositions) UnOrderPositions {
	for i := range b {
		a[i] = struct{}{}
	}
	return a
}
func (a UnOrderPositions) Set(pos int) UnOrderPositions {
	a[pos] = struct{}{}
	return a
}

func (a UnOrderPositions) Reverse(ret UnOrderPositions, size int) UnOrderPositions {
	var ok bool
	if ret == nil {
		for i := 0; i < size; i++ {
			if _, ok = a[i]; ok {
				delete(a, i)
			} else {
				a[i] = struct{}{}
			}
		}
		return a
	} else {
		for i := 0; i < size; i++ {
			if _, ok = a[i]; !ok {
				ret[i] = struct{}{}
			}
		}
		return ret
	}
}
func (a UnOrderPositions) First() (int, bool) {
	for i := range a {
		return i, true
	}
	return 0, false
}
func (this intIndex) Get(value Value, values Values) UnOrderPositions {
	if p, ok := this[value.Int()]; ok {
		return p
	} else {
		return nil
	}
}
func (this intIndex) Build(values Values, vars ...Value) Indexes {
	if len(vars) > 0 {
		for i, l := 0, values.Count(); i < l; i++ {
			a := values.Get(i).Int()
			for _, b := range vars {
				if a == b.Int() {
					if pos, ok := this[a]; ok {
						this[a] = pos.Set(i)
					} else {
						this[a] = make(UnOrderPositions).Set(i)
					}
				}
			}
		}
	} else {
		for i, l := 0, values.Count(); i < l; i++ {
			a := values.Get(i).Int()
			if pos, ok := this[a]; ok {
				this[a] = pos.Set(i)
			} else {
				this[a] = make(UnOrderPositions).Set(i)
			}
		}
	}
	return this
}
