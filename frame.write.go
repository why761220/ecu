package ecu

import (

	"encoding/binary"
	"hash"
	"math"
)

func (this *DataFrame) Add(getter Getter, keys IterKeys) (err error) {
	if len(this.dims) == 0 {
		return this.add1(getter, keys)
	} else {
		return this.add2(getter)
	}
}
func (this *DataFrame) add1(getter Getter, keys IterKeys) (err error) {
	if this.any {
		if err = keys(func(name string) error {
			if col, ok := this.names[name]; ok {
				this.vars[col], err = this.vars[col].Add(getter)
				return err
			} else {
				var vars Values
				if vars, err = newValues(values{Name: name, DataType: TypeNil}).Resize(this.count).Add(getter); err == nil {
					this.names[name] = len(this.vars)
					this.vars = append(this.vars, vars)
				}
				return err
			}
		}); err != nil {
			for i := range this.vars {
				this.vars[i] = this.vars[i].Resize(this.count)
			}
		} else {
			count := this.count + 1
			for i := range this.vars {
				this.vars[i] = this.vars[i].Resize(count)
			}
			this.count = count
			this.writed = this.writed + 1
		}
	} else {
		for col := range this.vars {
			if this.vars[col], err = this.vars[col].Add(getter); err != nil {
				break
			}
		}
		if err != nil {
			for i := range this.vars {
				this.vars[i] = this.vars[i].Resize(this.count)
			}
		} else {
			count := this.count + 1
			for i := range this.vars {
				this.vars[i] = this.vars[i].Resize(count)
			}
			this.count = count
			this.writed = this.writed + 1
		}
	}
	return
}
func (this *DataFrame) add2(getter Getter) (err error) {
	var v Value
	bits := make([]byte, 8)
	bits16 := make([]byte, 0, 16)
	md := hashPool.Get().(hash.Hash)
	defer hashPool.Put(md)
	md.Reset()
	if this.ti >= 0 {
		if v, err = this.vars[this.ti].GetFrom(getter); err != nil {
			return
		}
		binary.BigEndian.PutUint64(bits, uint64(v.Int()))
		if _, err = md.Write(bits); err != nil {
			return
		}
	}
	for _, col := range this.dims {
		if v, err = this.vars[col].GetFrom(getter); err != nil {
			return
		} else if v == nil || v.Type() == TypeNil {
			binary.BigEndian.PutUint64(bits, 0)
			if _, err = md.Write(bits); err != nil {
				return
			}
		} else {
			switch v.Type() & TypeMask {
			case TypeBool, TypeInt, TypeTime:
				binary.BigEndian.PutUint64(bits, uint64(v.Int()))
				if _, err = md.Write(bits); err != nil {
					return
				}
			case TypeFloat:
				binary.BigEndian.PutUint64(bits, math.Float64bits(v.Float()))
				if _, err = md.Write(bits); err != nil {
					return
				}
			case TypeStr:
				if _, err = md.Write([]byte(v.Str())); err != nil {
					return
				}
			default:
				return errs.New("type is not dimensions!")
			}
		}
	}

	h := binary.BigEndian.Uint64(md.Sum(bits16)[4:12])

	if pos, ok := this.pos[h]; ok {
		var cancel Cancel
		cancels := make([]Cancel, 0, len(this.metrics))
		for _, col := range this.metrics {
			if v, err = this.vars[col].GetFrom(getter); err != nil {
				break
			} else if this.vars[col], cancel, err = this.vars[col].Set(pos, v, true); err != nil {
				break
			}
			cancels = append(cancels, cancel)
		}
		if err != nil {
			for _, cancel := range cancels {
				cancel()
			}
		} else {
			this.writed = this.writed + 1
		}
	} else {
		for col, vars := range this.vars {
			if this.vars[col], err = vars.Add(getter); err != nil {
				break
			}
		}
		if err != nil {
			for i := 0; i < len(this.vars); i++ {
				this.vars[i] = this.vars[i].Resize(this.count)
			}
		} else {
			pos = this.count
			if this.pos == nil {
				this.pos = make(map[uint64]int)
			}
			this.pos[h] = pos
			this.count = this.count + 1
			this.writed = this.writed + 1
		}
	}

	return
}
func (this *DataFrame) Merge(other *DataFrame, rows Positions) (err error) {
	if rows == nil {
		for i, l := 0, other.count; err == nil && i < l; i++ {
			err = this.Add(func(name string) (value Value, err error) {
				return other.Get(i, name), nil
			}, func(iter func(key string) error) (err error) {
				for i := range other.vars {
					if err = iter(other.vars[i].GetName()); err != nil {
						return
					}
				}
				return
			})
		}
	} else {
		rows.Range(func(row int) bool {
			err = this.Add(func(name string) (value Value, err error) {
				return other.Get(row, name), nil
			}, func(iter func(key string) error) (err error) {
				for i := range other.vars {
					if err = iter(other.vars[i].GetName()); err != nil {
						return
					}
				}
				return
			})
			return err == nil
		})
	}

	return
}
func (this *DataFrame) Set(i int, name string, value Value) (cancel Cancel, err error) {
	if col, ok := this.names[name]; ok {
		this.vars[col], cancel, err = this.vars[col].Set(i, value, false)
	} else if this.any {
		var vars Values
		if vars, cancel, err = newValues(values{Name: name, DataType: value.Type()}).Resize(this.count).Set(i, value, false); err == nil {
			this.names[name] = len(this.vars)
			this.vars = append(this.vars, vars)
		}
	} else {
		return nil, errs.New("field not found!")
	}
	return
}

func (this *DataFrame) Get(i int, name string) Value {
	if col, ok := this.names[name]; ok {
		return this.vars[col].Get(i)
	} else {
		return nil
	}
}
