package ecu

import (
	"crypto/md5"
	"encoding/binary"
	"hash"
	"math"
	"strings"
	"sync"
	"time"
)

type IterKeys func(iter func(key string) error) (err error)
type ColType int

const (
	Metric ColType = iota
	Dimension
	TimeScale
)

func (c ColType) MarshalJSON() ([]byte, error) {
	switch c {
	case Dimension:
		return []byte(`"dimension"`), nil
	case TimeScale:
		return []byte(`"timescale"`), nil
	default:
		return []byte(`"metric"`), nil
	}
}

func (c *ColType) UnmarshalJSON(v []byte) error {
	s := string(v)
	switch strings.ToLower(s) {
	case `"dimension"`:
		*c = Dimension
	case `"timescale"`:
		*c = TimeScale
	case `"metric"`:
		*c = Metric
	default:
		*c = Metric
	}
	return nil
}

type Column values

type Desc struct {
	Name       string   `json:"name,omitempty"`
	Comment    string   `json:"comment,omitempty"`
	Time       *Column  `json:"time,omitempty"`
	Dimensions []Column `json:"dimensions,omitempty"`
	Metrics    []Column `json:"metrics,omitempty"`
	ETag       string
}

var hashPool = sync.Pool{
	New: func() interface{} {
		return md5.New()
	},
}

func (d Desc) NewFrame() *DataFrame {
	return NewFrame(d.Time, d.Dimensions, d.Metrics)
}

func (d Desc) Verify() error {
	return nil
}

type Sort struct {
	Name string `json:"name,omitempty"`
	Desc bool   `json:"desc,omitempty"`
}
type FrameInfo struct {
	ID    uint64
	Start int64
	End   int64
	Addr  string
}
type DataFrame struct {
	id       uint64
	count    int
	writed   int
	any      bool
	vars     []Values
	names    map[string]int
	indices  map[string]Indexes
	ti       int
	dims     []int
	metrics  []int
	pos      map[uint64]int
	modified map[int]struct{}
}

func AnyFrame() (this *DataFrame) {
	return &DataFrame{
		id:    ID64(),
		ti:    -1,
		vars:  make([]Values, 0, 32),
		names: make(map[string]int),
		any:   true,
	}
}

func NewFrame(ti *Column, dims, metrics []Column) (this *DataFrame) {
	this = AnyFrame()
	this.any = false
	if ti != nil {
		this.ti = len(this.vars)
		this.names[ti.Name] = len(this.vars)
		ti.Kind, ti.DataType = TimeScale, TypeTime
		this.vars = append(this.vars, newValues(values(*ti)))
	} else {
		this.ti = -1
	}
	for i := range dims {
		if dims[i].Name == "" {
			continue
		} else if _, ok := this.names[dims[i].Name]; ok {
			continue
		}
		dims[i].Kind = Dimension
		this.dims = append(this.dims, len(this.vars))
		this.names[dims[i].Name] = len(this.vars)
		this.vars = append(this.vars, newValues(values(dims[i])))
	}
	for i := range metrics {
		if metrics[i].Name == "" {
			continue
		} else if _, ok := this.names[metrics[i].Name]; ok {
			continue
		}
		metrics[i].Kind = Metric
		this.metrics = append(this.metrics, len(this.vars))
		this.names[metrics[i].Name] = len(this.vars)
		this.vars = append(this.vars, newValues(values(metrics[i])))
	}
	return this
}
func (this DataFrame) ID() uint64 {
	return this.id
}
func (this DataFrame) IsAny() bool {
	return this.any
}
func (this DataFrame) IsDetail() bool {
	return len(this.dims) == 0
}
func (this DataFrame) Count() int {
	return this.count
}
func (this DataFrame) Writed() int {
	return this.writed
}
func (this DataFrame) Periods() (int64, int64) {
	if this.ti >= 0 {
		return this.vars[this.ti].Periods()
	} else {
		return 0, 0
	}
}

func (this *DataFrame) Locate(ins func(iter func(name string, value Value))) func(name string) Value {
	var bmis []UnOrderPositions
	existed := true
	ins(func(name string, value Value) {
		var (
			ok      bool
			col     int
			indices Indexes
		)
		if !existed {
			return
		}
		if indices, ok = this.indices[name]; !ok {
			if col, ok = this.names[name]; !ok {
				return
			}
			indices = NewIndexes(this.vars[col].Type())
			this.indices[name] = indices
		}
		if b := indices.Get(value, this.vars[col]); b != nil {
			bmis = append(bmis, b)
		} else {
			existed = false
		}
	})
	if !existed {
		return nil
	} else if bmi := BmiAnd(make(UnOrderPositions), bmis...); bmi.Len() != 1 {
		return nil
	} else {
		return func(name string) Value {
			if pos, ok := bmi.First(); ok {
				if col, ok := this.names[name]; ok {
					return this.vars[col].Get(pos)
				}
			}
			return Nil
		}
	}
}

func (this *DataFrame) Reindex(names ...string) error {
	if len(names) > 0 {
		indices := make(map[string]Indexes)
		for _, name := range names {
			if col, ok := this.names[name]; ok {
				indices[name] = NewIndexes(this.vars[col].Type()).Build(this.vars[col]).Build(this.vars[col])
			} else {
				return errs.New("name is out of names!")
			}
		}
		this.indices = indices
	} else {
		indices := make(map[string]Indexes)
		for col := range this.vars {
			if this.vars[col].IsIndex() {
				indices[this.vars[col].GetName()] = NewIndexes(this.vars[col].Type()).Build(this.vars[col]).Build(this.vars[col])
			}
		}
		this.indices = indices
	}
	return nil
}

func (this *DataFrame) Clone(data bool) (ret *DataFrame) {
	ret = &DataFrame{
		id: ID64(),
		ti: this.ti,
	}
	if this.names != nil {
		ret.names = make(map[string]int)
		for k, v := range this.names {
			ret.names[k] = v
		}
	}
	if this.vars != nil {
		ret.vars = make([]Values, len(this.vars))
		for i := range this.vars {
			ret.vars[i] = newValues(this.vars[i].base())
		}
	}
	if this.metrics != nil {
		ret.metrics = append(make([]int, 0, len(this.metrics)), this.metrics...)
	}
	if this.dims != nil {
		ret.dims = append(make([]int, 0, len(this.dims)), this.dims...)
	}
	if this.pos != nil {
		ret.pos = make(map[uint64]int)
	}
	return ret
}

func (this *DataFrame) Close() {
	for i := range this.vars {
		this.vars[i].Resize(0)
	}
}


func (this DataFrame) rePos() (err error) {
	var v Value
	var k uint64
	h := hashPool.Get().(hash.Hash)
	defer hashPool.Put(h)
	pos := make(map[uint64]int)
	bits := make([]byte, 8)
	bits16 := make([]byte, 0, 16)
	for row := 0; row < this.count; row++ {
		h.Reset()
		if this.ti >= 0 {
			v := this.vars[this.ti].Get(row)
			binary.BigEndian.PutUint64(bits, uint64(v.Int()))
			if _, err = h.Write(bits); err != nil {
				return
			}
		}

		for _, col := range this.dims {
			if v = this.vars[col].Get(row); v == nil || v.Type() == TypeNil {
				binary.BigEndian.PutUint64(bits, 0)
				if _, err = h.Write(bits); err != nil {
					return
				}
			} else {
				switch v.Type() & TypeMask {
				case TypeBool, TypeInt, TypeTime:
					binary.BigEndian.PutUint64(bits, uint64(v.Int()))
					if _, err = h.Write(bits); err != nil {
						return
					}
				case TypeFloat:
					binary.BigEndian.PutUint64(bits, math.Float64bits(v.Float()))
					if _, err = h.Write(bits); err != nil {
						return
					}
				case TypeStr:
					if _, err = h.Write([]byte(v.Str())); err != nil {
						return
					}
				default:
					return errs.New("type is not dimensions!")
				}
			}
		}
		k = binary.BigEndian.Uint64(h.Sum(bits16)[4:12])
		if _, ok := pos[k]; ok {
			return errs.New("hash code is repeat!")
		} else {
			pos[k] = row
		}
	}
	this.pos = pos
	return
}
func (this DataFrame) GetNames() (ret []string) {
	for i := range this.vars {
		ret = append(ret, this.vars[i].GetName())
	}
	return
}

func (this DataFrame) HashRows(ti *time.Time, dims []Column, mod int) ([]int, error) {
	return nil, nil
}
