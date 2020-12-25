package ecu

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"
)

type tarHeader struct {
	name string
	bs   *bytes.Buffer
}

func (t tarHeader) Name() string {
	return t.name
}

func (t tarHeader) Size() int64 {
	if t.bs == nil {
		return 0
	} else {
		return int64(t.bs.Len())
	}

}

func (t tarHeader) Mode() os.FileMode {
	return os.ModePerm
}

func (t tarHeader) ModTime() time.Time {
	return time.Now()
}

func (t tarHeader) IsDir() bool {
	return t.bs == nil
}

func (t tarHeader) Sys() interface{} {
	return nil
}

var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReader(bytes.NewReader(nil))
	},
}

type PacketFormat int

func (p PacketFormat) MarshalJSON() ([]byte, error) {
	switch p {
	case PfTar:
		return []byte(`"tar"`), nil
	case PfTxtRow:
		return []byte(`"TxtRow"`), nil
	case PfJsonRow:
		return []byte(`"JsonRow"`), nil
	case PfJsonArray:
		return []byte(`"JsonArray"`), nil
	case PfJsonColumn:
		return []byte(`"JsonColumn"`), nil
	default:
		return nil, errs.New("unknown packet format!")
	}
}

func (p *PacketFormat) UnmarshalJSON(v []byte) error {
	switch strings.ToLower(string(v)) {
	case `"tar"`:
		*p = PfTar
	case `"txt"`, `"txtrow"`:
		*p = PfTxtRow
	case `"jsonrow"`:
		*p = PfJsonRow
	case `"json"`, `"jsonarray"`:
		*p = PfJsonArray
	case `"jsoncolumn"`, `"column"`:
		*p = PfJsonColumn
	default:
		return errs.New("unkown packet format!")
	}
	return nil
}

const (
	PfTar PacketFormat = iota
	PfTxtRow
	PfJsonRow
	PfJsonArray
	PfJsonColumn
)

func ParsePacketFormat(s string, _default PacketFormat) PacketFormat {
	switch s {
	case "TxtRow":
		return PfTxtRow
	case "JsonRow":
		return PfJsonRow
	case "PfJsonArray":
		return PfJsonArray
	case "PfJsonColumn":
		return PfJsonColumn
	}
	return _default
}

type FrameReader struct {
	*DataFrame
	Format PacketFormat
	cols   map[string]string
	Rows   Positions
	Start  int
	End    int
	bs     *bytes.Buffer
	Layout string
	Prec   string
	Delim  string
}
type FrameWriter struct {
	*DataFrame
	Format PacketFormat
	Paths  []string
	bs     *bytes.Buffer
}

func (this FrameReader) outVars() (vars []Values, names []string, rows Positions, err error) {
	if len(this.cols) > 0 {
		vars, names = make([]Values, 0, len(this.vars)), make([]string, 0, len(this.vars))
		for src, dst := range this.cols {
			if col, ok := this.names[src]; ok {
				vars, names = append(vars, this.vars[col]), append(names, dst)
			} else {
				err = errs.New(src + " column not found!")
				return
			}
		}
	} else {
		vars = this.vars
		names = make([]string, len(vars))
		for i := range vars {
			names[i] = vars[i].GetName()
		}
	}
	if this.Rows == nil || this.Rows.Len() == 0 {
		rows = AllPositions(this.count)
	} else {
		rows = this.Rows
	}
	return
}
func (this FrameReader) TxtRowWriteTo(w io.Writer) (n int64, err error) {
	var nn int
	var v Value
	var fmt string
	var vars []Values
	var rows Positions
	var delim []byte
	if len(this.Delim) == 0 {
		delim = []byte{','}
	} else {
		delim = []byte(this.Delim)
	}
	if this.DataFrame == nil {
		return 0, nil
	} else if vars, _, rows, err = this.outVars(); err != nil || len(vars) == 0 {
		return
	}
	count, start, end := 0, this.Start, this.End
	if end <= 0 {
		end = rows.Len()
	}
	rows.Range(func(row int) bool {
		if count < start {
			return true
		} else if count >= end {
			return false
		}
		count++
		first := true
		for col := range vars {
			if first {
				first = false
			} else if nn, err = w.Write(delim); err != nil {
				return false
			} else {
				n += int64(nn)
			}
			if v = vars[col].Get(row); v != nil && v != Nil {
				switch v.Type() & TypeMask {
				case TypeFloat:
					fmt = this.Prec
				case TypeTime:
					fmt = this.Layout
				}
				if fmt == "" {
					fmt = vars[col].Format()
				}
				if nn, err = w.Write([]byte(v.Format(fmt))); err != nil {
					return false
				} else {
					n += int64(nn)
				}
			}
		}
		if nn, err = w.Write([]byte{'\n'}); err != nil {
			return false
		} else {
			n += int64(nn)
		}
		return true
	})
	return
}

func (this FrameReader) JsonRowWriteTo(w io.Writer) (n int64, err error) {
	if this.DataFrame == nil {
		return 0, errs.New("frame is nil!")
	}
	if vars, names, rows, err := this.outVars(); err == nil && len(vars) > 0 {
		var outer map[string]interface{}
		encoder := json.NewEncoder(w)
		var fmt string
		count, start, end := 0, this.Start, this.End
		if end <= 0 {
			end = rows.Len()
		}
		rows.Range(func(row int) bool {
			if count < start {
				return true
			} else if count >= end {
				return false
			}
			count++
			outer = make(map[string]interface{})
			for col := range vars {
				if vars[col].Type() == TypeTime {
					if v := vars[col].Get(row); v != nil && v != Nil {
						if fmt = vars[col].Format(); fmt == "" {
							fmt = this.Layout
						}
						outer[names[col]] = FormatTime(v.Int(), fmt)
					}
				} else if v := vars[col].Get(row).Base(); v != nil && v != Nil {
					outer[names[col]] = vars[col].Get(row).Base()
				}
			}
			err = encoder.Encode(outer)
			return err == nil
		})
	}
	return
}
func (this FrameReader) JsonArrayWriteTo(w io.Writer) (n int64, err error) {
	if this.DataFrame == nil {
		return 0, errs.New("frame is nil!")
	}
	if vars, names, rows, err := this.outVars(); err == nil && len(vars) > 0 {
		var row map[string]interface{}
		outs := make([]map[string]interface{}, 0, rows.Len())
		count, start, end := 0, this.Start, this.End
		if end <= 0 {
			end = rows.Len()
		}
		rows.Range(func(pos int) bool {
			if count < start {
				return true
			} else if count >= end {
				return false
			}
			count++
			row = make(map[string]interface{})
			for col := range vars {
				row[names[col]] = vars[col].Get(pos).Base()
			}
			outs = append(outs, row)
			return true
		})
		err = json.NewEncoder(w).Encode(outs)
	}
	return
}
func (this FrameReader) JsonColumnWriteTo(w io.Writer) (n int64, err error) {
	if this.DataFrame == nil {
		return 0, errs.New("frame is nil!")
	}
	if vars, _, rows, err := this.outVars(); err == nil && len(vars) > 0 {
		outs := make([][]interface{}, len(vars))
		count, start, end := 0, this.Start, this.End
		if end <= 0 {
			end = rows.Len()
		}
		rows.Range(func(pos int) bool {
			if count < start {
				return true
			} else if count >= end {
				return false
			}
			count++
			for col := 0; col < len(vars); col++ {
				if vars[col].GetName() == "isError" {
					println("111")
				}
				outs[col] = append(outs[col], vars[col].Get(pos).Base())
			}
			return true
		})
		ret := make(map[string][]interface{})
		for col := 0; col < len(vars); col++ {
			ret[vars[col].GetName()] = outs[col]
		}
		err = json.NewEncoder(w).Encode(ret)
	}
	return
}
func (this FrameWriter) JsonColumnReadFrom(r io.Reader) (n int64, err error) {
	var ok bool
	var col int
	var arr []interface{}
	var outs map[string]interface{}
	count := this.count
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	if err = decoder.Decode(&outs); err != nil {
		return
	}
	for name, vars := range outs {
		if col, ok = this.names[name]; !ok {
			col = len(this.vars)
			vars := newValues(values{}).Resize(count)
			this.vars = append(this.vars, vars)
			this.names[name] = col
		}
		if arr, ok = vars.([]interface{}); ok {
			for i := range arr {
				if this.vars[col], err = this.vars[col].Add(func(name string) (Value, error) {
					return NewValue(arr[i])
				}); err != nil {
					return
				}
			}
		}
	}
	for col = range this.vars {
		if t := this.vars[col].Count(); t > count {
			count = t
		}
	}
	for col = range this.vars {
		this.vars[col].Resize(count)
	}
	this.count = count
	return
}

func (this FrameReader) TarWriteTo(w io.Writer) (n int64, err error) {
	if this.DataFrame == nil {
		return 0, nil
	}
	var h *tar.Header
	tw := tar.NewWriter(w)
	defer func() {
		if err := tw.Close(); err != nil {
			errs.Println(err)
		}
	}()
	bs := bytesBufferPool.Get().(*bytes.Buffer)
	defer bytesBufferPool.Put(bs)
	for i := range this.vars {
		bs.Reset()
		if err = this.vars[i].Save(bs); err != nil {
			return
		} else if h, err = tar.FileInfoHeader(tarHeader{name: "data/" + this.vars[i].GetName(), bs: bs}, this.vars[i].GetName()); err != nil {
			return
		} else if err = tw.WriteHeader(h); err != nil {
			return
		}
		if _, err = io.Copy(tw, bs); err != nil {
			return
		}
	}
	return
}

func (this FrameReader) TxtRowReadFrom(r io.Reader) (n int64, err error) {
	var v string
	var line []byte
	if this.DataFrame == nil {
		return 0, nil
	}
	br := bufioReaderPool.Get().(*bufio.Reader)
	bl := bufioReaderPool.Get().(*bufio.Reader)
	defer func() {
		bufioReaderPool.Put(br)
		bufioReaderPool.Put(bl)
	}()
	br.Reset(r)
	for err == nil {
		if line, _, err = br.ReadLine(); err == io.EOF || err == io.ErrUnexpectedEOF {
			err = nil
			break
		} else if err == nil {
			bl.Reset(bytes.NewReader(line))
			for i := 0; err == nil && i < len(this.vars); i++ {
				if v, err = bl.ReadString(','); err == io.EOF {
					err = nil
					break
				} else if err == nil {
					this.vars[i], err = this.vars[i].Add(func(name string) (Value, error) {
						return strValue(v), nil
					})
				}
			}
		}
	}
	return
}

func (this *FrameWriter) TarReadFrom(r io.Reader) (n int64, err error) {
	var (
		ok    bool
		i     int
		count int
		tr    *tar.Reader
		vs    Values
		bs    []byte
		col   int
		base  values
	)
	if this.DataFrame == nil {
		return 0, nil
	}
	tr = tar.NewReader(r)
	buf := bufioReaderPool.Get().(*bufio.Reader)
	defer bufioReaderPool.Put(buf)

	for {
		this.ti = -1
		if _, err = tr.Next(); err == io.EOF {
			err = nil
			break
		} else if err != nil {
			break
		}
		buf.Reset(tr)
		base = values{}
		if bs, _, err = buf.ReadLine(); err != nil {
			break
		} else if err = json.Unmarshal(bs, &base); err != nil {
			break
		}
		if col, ok = this.names[base.GetName()]; ok && this.vars[col].Type() != TypeNil {
			if this.vars[col], err = this.vars[col].load(buf, base.Size, true); err != nil {
				break
			} else {
				vs = this.vars[col]
			}
		} else if vs, err = newValues(base).Resize(this.count).load(buf, base.Size, true); err != nil {
			break
		} else {
			col = len(this.vars)
			this.names[vs.GetName()] = col
			this.vars = append(this.vars, vs)
			switch vs.GetKind() {
			case TimeScale:
				this.ti = col
			case Dimension:
				this.dims = append(this.dims, col)
			case Metric:
				this.metrics = append(this.metrics, col)
			}
		}
		if vs.Count() > count {
			count = vs.Count()
		}
		if vs.IsIndex() {
			if this.indices == nil {
				this.indices = make(map[string]Indexes)
			}
			this.indices[vs.GetName()] = NewIndexes(vs.Type())
		}
	}
	if err == nil {
		for i = range this.vars {
			this.vars[i] = this.vars[i].Resize(count)
		}
		this.count = count
		this.pos = make(map[uint64]int)
		md := hashPool.Get().(hash.Hash)
		defer hashPool.Put(md)
		var v Value
		bits := make([]byte, 8)
		bits16 := make([]byte, 0, 16)
		for pos := 0; pos < count; pos++ {
			md.Reset()
			if this.ti >= 0 {
				if v = this.vars[this.ti].Get(pos); v == nil || v.Type() == TypeNil {
					binary.BigEndian.PutUint64(bits, 0)
				} else {
					binary.BigEndian.PutUint64(bits, uint64(v.Int()))
				}
				if _, err = md.Write(bits); err != nil {
					return
				}
			}
			for _, col := range this.dims {
				if v = this.vars[col].Get(pos); v == nil || v.Type() == TypeNil {
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
						return 0, errs.New("type is not dimensions!")
					}
				}
			}
			h := binary.BigEndian.Uint64(md.Sum(bits16)[4:12])
			this.pos[h] = pos
		}
	} else {
		this.DataFrame.Close()
	}
	return
}
func (this *FrameWriter) extract(keys []string, parent interface{}, out func(row map[string]interface{}) error) (err error) {
	switch parent := parent.(type) {
	case map[string]interface{}:
		if len(keys) > 0 {
			if child, ok := parent[keys[0]]; ok {
				return this.extract(keys[1:], child, out)
			}
		} else {
			return out(parent)
		}
	case []interface{}:
		for i := range parent {
			if err = this.extract(keys, parent[i], out); err != nil {
				return
			}
		}
	default:
		if len(keys) > 0 {
			if v := reflect.ValueOf(parent).FieldByName(keys[0]); !v.IsNil() {
				return this.extract(keys[1:], v.Interface(), out)
			}
		}
	}
	return
}

func (this *FrameWriter) JsonReadFrom(r io.Reader) (n int64, err error) {
	var data interface{}
	decoder := json.NewDecoder(r)
	decoder.UseNumber()

	for err = decoder.Decode(&data); err == nil; err = decoder.Decode(&data) {
		if err = this.extract(this.Paths, data, func(row map[string]interface{}) error {
			return this.Add(func(name string) (Value, error) {
				if v, ok := row[name]; ok {
					return NewValue(v)
				} else {
					return nil, nil
				}
			}, func(iter func(key string) error) (err error) {
				for key := range row {
					if err = iter(key); err != nil {
						return
					}
				}
				return
			})
		}); err != nil {
			break
		}
	}
	if err == io.EOF {
		return 0, nil
	} else {
		return
	}
}
func Merge(dst, src *DataFrame, rows Positions, names map[string]string) error {
	return dst.Merge(src, rows)
}

func FromFile(fileName string, etag string) (*DataFrame, string, error) {
	var err error
	var file *os.File
	var r io.Reader
	if file, err = os.Open(fileName); err != nil {
		return nil, etag, errs.New(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			errs.Println(err)
		}
	}()
	if path.Ext(fileName) == ".gz" {
		if gr, err := gzip.NewReader(file); err != nil {
			return nil, etag, errs.New(err)
		} else {
			defer func() {
				if err := gr.Close(); err != nil {
					errs.Println(err)
				}
			}()
			r = gr
		}
	} else {
		r = file
	}
	tr := FrameWriter{DataFrame: AnyFrame(), Format: PfTar}
	if _, err = tr.ReadFrom(r); err != nil {
		tr.Close()
		return nil, etag, err
	} else {
		return tr.DataFrame, etag, nil
	}
}
func FromRemote(addr *url.URL, etag string) (*DataFrame, string, error) {
	var (
		code int
		err  error
		req  *http.Request
		resp *http.Response
		gr   *gzip.Reader
		bs   []byte
	)

	if req, err = http.NewRequest(http.MethodGet, addr.String(), nil); err != nil {
		return nil, etag, err
	}
	req.Header.Set("If-Modified-Since", etag)
	if resp, err = http.DefaultClient.Do(req); err != nil {
		return nil, etag, err
	} else if code = resp.StatusCode; code == http.StatusNotModified || code == http.StatusNotFound {
		return nil, etag, resp.Body.Close()
	} else if code != http.StatusOK {
		if bs, err = ioutil.ReadAll(resp.Body); err != nil {
			_ = resp.Body.Close()
			return nil, etag, err
		} else if err = resp.Body.Close(); err != nil {
			return nil, etag, err
		} else {
			return nil, etag, errs.New(string(bs))
		}
	}
	if gr, err = gzip.NewReader(resp.Body); err != nil {
		return nil, etag, errs.New(err)
	}
	defer gr.Close()
	tr := FrameWriter{DataFrame: AnyFrame()}
	if _, err = tr.ReadFrom(gr); err != nil {
		tr.DataFrame.Close()
		_ = resp.Body.Close()
		return nil, etag, err
	} else {
		if err = resp.Body.Close(); err != nil {
			errs.Println(err)
		}
		return tr.DataFrame, resp.Header.Get("Last-Modified"), nil
	}
}
func LoadFrame(src string, etag string) (*DataFrame, string, error) {
	if addr, err := url.Parse(src); err != nil {
		return nil, etag, errs.New(err)
	} else if addr.Scheme == "file" {
		return FromFile(addr.Host+addr.Path, etag)
	} else if addr.Scheme == "http" {
		return FromRemote(addr, etag)
	} else {
		return nil, etag, errs.New("scheme is error!")
	}
}
func (this *DataFrame) SaveFile(dst string) (err error) {
	var file *os.File
	var gw *gzip.Writer
	if err = os.MkdirAll(path.Dir(dst), os.ModePerm); err != nil {
		return errs.New(err)
	}
	if file, err = os.Create(dst); err != nil {
		return errs.New(err)
	}
	defer file.Close()
	gw = gzip.NewWriter(file)
	defer gw.Close()
	_, err = FrameReader{DataFrame: this}.WriteTo(gw)
	return err
}
func (this *DataFrame) SaveTo(dst *url.URL, name string) error {
	return nil
}
func (this *DataFrame) Save(name string) error {
	if dst, err := url.Parse(name); err != nil {
		return err
	} else if dst.Scheme == "file" {
		return this.SaveFile(dst.Host + dst.Path)
	} else if dst.Scheme == "http" {
		return this.SaveTo(dst, name)
	} else {
		return errs.New("scheme is error!")
	}
}

var bytesBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 4096*4096))
	},
}

func (this FrameReader) Close() error {
	if this.bs != nil {
		bufioReaderPool.Put(this.bs)
		this.bs = nil
	}
	return nil
}

func (this FrameReader) WriteTo(w io.Writer) (n int64, err error) {
	switch this.Format {
	case PfTar:
		return this.TarWriteTo(w)
	case PfTxtRow:
		return this.TxtRowWriteTo(w)
	case PfJsonRow:
		return this.JsonRowWriteTo(w)
	case PfJsonArray:
		return this.JsonArrayWriteTo(w)
	case PfJsonColumn:
		return this.JsonColumnWriteTo(w)
	default:
		return this.TarWriteTo(w)
	}
}

func (this FrameReader) Read(p []byte) (int, error) {
	if this.bs == nil {
		this.bs = bytesBufferPool.Get().(*bytes.Buffer)
		this.bs.Reset()
		if _, err := this.WriteTo(this.bs); err != nil {
			return 0, err
		}
	}
	return this.bs.Read(p)
}

func (this *FrameWriter) ReadFrom(r io.Reader) (n int64, err error) {
	switch this.Format {
	case PfTar:
		return this.TarReadFrom(r)
	case PfJsonRow:
		return this.JsonReadFrom(r)
	case PfJsonColumn:
		return this.JsonColumnReadFrom(r)
	default:
		return this.TarReadFrom(r)
	}
}

func (this FrameWriter) Write(p []byte) (n int, err error) {
	if this.bs == nil {
		this.bs = bytesBufferPool.Get().(*bytes.Buffer)
		this.bs.Reset()
	}
	return this.bs.Write(p)
}

func (this FrameWriter) Close() (err error) {
	if this.bs != nil {
		_, err = this.ReadFrom(this.bs)
		bytesBufferPool.Put(this.bs)
		this.bs = nil
	}
	return
}
