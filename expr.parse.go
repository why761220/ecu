package ecu

import (

	"regexp"
	"strconv"
	"strings"
	"time"
)

type JoinColumn struct {
	Left  Expr `json:"left,omitempty"`
	Right Expr `json:"right,omitempty"`
}

type Table struct {
	Name     string   `json:"name,omitempty"`
	Alias    string   `json:"alias,omitempty"`
	InnerSel *SelExpr `json:"innerSel,omitempty"`
}
type OutColumn struct {
	Expr  *Expr  `json:"expr,omitempty"`
	Alias string `json:"alias,omitempty"`
}

type Join struct {
	Table
	Cols []JoinColumn `json:"cols,omitempty"`
}
type GrpByTime struct {
	Name      string        `json:"name,omitempty"`
	Prefix    string        `json:"prefix,omitempty"`
	Precision string        `json:"precision,omitempty"`
	Offset    time.Duration `json:"offset,omitempty"`
}
type SelExpr struct {
	Table
	Top     int          `json:"top,omitempty"`
	Outs    []*OutColumn `json:"outs,omitempty"`
	Joins   []Join       `json:"joins,omitempty"`
	Where   *Expr        `json:"where,omitempty"`
	TimeGrp *GrpByTime   `json:"timeGrp,omitempty"`
	GrpBys  []Column     `json:"grpBys,omitempty"`
	Having  *Expr        `json:"having,omitempty"`
	Sorts   []Sort       `json:"sorts,omitempty"`
	Limit   uint         `json:"limit,omitempty"`
}

func New(Oper OperType, args ...*Expr) (*Expr, error) {
	return &Expr{Oper: Oper, Args: args}, nil
}
func NewAll(Prefix string) *Expr {
	return &Expr{Oper: All, Prefix: Prefix}
}
func NewConst(value interface{}) (*Expr, error) {
	switch v := value.(type) {
	case nil:
		return &Expr{Oper: Const, Const: nil}, nil
	case int64:
		return &Expr{Oper: Const, Const: intValue(v)}, nil
	case float64:
		return &Expr{Oper: Const, Const: floatValue(v)}, nil
	case string:
		return &Expr{Oper: Const, Const: strValue(v)}, nil
	case bool:
		return &Expr{Oper: Const, Const: b2v(v)}, nil
	default:
		return nil, errs.New("type is error!")
	}
}
func NewField(field, Prefix string) *Expr {
	return &Expr{Oper: Field, Field: field, Prefix: Prefix}
}
func NewSelectField(sel *SelExpr) (*Expr, error) {
	return &Expr{Oper: SelectField, sel: sel}, nil
}
func NewBetween(opr OperType, left, a1, a2 *Expr) (*Expr, error) {
	switch opr {
	case Between, NotBetween:
		return &Expr{Oper: opr, Args: []*Expr{left, a1, a2}}, nil
	default:
		return nil, errs.New("opr arg is error!")
	}
}
func NewIn(opr OperType, left *Expr, argList []*Expr) (*Expr, error) {
	args := append(append(make([]*Expr, 0, len(argList)+1), left), argList...)
	switch opr {
	case In, NotIn:
		return &Expr{Oper: opr, Args: args}, nil
	default:
		return nil, errs.New("opr arg is error!")
	}
}

//select--from--join--where--group by--having--order by---

type parser struct {
	sql string
}
type token struct {
	key string
	pos int
}

func (this token) lower() string {
	return strings.ToLower(this.key)
}

var (
	tokenChars map[rune]string
	ToOpr      map[string]OperType
)

func init() {
	tokenChars = map[rune]string{
		' ':  "",
		'\t': "",
		'\r': "",
		'\n': "",
		',':  ",",
		'>':  ">",
		'<':  "<",
		'!':  "!",
		'=':  "=",
		'(':  "(",
		')':  ")",
		'+':  "+",
		'-':  "-",
		'*':  "*",
		'/':  "/",
	}
	ToOpr = map[string]OperType{
		"*":           Multiple,
		"/":           Divide,
		"+":           Plus,
		"-":           Minus,
		">":           Greater,
		"<":           Lesser,
		">=":          GreaterEqual,
		"<=":          LesserEqual,
		"=":           Equal,
		"<>":          NotEqual,
		"isnull":      IsNull,
		"isnotnull":   IsNotNull,
		"like":        Like,
		"notin":       NotIn,
		"in":          In,
		"between":     Between,
		"notbetween":  NotBetween,
		"not":         Not,
		"and":         And,
		"or":          Or,
		"timestamp":   Timestamp,
		"recordvalue": RecordValue,
		"to_float":    ToFloat,
		"to_int":      ToInt,
		"count":       Count,
		"distinct":    DistinctCount,
		"max":         Max,
		"min":         Min,
		"avg":         Avg,
		"sum":         Sum,
		//"group":      19,
		//"by":         20,
		"ifnull":      IfNull,
		"now":         Now,
		"lastrecord":  LastRecord,
		"firstrecord": FirstRecord,
	}
}
func retkeys(i int, ks []token) []token {
	if i >= len(ks) {
		return ks[0:0]
	} else {
		return ks[i:]
	}
}

func (this *parser) OprComp(v1, v2 OperType) bool {
	return v1 <= v2
}
func (this *parser) isAll(ks []token) (*Expr, []token, bool) {
	if l := len(ks); l > 1 {
		if ks[1].key == "*" && strings.HasSuffix(ks[0].key, ".") {
			return NewAll(strings.TrimSuffix(ks[0].key, ".")), retkeys(2, ks), true
		}
	} else if l > 0 {
		ss := strings.Split(ks[0].key, ".")
		switch len(ss) {
		case 1:
			if ss[0] == "*" {
				return NewAll(""), retkeys(1, ks), true
			} else {
				return nil, ks, false
			}
		case 2:
			if ss[1] == "*" {
				return NewAll(ss[0]), retkeys(1, ks), true
			} else {
				return nil, ks, false
			}
		}
	}

	return nil, ks, false
}
func (this *parser) isField(s string) (string, string, bool) {
	ss := strings.Split(s, ".")
	switch len(ss) {
	case 1:
		if ok, _ := regexp.MatchString(`\w`, ss[0]); ok {
			return ss[0], "", true
		} else {
			return "", "", false
		}
	case 2:
		if ok, _ := regexp.MatchString(`\w`, ss[0]); ok {
			if ok, _ := regexp.MatchString(`\w`, ss[1]); ok {
				return ss[1], ss[0], true
			}
		}
		return "", "", false
	default:
		return "", "", false
	}
}
func (this *parser) isConst(v string) (*Expr, bool) {
	if strings.HasPrefix(v, "'") {
		if strings.HasSuffix(v, "'") {
			v = strings.Trim(v, "'")
			// if ti, err := time.ParseInLocation(DateTimeLayOut, v, time.Local); err == nil {
			// 	if e, err := NewConst(ti.UnixNano()); err != nil {
			// 		return nil, false
			// 	} else {
			// 		return e, true
			// 	}
			// } else if ti, err = time.ParseInLocation(DateLayOut, v, time.Local); err == nil {
			// 	if e, err := NewConst(ti.UnixNano()); err != nil {
			// 		return nil, false
			// 	} else {
			// 		return e, true
			// 	}
			// }
			if e, err := NewConst(v); err != nil {
				return nil, false
			} else {
				return e, true
			}
		} else {
			return nil, false
		}
	} else if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		if e, err := NewConst(i); err != nil {
			return nil, false
		} else {
			return e, true
		}
	} else if f, err := strconv.ParseFloat(v, 64); err == nil {
		if e, err := NewConst(f); err != nil {
			return nil, false
		} else {
			return e, true
		}
	} else if b, err := strconv.ParseBool(v); err == nil {
		if e, err := NewConst(b); err != nil {
			return nil, false
		} else {
			return e, true
		}
	} else {
		return nil, false
	}
}

func (this *parser) Token(sql string) []token {
	this.sql = sql
	ret := make([]token, 0, 100)
	var t string
	yh := false
	pos := 0
	for i, s := range sql {

		if s == '\'' {
			if yh {
				ret = append(ret, token{key: "'" + t + "'", pos: i})
				t = ""
			} else {
				pos = i
			}
			yh = !yh
			continue
		}
		if yh {
			t += string(s)
		} else {
			if v, ok := tokenChars[s]; ok {
				if len(t) > 0 {
					ret = append(ret, token{key: t, pos: pos})
					t = ""
				}
				if v != "" {
					ret = append(ret, token{key: v, pos: i})
				}
			} else {
				if t == "" {
					pos = i
				}
				t += string(s)
			}
		}

	}
	if len(t) > 0 {
		ret = append(ret, token{key: t, pos: pos})
	}
	return ret
}

func (this *parser) GrammarBrackets(ks []token) (*Expr, []token, error) {
	cnt := 0
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			} else {
				if ks[0].lower() == "select" {
					if sel, err := this.AnalysisSelect(ks[0:i]); err != nil {
						return nil, nil, err
					} else if e, err := NewSelectField(sel); err != nil {
						return nil, nil, err
					} else {
						return e, retkeys(i+1, ks), nil
					}
				} else if e, err := this.GrammarFirst(ks[0:i]); err != nil {
					return nil, nil, err
				} else if e, err := New(Brackets, e); err != nil {
					return nil, nil, err
				} else {
					return e, retkeys(i+1, ks), nil
				}
			}
		}
	}
	return nil, nil, errs.New("expr is incomplete!")
}

func (this *parser) GrammarMember(ks []token) (*Expr, []token, error) {

	s := ks[0].key
	if s == "(" {
		return this.GrammarBrackets(ks[1:])
	} else if len(ks) > 2 && ks[1].key == "(" {
		return this.GrammarFunc(s, ks[2:])
	} else if v, ok := this.isConst(s); ok {
		return v, retkeys(1, ks), nil
	} else if e, rks, ok := this.isAll(ks); ok {
		return e, rks, nil
	} else if name, prefix, ok := this.isField(s); ok {
		return NewField(name, prefix), retkeys(1, ks), nil
	}
	return nil, nil, errs.New("unkown member!")
}

func (this *parser) GrammarOperator(ks []token) (OperType, []token, error) {
	loop := func() (i int, opr string) {
		var s token
		for i, s = range ks {
			switch s.key {
			case "+", "-", "*", "/", "=", "<", ">", "and", "like", "or", "is", "not", "null", "between", "in":
				opr += s.key
			default:
				return
			}
		}
		if i == len(ks)-1 {
			i++
		}
		return
	}

	if i, o := loop(); o == "" {
		return 0, ks, errs.New("invalid opr for " + ks[0].lower())
	} else if opr, ok := ToOpr[o]; ok {
		return opr, ks[i:], nil
	} else {
		return opr, ks[i:], errs.New("invalid opr for " + o)
	}
}

func (this *parser) GrammarFirst(ks []token) (*Expr, error) {
	var left *Expr
	var err error
	var opr OperType
	if len(ks) == 0 {
		return nil, errs.New("111")
	}
	if left, ks, err = this.GrammarMember(ks); err != nil {
		return nil, err
	} else if len(ks) == 0 {
		return left, nil
	}
	if opr, ks, err = this.GrammarOperator(ks); err != nil {
		return nil, err
	}
	switch opr {
	case IsNull, IsNotNull:
		if left, err = New(opr, left); err != nil {
			return nil, err
		} else if len(ks) == 0 {
			return left, nil
		} else if opr, ks, err = this.GrammarOperator(ks); err != nil {
			return nil, err
		}
	}
	if len(ks) == 0 {
		return nil, errs.New("expr is incomplete!")
	}
	return this.GrammarExpr(left, opr, ks)
}

func (this *parser) GrammarExpr(prior *Expr, popr OperType, ks []token) (*Expr, error) {

	var left *Expr
	var err error
	var opr OperType

	if len(ks) == 0 {
		return nil, errs.New("expr is incomplete!")
	}
	switch popr {
	case Not:
		switch ks[0].lower() {
		case "between":
			return this.GrammarExpr(prior, NotBetween, ks[1:])
		case "in":
			return this.GrammarExpr(prior, NotIn, ks[1:])
		}
	case In, NotIn:
		if prior, ks, err = this.GrammarIn(popr, prior, ks); err != nil {
			return nil, err
		} else if len(ks) > 0 {
			if popr, ks, err = this.GrammarOperator(ks); err != nil {
				return nil, err
			} else {
				return this.GrammarExpr(prior, popr, ks)
			}
		} else {
			return prior, nil
		}
	case Between, NotBetween:
		return this.GrammarBetween(popr, prior, ks)
	}

	if left, ks, err = this.GrammarMember(ks); err != nil {
		return nil, err
	} else if len(ks) == 0 {
		return New(popr, prior, left)
	}

	if opr, ks, err = this.GrammarOperator(ks); err != nil {
		return nil, err
	} else if len(ks) == 0 {
		return nil, errs.New("expr is incomplete!")
	}

	if this.OprComp(popr, opr) {
		if prior, err = New(popr, prior, left); err != nil {
			return nil, err
		} else {
			return this.GrammarExpr(prior, opr, ks)
		}
	} else if left, err = this.GrammarExpr(left, opr, ks); err != nil {
		return nil, err
	} else {
		return New(popr, prior, left)
	}
}

func (this *parser) GrammarBetweenAnd2(prior *Expr, popr OperType, _ks []token) (left *Expr, opr OperType, ks []token, err error) {
	ks = _ks
	if left, ks, err = this.GrammarMember(ks); err != nil {
		return
	} else if len(ks) == 0 {
		left, err = New(popr, prior, left)
		return
	}

	if opr, ks, err = this.GrammarOperator(ks); err != nil {
		return
	} else if len(ks) == 0 {
		err = errs.New("expr is incomplete!")
		return
	}

	if this.OprComp(Between, opr) {
		left, err = New(popr, prior, left)
		return
	} else if this.OprComp(popr, opr) {
		if left, err = New(popr, prior, left); err != nil {
			return
		} else {
			return this.GrammarBetweenAnd2(left, opr, ks)
		}
	} else {
		if left, opr, ks, err = this.GrammarBetweenAnd2(left, opr, ks); err != nil {
			return
		} else {
			left, err = New(popr, prior, left)
			return
		}
	}
}
func (this *parser) GrammarBetweenAnd(bleft *Expr, bopr OperType, a1 *Expr, ks []token) (left *Expr, err error) {
	var opr OperType

	if left, ks, err = this.GrammarMember(ks); err != nil {
		return
	} else if len(ks) == 0 {
		return NewBetween(bopr, bleft, a1, left)
	}

	if opr, ks, err = this.GrammarOperator(ks); err != nil {
		return nil, err
	} else if len(ks) == 0 {
		return nil, errs.New("expr is incomplete!")
	}

	if this.OprComp(bopr, opr) {
		if left, err = NewBetween(bopr, bleft, a1, left); err != nil {
			return
		} else {
			return this.GrammarExpr(left, opr, ks)
		}
	} else if left, opr, ks, err = this.GrammarBetweenAnd2(left, opr, ks); err != nil {
		return
	} else if left, err = NewBetween(bopr, bleft, a1, left); err != nil {
		return
	} else if len(ks) > 0 && opr != OperInvalid {
		return this.GrammarExpr(left, opr, ks)
	} else {
		return
	}
}
func (this *parser) GrammarBetween(popr OperType, left *Expr, ks []token) (*Expr, error) {
	cnt := 0
	for i, s := range ks {
		switch s.lower() {
		case "(":
			cnt++
		case ")":
			cnt--
		case "and":
			if a1, err := this.GrammarFirst(ks[:i]); err != nil {
				return nil, err
			} else if ks = ks[i+1:]; len(ks) > 0 {
				return this.GrammarBetweenAnd(left, popr, a1, ks)
			} else {
				return nil, errs.New("expr is incomplete!")
			}
		}
	}
	return nil, errs.New("expr is incomplete!")
}
func (this *parser) GrammarIn(popr OperType, left *Expr, ks []token) (*Expr, []token, error) {
	if len(ks) < 2 || ks[0].lower() != "(" {
		return nil, nil, errs.New("expr is incomplete!")
	}
	ks = ks[1:]

	cnt, pos, args := 0, 0, make([]*Expr, 0, 10)
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			} else {
				if i > pos {
					if e, err := this.GrammarFirst(ks[pos:i]); err != nil {
						return nil, nil, err
					} else if e, err = NewIn(popr, left, append(args, e)); err != nil {
						return nil, nil, err
					} else {
						return e, retkeys(i+1, ks), nil
					}
				} else {
					return nil, nil, errs.New("in args is empty!")
				}
			}
		case ",":
			if cnt == 0 {
				if i > pos {
					if e, err := this.GrammarFirst(ks[pos:i]); err != nil {
						return nil, nil, err
					} else {
						args = append(args, e)
						pos = i + 1
					}
				} else {
					return nil, nil, errs.New("arg is empty!")
				}
			}
		}
	}
	return nil, nil, errs.New("expr is incomplete!")
}
func (this *parser) GrammarFunc(name string, ks []token) (*Expr, []token, error) {
	f, ok := ToOpr[strings.ToLower(name)]
	if !ok {
		return nil, nil, errs.New(name + " is unkown function!")
	}
	cnt := 0
	pos := 0
	args := make([]*Expr, 0, 10)
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			} else {
				if i > pos {
					if e, err := this.GrammarFirst(ks[pos:i]); err != nil {
						return nil, nil, err
					} else if e, err = New(f, append(args, e)...); err != nil {
						return nil, nil, err
					} else {
						return e, retkeys(i+1, ks), nil
					}
				} else if len(args) == 0 {
					if e, err := New(f); err != nil {
						return nil, nil, err
					} else {
						return e, retkeys(i+1, ks), nil
					}
				} else {
					return nil, nil, errs.New("arg is empty!")
				}
			}
		case ",":
			if cnt == 0 {
				if i > pos {
					if e, err := this.GrammarFirst(ks[pos:i]); err != nil {
						return nil, nil, err
					} else {
						args = append(args, e)
						pos = i + 1
					}
				} else {
					return nil, nil, errs.New("arg is empty!")
				}
			}
		}
	}
	return nil, nil, errs.New("111")
}

func (this *parser) AnalysisWhere(sel *SelExpr, ks []token) error {
	cnt := 0
	pos := 0
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			}
		case "group":
			if cnt > 0 {
				continue
			}
			if i == pos {
				return errs.New("111")
			}
			if e, err := this.GrammarFirst(ks[pos:i]); err != nil {
				return err
			} else {
				sel.Where = e
				return this.AnalysisGrps(sel, retkeys(i+1, ks))
			}
		case "order":
			if cnt > 0 {
				continue
			}
			if i == pos {
				return errs.New("111")
			}
			if e, err := this.GrammarFirst(ks[pos:i]); err != nil {
				return err
			} else {
				sel.Where = e
				return this.AnalysisOrderBy(sel, retkeys(i+1, ks))
			}
		}
	}

	if e, err := this.GrammarFirst(ks); err != nil {
		return err
	} else {
		sel.Where = e
		return nil
	}
}

func (this *parser) AnalysisTimeOffset(s string) (ret time.Duration, err error) {
	reg1 := regexp.MustCompile(`\d{1,2}d|\d{1,2}h|\d{1,2}mi|\d{1,2}s|\d{1,3}ms|\d{1,3}ns`)
	reg2 := regexp.MustCompile(`(\d+)|(d|h|mi|ms|ns|s)`)
	tss := reg1.FindAllString(s, -1)
	for _, ts := range tss {
		t := reg2.FindAllString(ts, -1)
		if v, err := strconv.ParseInt(t[0], 10, 64); err != nil {
			return time.Duration(0), err
		} else {
			switch t[1] {
			case "d":
				ret += time.Hour * time.Duration(24*v)
			case "h":
				ret += time.Hour * time.Duration(v)
			case "mi":
				ret += time.Minute * time.Duration(v)
			case "s":
				ret += time.Second * time.Duration(v)
			case "ms":
				ret += time.Millisecond * time.Duration(v)
			case "ns":
				ret += time.Nanosecond * time.Duration(v)
			}
		}
	}
	return
}
func (this *parser) AnalysisTop(ks []token) (sel *SelExpr, err error) {
	getTopValue := func(ks []token) (int, error) {
		s := ""
		for _, v := range ks {
			s += v.key
		}
		i, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return int(i), errs.New(err)
		} else {
			return int(i), err
		}
	}
	pos := 0
	for i, s := range ks {
		switch strings.ToLower(s.key) {
		case "for":
			if pos == 0 {
				pos = i
			} else {
				return nil, errs.New("top query missing subsequent keywords!")
			}
		case "select":
			if pos == 0 {
				return nil, errs.New("top query missing subsequent keywords!")
			}
			var topValue int
			if topValue, err = getTopValue(ks[1 : i-1]); err != nil {
				return
			} else if sel, err = this.AnalysisSelect(retkeys(i, ks)); err != nil {
				return
			} else {
				sel.Top = topValue
				return
			}
		}
	}
	return nil, errs.New("top query missing subsequent keywords!")
}
func (this *parser) AnalysisHaving(sel *SelExpr, ks []token) (err error) {
	pos := 0
	cnt := 0
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			}
		case "order":
			if cnt > 0 {
				continue
			}
			if sel.Having, err = this.GrammarFirst(ks[pos:i]); err != nil {
				return err
			}
			switch s.key {
			case "order":
				return this.AnalysisOrderBy(sel, retkeys(i+1, ks))
			}
		}
	}
	if len(ks) > pos {
		if sel.Having, err = this.GrammarFirst(ks[pos:]); err != nil {
			return err
		}
	} else {
		return errs.New("after having not found expr!")
	}
	return nil
}
func (this *parser) AnalysisGrps(sel *SelExpr, ks []token) error {

	AnalyTimeGrp := func(name string, ks []token) (*GrpByTime, error) {
		switch len(ks) {
		case 1:
			return &GrpByTime{Precision: ks[0].key}, nil
		case 3:
			if ks[1].key == "," {
				if offset, err := this.AnalysisTimeOffset(ks[2].key); err != nil {
					return nil, err
				} else {
					return &GrpByTime{Precision: ks[0].key, Offset: offset}, nil
				}
			}
		}
		return nil, errs.New("group by time is error!")
	}
	AnalyGrp := func(ks []token) error {
		l := len(ks)
		if l == 1 {
			if name, prefix, ok := this.isField(ks[0].key); ok {
				sel.GrpBys = append(sel.GrpBys, Column{Field: name, Prefix: prefix})
				return nil
			} else {
				return errs.New("is not field!")
			}
		} else if l > 3 {
			if ks[1].key == "(" && ks[l-1].key == ")" {
				if grp, err := AnalyTimeGrp(ks[0].key, ks[2:l-1]); err != nil {
					return err
				} else {
					sel.TimeGrp = grp
					return nil
				}
			} else {
				return errs.New("111")
			}
		} else {
			return errs.New("111")
		}
	}
	if len(ks) == 0 || ks[0].key != "by" {
		return errs.New("not found 'by' key!")
	} else {
		ks = ks[1:]
	}
	cnt := 0
	pos := 0
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			}
		case ",", "having", "order":
			if cnt > 0 {
				continue
			}
			if err := AnalyGrp(ks[pos:i]); err != nil {
				return err
			}
			switch s.key {
			case ",":
				pos = i + 1
			case "having":
				return this.AnalysisHaving(sel, retkeys(i+1, ks))
			case "order":
				return this.AnalysisOrderBy(sel, retkeys(i+1, ks))
			}
		}
	}
	if len(ks) > pos {
		if err := AnalyGrp(ks[pos:]); err != nil {
			return err
		}
	}
	return nil
}
func (this *parser) AnalysisFromTbl(ks []token) (Table, error) {
	NewSelTbl := func(name, alias string, sel *SelExpr) Table {
		return Table{
			Name:     name,
			Alias:    alias,
			InnerSel: sel,
		}
	}

	l := len(ks)

	if l > 0 && ks[0].key == "(" {
		if l > 2 && ks[l-1].key == ")" {
			if sub, err := this.AnalysisSelect(ks[1 : l-1]); err != nil {
				return Table{}, err
			} else {
				return NewSelTbl("", "", sub), nil
			}
		} else if l > 3 && ks[l-2].key == ")" {
			if sub, err := this.AnalysisSelect(ks[1 : l-2]); err != nil {
				return Table{}, err
			} else {
				return NewSelTbl("", ks[l-1].key, sub), nil
			}
		} else {
			return Table{}, errs.New("111")
		}
	} else if l == 2 {
		return NewSelTbl(ks[0].key, ks[1].key, nil), nil
	} else if l == 1 {
		return NewSelTbl(ks[0].key, "", nil), nil
	} else {
		return Table{}, errs.New("not found where!")
	}
}
func (this *parser) AnalysisOrderField(ks []token) (Sort, error) {
	switch len(ks) {
	case 1:
		return Sort{Name: ks[0].key}, nil
	case 2:
		switch ks[1].key {
		case "desc":
			return Sort{Name: ks[0].key, Desc: true}, nil
		default:
			return Sort{Name: ks[0].key, Desc: false}, nil
		}
	}
	return Sort{}, errs.New("order by field is error!")
}

func (this *parser) AnalysisOrderBy(sel *SelExpr, ks []token) error {
	if len(ks) >= 2 {
		if ks[0].key != "by" {
			return errs.New("'order' next not found 'by'!")
		} else {
			ks = ks[1:]
		}
	}
	pos := 0
	for i, s := range ks {
		switch s.key {
		case ",":
			if i <= pos {
				return errs.New("111")
			}
			if field, err := this.AnalysisOrderField(ks[pos:i]); err != nil {
				return err
			} else {
				sel.Sorts = append(sel.Sorts, field)
			}
			pos = i + 1
		}
	}
	if len(ks) > pos {
		if field, err := this.AnalysisOrderField(ks[pos:]); err != nil {
			return err
		} else {
			sel.Sorts = append(sel.Sorts, field)
		}
	}
	return nil
}
func (this *parser) AnalysisJoinOnFields(ks []token) ([]JoinColumn, error) {
	pos := 0
	ret := make([]JoinColumn, 0)
	add := func(ks []token) error {
		if len(ks) == 3 || ks[1].key == "=" {
			if name1, prefix1, ok1 := this.isField(ks[0].key); ok1 {
				if name2, prefix2, ok2 := this.isField(ks[2].key); ok2 {
					left := Expr{Oper: Field, Field: name1, Prefix: prefix1}
					right := Expr{Oper: Field, Field: name2, Prefix: prefix2}
					ret = append(ret, JoinColumn{Left: left, Right: right})
					return nil
				}
			}
		}
		return errs.New("order by column is error!")
	}
	for i, s := range ks {
		switch s.key {
		case "and":
			if i == (pos + 3) {
				if err := add(ks[pos:i]); err != nil {
					return nil, err
				}
				pos = i + 1
			} else {
				return nil, errs.New("order by column is error!")
			}
		}
	}
	if len(ks) == pos {
		return ret, nil
	} else if len(ks) == (pos + 3) {
		if err := add(ks[pos:]); err != nil {
			return nil, err
		} else {
			return ret, nil
		}
	} else {
		return nil, errs.New("order by column is error!")
	}
}
func (this *parser) AnalysisJoinOn(sel *SelExpr, tbl Table, ks []token) (err error) {
	cnt := 0
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			}
		case "where", "join", "group", "order":
			if cnt > 0 {
				continue
			}
			if i == 0 {
				return errs.New("111")
			}
			if cols, err := this.AnalysisJoinOnFields(ks[0:i]); err != nil {
				return err
			} else {
				sel.Joins = append(sel.Joins, Join{Table: tbl, Cols: cols})
			}
			switch s.key {
			case "where":
				return this.AnalysisWhere(sel, retkeys(i+1, ks))
			case "group":
				return this.AnalysisGrps(sel, retkeys(i+1, ks))
			case "join":
				return this.AnalysisJoin(sel, retkeys(i+1, ks))
			case "order":
				return this.AnalysisOrderBy(sel, retkeys(i+1, ks))
			}
		}
	}
	return errs.New("no not exist at after join!")
}
func (this *parser) AnalysisJoin(sel *SelExpr, ks []token) (err error) {

	cnt := 0
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			}
		case "on":
			if i == 0 {
				return errs.New("111")
			}
			if tbl, err := this.AnalysisFromTbl(ks[0:i]); err != nil {
				return err
			} else {
				return this.AnalysisJoinOn(sel, tbl, retkeys(i+1, ks))
			}
		}
	}
	return errs.New("no not exist at after join!")
}
func (this *parser) AnalysisFrom(sel *SelExpr, ks []token) (err error) {
	cnt := 0
	pos := 0
	for i, s := range ks {
		switch s.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			}
		case "join", "where", "group", "order":
			if cnt > 0 {
				continue
			}
			if i == pos {
				return errs.New("111")
			}
			if sel.Table, err = this.AnalysisFromTbl(ks[pos:i]); err != nil {
				return
			}
			switch s.key {
			case "join":
				return this.AnalysisJoin(sel, retkeys(i+1, ks))
			case "where":
				return this.AnalysisWhere(sel, retkeys(i+1, ks))
			case "group":
				return this.AnalysisGrps(sel, retkeys(i+1, ks))
			case "order":
				return this.AnalysisOrderBy(sel, retkeys(i+1, ks))
			}
		}
	}
	if pos < len(ks) {
		if sel.Table, err = this.AnalysisFromTbl(ks[pos:]); err != nil {
			return err
		}
	} else if sel.Name == "" || sel.InnerSel == nil {
		return errs.New("not found tables!")
	}
	return nil
}

func (this *parser) AnalysisField(sel *SelExpr, ks []token) error {

	var alias string
	if l := len(ks); l >= 3 && ks[l-2].key == "as" {
		alias = ks[l-1].key
		ks = ks[:l-2]
	}
	if e, err := this.GrammarFirst(ks); err != nil {
		return err
	} else {
		sel.Outs = append(sel.Outs, &OutColumn{Expr: e, Alias: alias})
	}
	return nil
}

func (this *parser) AnalysisSelect(ks []token) (sel *SelExpr, err error) {

	if len(ks) == 0 {
		return nil, errs.New("expr is empty!")
	}
	sel = new(SelExpr)
	switch ks[0].key {
	case "select":
	default:
		return nil, errs.New("not found select key as first!")
	}

	ks = ks[1:]
	cnt := 0
	pos := 0

	for i, t := range ks {
		switch t.key {
		case "(":
			cnt++
		case ")":
			if cnt > 0 {
				cnt--
			}
		case "from":
			if cnt > 0 {
				continue
			}
			if i > pos {
				if err := this.AnalysisField(sel, ks[pos:i]); err != nil {
					return nil, err
				}
				//if ks[pos].key == "*" {
				//	if err := this.AnalysisSelAll(sel); err != nil {
				//		return nil, err
				//	}
				//} else if err := this.AnalysisField(sel, ks[pos:i]); err != nil {
				//	return nil, err
				//}
				if len(ks) > (i + 1) {
					if err := this.AnalysisFrom(sel, ks[i+1:]); err != nil {
						return nil, err
					} else {
						return sel, nil
					}
				} else {
					return nil, errs.New("there was nothing after 'from'!")
				}
			} else {
				return nil, errs.New("111")
			}
		case ",":
			if cnt > 0 {
				continue
			}
			if i > pos {
				if err := this.AnalysisField(sel, ks[pos:i]); err != nil {
					return nil, err
				}
				pos = i + 1
			} else {
				return nil, errs.New("field is empty!")
			}
		}
	}
	return nil, errs.New("no 'from' was found!")
}

func Parse(sql string) (interface{}, error) {
	this := parser{}
	ks := this.Token(sql)
	if len(ks) == 0 {
		return nil, errs.New("expr is empty!")
	}
	switch strings.ToLower(ks[0].key) {
	case "select":
		return this.AnalysisSelect(ks)

	default:
		return this.GrammarFirst(ks)
	}
}
func ParseExpr(e string) (*Expr, error) {
	this := parser{}
	ks := this.Token(e)
	if len(ks) == 0 {
		return nil, errs.New("expr is empty!")
	}
	return this.GrammarFirst(ks)
}
func ParseSQL(sql string) (*SelExpr, error) {
	this := parser{}
	ks := this.Token(sql)
	if len(ks) == 0 {
		return nil, errs.New("expr is empty!")
	}
	switch strings.ToLower(ks[0].key) {
	case "select":
		return this.AnalysisSelect(ks)
	case "top":
		return this.AnalysisTop(ks)
	default:
		return nil, errs.New("expr is not select!")
	}
}
