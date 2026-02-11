package api

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type expressionContext struct {
	names  map[string]string
	values map[string]types.AttributeValue
}

func newExpressionContext(names map[string]string, values map[string]types.AttributeValue) expressionContext {
	if names == nil {
		names = map[string]string{}
	}
	if values == nil {
		values = map[string]types.AttributeValue{}
	}
	return expressionContext{names: names, values: values}
}

func (c expressionContext) resolveName(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, "#") {
		if resolved, ok := c.names[name]; ok {
			return resolved
		}
	}
	return name
}

func (c expressionContext) resolveValue(token string) (types.AttributeValue, error) {
	token = strings.TrimSpace(token)
	v, ok := c.values[token]
	if !ok {
		return nil, fmt.Errorf("ValidationException: missing expression value %s", token)
	}
	return v, nil
}

type tokenType int

const (
	tokEOF tokenType = iota
	tokIdent
	tokNameRef
	tokValueRef
	tokLParen
	tokRParen
	tokComma
	tokEqual
	tokNotEqual
	tokLT
	tokLTE
	tokGT
	tokGTE
)

type token struct {
	typ tokenType
	lit string
}

func lexExpression(input string) ([]token, error) {
	var out []token
	for i := 0; i < len(input); {
		r := rune(input[i])
		if unicode.IsSpace(r) {
			i++
			continue
		}
		switch input[i] {
		case '(':
			out = append(out, token{typ: tokLParen, lit: "("})
			i++
		case ')':
			out = append(out, token{typ: tokRParen, lit: ")"})
			i++
		case ',':
			out = append(out, token{typ: tokComma, lit: ","})
			i++
		case '=':
			out = append(out, token{typ: tokEqual, lit: "="})
			i++
		case '<':
			if i+1 < len(input) && input[i+1] == '>' {
				out = append(out, token{typ: tokNotEqual, lit: "<>"})
				i += 2
			} else if i+1 < len(input) && input[i+1] == '=' {
				out = append(out, token{typ: tokLTE, lit: "<="})
				i += 2
			} else {
				out = append(out, token{typ: tokLT, lit: "<"})
				i++
			}
		case '>':
			if i+1 < len(input) && input[i+1] == '=' {
				out = append(out, token{typ: tokGTE, lit: ">="})
				i += 2
			} else {
				out = append(out, token{typ: tokGT, lit: ">"})
				i++
			}
		case '#':
			j := i + 1
			for j < len(input) && isNameChar(input[j]) {
				j++
			}
			if j == i+1 {
				return nil, fmt.Errorf("ValidationException: invalid name reference")
			}
			out = append(out, token{typ: tokNameRef, lit: input[i:j]})
			i = j
		case ':':
			j := i + 1
			for j < len(input) && isNameChar(input[j]) {
				j++
			}
			if j == i+1 {
				return nil, fmt.Errorf("ValidationException: invalid value reference")
			}
			out = append(out, token{typ: tokValueRef, lit: input[i:j]})
			i = j
		default:
			if !isNameChar(input[i]) {
				return nil, fmt.Errorf("ValidationException: invalid token %q", string(input[i]))
			}
			j := i
			for j < len(input) && isNameChar(input[j]) {
				j++
			}
			out = append(out, token{typ: tokIdent, lit: input[i:j]})
			i = j
		}
	}
	out = append(out, token{typ: tokEOF})
	return out, nil
}

func isNameChar(b byte) bool {
	return b == '_' || b == '-' || b == '.' || unicode.IsLetter(rune(b)) || unicode.IsDigit(rune(b))
}

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) curr() token { return p.tokens[p.pos] }
func (p *parser) next() {
	if p.pos < len(p.tokens)-1 {
		p.pos++
	}
}
func (p *parser) match(tt tokenType) bool {
	if p.curr().typ == tt {
		p.next()
		return true
	}
	return false
}

func (p *parser) matchKeyword(word string) bool {
	if p.curr().typ == tokIdent && strings.EqualFold(p.curr().lit, word) {
		p.next()
		return true
	}
	return false
}

func (p *parser) expect(tt tokenType, msg string) error {
	if !p.match(tt) {
		return fmt.Errorf("ValidationException: %s", msg)
	}
	return nil
}

type boolExpr interface {
	eval(map[string]types.AttributeValue, expressionContext) (bool, error)
}
type valueExpr interface {
	eval(map[string]types.AttributeValue, expressionContext) (types.AttributeValue, error)
}

type logicalExpr struct {
	op          string
	left, right boolExpr
}

func (e logicalExpr) eval(item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	l, err := e.left.eval(item, ctx)
	if err != nil {
		return false, err
	}
	if e.op == "OR" && l {
		return true, nil
	}
	if e.op == "AND" && !l {
		return false, nil
	}
	r, err := e.right.eval(item, ctx)
	if err != nil {
		return false, err
	}
	if e.op == "AND" {
		return l && r, nil
	}
	return l || r, nil
}

type notExpr struct{ inner boolExpr }

func (e notExpr) eval(item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	v, err := e.inner.eval(item, ctx)
	return !v, err
}

type attrPath struct{ name string }

func (e attrPath) eval(item map[string]types.AttributeValue, ctx expressionContext) (types.AttributeValue, error) {
	return item[ctx.resolveName(e.name)], nil
}

type valueRef struct{ ref string }

func (e valueRef) eval(_ map[string]types.AttributeValue, ctx expressionContext) (types.AttributeValue, error) {
	return ctx.resolveValue(e.ref)
}

type sizeExpr struct{ path attrPath }

func (e sizeExpr) eval(item map[string]types.AttributeValue, ctx expressionContext) (types.AttributeValue, error) {
	v, _ := e.path.eval(item, ctx)
	if v == nil {
		return nil, nil
	}
	sz, err := attributeValueSize(v)
	if err != nil {
		return nil, err
	}
	return &types.AttributeValueMemberN{Value: strconv.Itoa(sz)}, nil
}

type cmpExpr struct {
	left  valueExpr
	op    string
	right valueExpr
}

func (e cmpExpr) eval(item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	l, err := e.left.eval(item, ctx)
	if err != nil {
		return false, err
	}
	r, err := e.right.eval(item, ctx)
	if err != nil {
		return false, err
	}
	if l == nil || r == nil {
		return false, nil
	}
	if e.op == "<>" {
		return !attributeValueEqual(l, r), nil
	}
	cmp, err := compareAttributeValues(l, r)
	if err != nil {
		return false, err
	}
	switch e.op {
	case "=":
		return cmp == 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	}
	return false, fmt.Errorf("ValidationException: unsupported ConditionExpression")
}

type betweenExpr struct{ target, lower, upper valueExpr }

func (e betweenExpr) eval(item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	t, err := e.target.eval(item, ctx)
	if err != nil {
		return false, err
	}
	l, err := e.lower.eval(item, ctx)
	if err != nil {
		return false, err
	}
	u, err := e.upper.eval(item, ctx)
	if err != nil {
		return false, err
	}
	if t == nil || l == nil || u == nil {
		return false, nil
	}
	c1, err := compareAttributeValues(t, l)
	if err != nil {
		return false, err
	}
	c2, err := compareAttributeValues(t, u)
	if err != nil {
		return false, err
	}
	return c1 >= 0 && c2 <= 0, nil
}

type inExpr struct {
	target  valueExpr
	choices []valueExpr
}

func (e inExpr) eval(item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	t, err := e.target.eval(item, ctx)
	if err != nil {
		return false, err
	}
	if t == nil {
		return false, nil
	}
	for _, c := range e.choices {
		v, err := c.eval(item, ctx)
		if err != nil {
			return false, err
		}
		if v != nil && attributeValueEqual(t, v) {
			return true, nil
		}
	}
	return false, nil
}

type fnExpr struct {
	name string
	args []valueExpr
}

func (e fnExpr) eval(item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	switch strings.ToLower(e.name) {
	case "attribute_exists":
		if len(e.args) != 1 {
			return false, fmt.Errorf("ValidationException: invalid attribute_exists")
		}
		v, err := e.args[0].eval(item, ctx)
		if err != nil {
			return false, err
		}
		return v != nil, nil
	case "attribute_not_exists":
		if len(e.args) != 1 {
			return false, fmt.Errorf("ValidationException: invalid attribute_not_exists")
		}
		v, err := e.args[0].eval(item, ctx)
		if err != nil {
			return false, err
		}
		return v == nil, nil
	case "begins_with":
		if len(e.args) != 2 {
			return false, fmt.Errorf("ValidationException: invalid begins_with")
		}
		a, err := e.args[0].eval(item, ctx)
		if err != nil {
			return false, err
		}
		b, err := e.args[1].eval(item, ctx)
		if err != nil {
			return false, err
		}
		as, okA := a.(*types.AttributeValueMemberS)
		bs, okB := b.(*types.AttributeValueMemberS)
		if !okA || !okB || a == nil || b == nil {
			return false, nil
		}
		return strings.HasPrefix(as.Value, bs.Value), nil
	case "contains":
		if len(e.args) != 2 {
			return false, fmt.Errorf("ValidationException: invalid contains")
		}
		a, err := e.args[0].eval(item, ctx)
		if err != nil {
			return false, err
		}
		b, err := e.args[1].eval(item, ctx)
		if err != nil {
			return false, err
		}
		return containsAttributeValue(a, b)
	default:
		return false, fmt.Errorf("ValidationException: unsupported ConditionExpression")
	}
}

func parseConditionExpression(expr string) (boolExpr, error) {
	toks, err := lexExpression(expr)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: toks}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.curr().typ != tokEOF {
		return nil, fmt.Errorf("ValidationException: invalid ConditionExpression")
	}
	return node, nil
}

func (p *parser) parseOr() (boolExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = logicalExpr{op: "OR", left: left, right: right}
	}
	return left, nil
}
func (p *parser) parseAnd() (boolExpr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("AND") {
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = logicalExpr{op: "AND", left: left, right: right}
	}
	return left, nil
}
func (p *parser) parseUnary() (boolExpr, error) {
	if p.matchKeyword("NOT") {
		in, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return notExpr{inner: in}, nil
	}
	if p.match(tokLParen) {
		in, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokRParen, "missing ')' in ConditionExpression"); err != nil {
			return nil, err
		}
		return in, nil
	}
	return p.parsePredicate()
}
func (p *parser) parsePredicate() (boolExpr, error) {
	if p.curr().typ == tokIdent && (strings.EqualFold(p.curr().lit, "attribute_exists") || strings.EqualFold(p.curr().lit, "attribute_not_exists") || strings.EqualFold(p.curr().lit, "begins_with") || strings.EqualFold(p.curr().lit, "contains")) {
		name := p.curr().lit
		p.next()
		if err := p.expect(tokLParen, "invalid function expression"); err != nil {
			return nil, err
		}
		args := []valueExpr{}
		for {
			arg, err := p.parseValueOperand()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			if p.match(tokComma) {
				continue
			}
			break
		}
		if err := p.expect(tokRParen, "missing ')' in function"); err != nil {
			return nil, err
		}
		return fnExpr{name: name, args: args}, nil
	}
	left, err := p.parseValueOperand()
	if err != nil {
		return nil, err
	}
	if p.matchKeyword("BETWEEN") {
		low, err := p.parseValueOperand()
		if err != nil {
			return nil, err
		}
		if !p.matchKeyword("AND") {
			return nil, fmt.Errorf("ValidationException: invalid BETWEEN expression")
		}
		high, err := p.parseValueOperand()
		if err != nil {
			return nil, err
		}
		return betweenExpr{target: left, lower: low, upper: high}, nil
	}
	if p.matchKeyword("IN") {
		if err := p.expect(tokLParen, "invalid IN expression"); err != nil {
			return nil, err
		}
		choices := []valueExpr{}
		for {
			v, err := p.parseValueOperand()
			if err != nil {
				return nil, err
			}
			choices = append(choices, v)
			if p.match(tokComma) {
				continue
			}
			break
		}
		if err := p.expect(tokRParen, "invalid IN expression"); err != nil {
			return nil, err
		}
		return inExpr{target: left, choices: choices}, nil
	}
	opTok := p.curr()
	p.next()
	op := ""
	switch opTok.typ {
	case tokEqual:
		op = "="
	case tokNotEqual:
		op = "<>"
	case tokLT:
		op = "<"
	case tokLTE:
		op = "<="
	case tokGT:
		op = ">"
	case tokGTE:
		op = ">="
	default:
		return nil, fmt.Errorf("ValidationException: invalid ConditionExpression")
	}
	right, err := p.parseValueOperand()
	if err != nil {
		return nil, err
	}
	return cmpExpr{left: left, op: op, right: right}, nil
}
func (p *parser) parseValueOperand() (valueExpr, error) {
	if p.curr().typ == tokIdent && strings.EqualFold(p.curr().lit, "size") {
		p.next()
		if err := p.expect(tokLParen, "invalid size expression"); err != nil {
			return nil, err
		}
		path, err := p.parsePathOperand()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokRParen, "invalid size expression"); err != nil {
			return nil, err
		}
		return sizeExpr{path: path}, nil
	}
	if p.curr().typ == tokValueRef {
		lit := p.curr().lit
		p.next()
		return valueRef{ref: lit}, nil
	}
	path, err := p.parsePathOperand()
	if err != nil {
		return nil, err
	}
	return path, nil
}
func (p *parser) parsePathOperand() (attrPath, error) {
	switch p.curr().typ {
	case tokNameRef, tokIdent:
		lit := p.curr().lit
		p.next()
		return attrPath{name: lit}, nil
	default:
		return attrPath{}, fmt.Errorf("ValidationException: invalid attribute path")
	}
}

func evaluateConditionExpression(expr string, item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true, nil
	}
	node, err := parseConditionExpression(expr)
	if err != nil {
		return false, err
	}
	return node.eval(item, ctx)
}

func applyUpdateExpression(item map[string]types.AttributeValue, expr string, ctx expressionContext) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	toks, err := lexExpression(expr)
	if err != nil {
		return err
	}
	p := &parser{tokens: toks}
	for p.curr().typ != tokEOF {
		keyword := ""
		switch {
		case p.matchKeyword("SET"):
			keyword = "SET"
		case p.matchKeyword("REMOVE"):
			keyword = "REMOVE"
		case p.matchKeyword("ADD"):
			keyword = "ADD"
		case p.matchKeyword("DELETE"):
			keyword = "DELETE"
		default:
			return fmt.Errorf("ValidationException: invalid UpdateExpression")
		}
		if err := applyUpdateClauseTokens(item, keyword, p, ctx); err != nil {
			return err
		}
	}
	return nil
}

func applyUpdateClauseTokens(item map[string]types.AttributeValue, keyword string, p *parser, ctx expressionContext) error {
	for {
		switch keyword {
		case "SET":
			path, err := p.parsePathOperand()
			if err != nil {
				return fmt.Errorf("ValidationException: invalid SET expression")
			}
			if err := p.expect(tokEqual, "invalid SET expression"); err != nil {
				return err
			}
			if p.curr().typ != tokValueRef {
				return fmt.Errorf("ValidationException: invalid SET expression")
			}
			v, err := ctx.resolveValue(p.curr().lit)
			if err != nil {
				return err
			}
			p.next()
			item[ctx.resolveName(path.name)] = v
		case "REMOVE":
			path, err := p.parsePathOperand()
			if err != nil {
				return fmt.Errorf("ValidationException: invalid REMOVE expression")
			}
			delete(item, ctx.resolveName(path.name))
		case "ADD":
			path, err := p.parsePathOperand()
			if err != nil {
				return fmt.Errorf("ValidationException: invalid ADD expression")
			}
			if p.curr().typ != tokValueRef {
				return fmt.Errorf("ValidationException: invalid ADD expression")
			}
			v, err := ctx.resolveValue(p.curr().lit)
			if err != nil {
				return err
			}
			p.next()
			updated, err := addAttributeValue(item[ctx.resolveName(path.name)], v)
			if err != nil {
				return err
			}
			item[ctx.resolveName(path.name)] = updated
		case "DELETE":
			path, err := p.parsePathOperand()
			if err != nil {
				return fmt.Errorf("ValidationException: invalid DELETE expression")
			}
			if p.curr().typ != tokValueRef {
				return fmt.Errorf("ValidationException: invalid DELETE expression")
			}
			v, err := ctx.resolveValue(p.curr().lit)
			if err != nil {
				return err
			}
			p.next()
			updated, err := deleteFromSet(item[ctx.resolveName(path.name)], v)
			if err != nil {
				return err
			}
			if updated == nil {
				delete(item, ctx.resolveName(path.name))
			} else {
				item[ctx.resolveName(path.name)] = updated
			}
		}
		if !p.match(tokComma) {
			break
		}
	}
	return nil
}

func attributeValueEqual(a, b types.AttributeValue) bool {
	cmp, err := compareAttributeValues(a, b)
	return err == nil && cmp == 0
}

func compareAttributeValues(a, b types.AttributeValue) (int, error) {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		if !ok {
			return 0, fmt.Errorf("ValidationException: incompatible attribute types")
		}
		return strings.Compare(av.Value, bv.Value), nil
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		if !ok {
			return 0, fmt.Errorf("ValidationException: incompatible attribute types")
		}
		ar, ok := new(big.Rat).SetString(av.Value)
		if !ok {
			return 0, fmt.Errorf("ValidationException: invalid numeric value")
		}
		br, ok := new(big.Rat).SetString(bv.Value)
		if !ok {
			return 0, fmt.Errorf("ValidationException: invalid numeric value")
		}
		return ar.Cmp(br), nil
	case *types.AttributeValueMemberB:
		bv, ok := b.(*types.AttributeValueMemberB)
		if !ok {
			return 0, fmt.Errorf("ValidationException: incompatible attribute types")
		}
		return bytes.Compare(av.Value, bv.Value), nil
	default:
		return 0, fmt.Errorf("ValidationException: unsupported attribute type")
	}
}

func addAttributeValue(current, add types.AttributeValue) (types.AttributeValue, error) {
	if addN, ok := add.(*types.AttributeValueMemberN); ok {
		incRat, ok := new(big.Rat).SetString(addN.Value)
		if !ok {
			return nil, fmt.Errorf("ValidationException: invalid numeric value %q", addN.Value)
		}
		if current == nil {
			return &types.AttributeValueMemberN{Value: addN.Value}, nil
		}
		currNum, ok := current.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fmt.Errorf("ValidationException: ADD requires existing Number attribute")
		}
		currRat, ok := new(big.Rat).SetString(currNum.Value)
		if !ok {
			return nil, fmt.Errorf("ValidationException: invalid numeric value %q", currNum.Value)
		}
		currRat.Add(currRat, incRat)
		return &types.AttributeValueMemberN{Value: ratToDynamoNumber(currRat)}, nil
	}
	return unionSet(current, add)
}

func ratToDynamoNumber(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	s := r.FloatString(10)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" || s == "-" {
		return "0"
	}
	return s
}

func deleteFromSet(current, remove types.AttributeValue) (types.AttributeValue, error) {
	if current == nil {
		return nil, nil
	}
	switch c := current.(type) {
	case *types.AttributeValueMemberSS:
		r, ok := remove.(*types.AttributeValueMemberSS)
		if !ok {
			return nil, fmt.Errorf("ValidationException: DELETE set type mismatch")
		}
		return subtractStringSet(c.Value, r.Value), nil
	case *types.AttributeValueMemberNS:
		r, ok := remove.(*types.AttributeValueMemberNS)
		if !ok {
			return nil, fmt.Errorf("ValidationException: DELETE set type mismatch")
		}
		return subtractStringSetAsNS(c.Value, r.Value), nil
	case *types.AttributeValueMemberBS:
		r, ok := remove.(*types.AttributeValueMemberBS)
		if !ok {
			return nil, fmt.Errorf("ValidationException: DELETE set type mismatch")
		}
		return subtractBinarySet(c.Value, r.Value), nil
	default:
		return nil, fmt.Errorf("ValidationException: DELETE currently supports Set values")
	}
}

func unionSet(current, add types.AttributeValue) (types.AttributeValue, error) {
	switch a := add.(type) {
	case *types.AttributeValueMemberSS:
		cur := []string{}
		if current != nil {
			s, ok := current.(*types.AttributeValueMemberSS)
			if !ok {
				return nil, fmt.Errorf("ValidationException: ADD set type mismatch")
			}
			cur = s.Value
		}
		return &types.AttributeValueMemberSS{Value: uniqueStrings(append(append([]string{}, cur...), a.Value...))}, nil
	case *types.AttributeValueMemberNS:
		cur := []string{}
		if current != nil {
			s, ok := current.(*types.AttributeValueMemberNS)
			if !ok {
				return nil, fmt.Errorf("ValidationException: ADD set type mismatch")
			}
			cur = s.Value
		}
		return &types.AttributeValueMemberNS{Value: uniqueStrings(append(append([]string{}, cur...), a.Value...))}, nil
	case *types.AttributeValueMemberBS:
		var cur [][]byte
		if current != nil {
			s, ok := current.(*types.AttributeValueMemberBS)
			if !ok {
				return nil, fmt.Errorf("ValidationException: ADD set type mismatch")
			}
			cur = s.Value
		}
		return &types.AttributeValueMemberBS{Value: uniqueBinary(append(append([][]byte{}, cur...), a.Value...))}, nil
	default:
		return nil, fmt.Errorf("ValidationException: ADD currently supports Number or Set values")
	}
}

func subtractStringSet(current, remove []string) types.AttributeValue {
	rm := map[string]struct{}{}
	for _, v := range remove {
		rm[v] = struct{}{}
	}
	out := make([]string, 0, len(current))
	for _, v := range current {
		if _, ok := rm[v]; !ok {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return &types.AttributeValueMemberSS{Value: out}
}
func subtractStringSetAsNS(current, remove []string) types.AttributeValue {
	rm := map[string]struct{}{}
	for _, v := range remove {
		rm[v] = struct{}{}
	}
	out := make([]string, 0, len(current))
	for _, v := range current {
		if _, ok := rm[v]; !ok {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return &types.AttributeValueMemberNS{Value: out}
}
func subtractBinarySet(current, remove [][]byte) types.AttributeValue {
	rm := map[string]struct{}{}
	for _, v := range remove {
		rm[string(v)] = struct{}{}
	}
	out := make([][]byte, 0, len(current))
	for _, v := range current {
		if _, ok := rm[string(v)]; !ok {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return &types.AttributeValueMemberBS{Value: out}
}
func uniqueStrings(in []string) []string {
	m := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, ok := m[v]; !ok {
			m[v] = struct{}{}
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}
func uniqueBinary(in [][]byte) [][]byte {
	m := map[string][]byte{}
	for _, v := range in {
		m[string(v)] = v
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}
	return out
}

func attributeValueSize(v types.AttributeValue) (int, error) {
	switch t := v.(type) {
	case *types.AttributeValueMemberS:
		return len(t.Value), nil
	case *types.AttributeValueMemberB:
		return len(t.Value), nil
	case *types.AttributeValueMemberSS:
		return len(t.Value), nil
	case *types.AttributeValueMemberNS:
		return len(t.Value), nil
	case *types.AttributeValueMemberBS:
		return len(t.Value), nil
	case *types.AttributeValueMemberL:
		return len(t.Value), nil
	case *types.AttributeValueMemberM:
		return len(t.Value), nil
	default:
		return 0, fmt.Errorf("ValidationException: size() unsupported for attribute type")
	}
}

func containsAttributeValue(container, needle types.AttributeValue) (bool, error) {
	if container == nil || needle == nil {
		return false, nil
	}
	switch c := container.(type) {
	case *types.AttributeValueMemberS:
		n, ok := needle.(*types.AttributeValueMemberS)
		if !ok {
			return false, nil
		}
		return strings.Contains(c.Value, n.Value), nil
	case *types.AttributeValueMemberSS:
		n, ok := needle.(*types.AttributeValueMemberS)
		if !ok {
			return false, nil
		}
		for _, v := range c.Value {
			if v == n.Value {
				return true, nil
			}
		}
		return false, nil
	case *types.AttributeValueMemberNS:
		n, ok := needle.(*types.AttributeValueMemberN)
		if !ok {
			return false, nil
		}
		for _, v := range c.Value {
			if v == n.Value {
				return true, nil
			}
		}
		return false, nil
	case *types.AttributeValueMemberBS:
		n, ok := needle.(*types.AttributeValueMemberB)
		if !ok {
			return false, nil
		}
		for _, v := range c.Value {
			if bytes.Equal(v, n.Value) {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, nil
	}
}

func updatedAttributes(oldItem, newItem map[string]types.AttributeValue) (updatedOld, updatedNew map[string]types.AttributeValue) {
	updatedOld = map[string]types.AttributeValue{}
	updatedNew = map[string]types.AttributeValue{}
	seen := map[string]struct{}{}
	for k := range oldItem {
		seen[k] = struct{}{}
	}
	for k := range newItem {
		seen[k] = struct{}{}
	}
	for k := range seen {
		ov := oldItem[k]
		nv := newItem[k]
		if ov == nil && nv == nil {
			continue
		}
		if ov != nil && nv != nil && attributeValueEqual(ov, nv) {
			continue
		}
		if ov != nil {
			updatedOld[k] = ov
		}
		if nv != nil {
			updatedNew[k] = nv
		}
	}
	if len(updatedOld) == 0 {
		updatedOld = nil
	}
	if len(updatedNew) == 0 {
		updatedNew = nil
	}
	return
}
