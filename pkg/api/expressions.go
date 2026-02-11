package api

import (
	"fmt"
	"math/big"
	"strings"

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

func evaluateConditionExpression(expr string, item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true, nil
	}
	for _, part := range splitByKeyword(expr, "AND") {
		ok, err := evaluateConditionAtom(strings.TrimSpace(part), item, ctx)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evaluateConditionAtom(atom string, item map[string]types.AttributeValue, ctx expressionContext) (bool, error) {
	if strings.HasPrefix(atom, "attribute_exists(") && strings.HasSuffix(atom, ")") {
		name := strings.TrimSuffix(strings.TrimPrefix(atom, "attribute_exists("), ")")
		_, ok := item[ctx.resolveName(name)]
		return ok, nil
	}
	if strings.HasPrefix(atom, "attribute_not_exists(") && strings.HasSuffix(atom, ")") {
		name := strings.TrimSuffix(strings.TrimPrefix(atom, "attribute_not_exists("), ")")
		_, ok := item[ctx.resolveName(name)]
		return !ok, nil
	}
	if strings.Contains(atom, " <>") || strings.Contains(atom, "<>") {
		parts := strings.SplitN(atom, "<>", 2)
		if len(parts) != 2 {
			return false, fmt.Errorf("ValidationException: invalid ConditionExpression")
		}
		name := ctx.resolveName(parts[0])
		rhs, err := ctx.resolveValue(parts[1])
		if err != nil {
			return false, err
		}
		lhs := item[name]
		if lhs == nil {
			return true, nil
		}
		return !attributeValueEqual(lhs, rhs), nil
	}
	if strings.Contains(atom, "=") {
		parts := strings.SplitN(atom, "=", 2)
		if len(parts) != 2 {
			return false, fmt.Errorf("ValidationException: invalid ConditionExpression")
		}
		name := ctx.resolveName(parts[0])
		rhs, err := ctx.resolveValue(parts[1])
		if err != nil {
			return false, err
		}
		lhs := item[name]
		if lhs == nil {
			return false, nil
		}
		return attributeValueEqual(lhs, rhs), nil
	}
	return false, fmt.Errorf("ValidationException: unsupported ConditionExpression")
}

func applyUpdateExpression(item map[string]types.AttributeValue, expr string, ctx expressionContext) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	remaining := expr
	for len(strings.TrimSpace(remaining)) > 0 {
		remaining = strings.TrimSpace(remaining)
		keyword := ""
		switch {
		case strings.HasPrefix(remaining, "SET "):
			keyword = "SET"
			remaining = strings.TrimSpace(strings.TrimPrefix(remaining, "SET"))
		case strings.HasPrefix(remaining, "REMOVE "):
			keyword = "REMOVE"
			remaining = strings.TrimSpace(strings.TrimPrefix(remaining, "REMOVE"))
		case strings.HasPrefix(remaining, "ADD "):
			keyword = "ADD"
			remaining = strings.TrimSpace(strings.TrimPrefix(remaining, "ADD"))
		case strings.HasPrefix(remaining, "DELETE "):
			keyword = "DELETE"
			remaining = strings.TrimSpace(strings.TrimPrefix(remaining, "DELETE"))
		default:
			return fmt.Errorf("ValidationException: invalid UpdateExpression")
		}
		nextIdx := len(remaining)
		for _, next := range []string{" SET ", " REMOVE ", " ADD ", " DELETE "} {
			if idx := strings.Index(remaining, next); idx >= 0 && idx < nextIdx {
				nextIdx = idx
			}
		}
		clause := strings.TrimSpace(remaining[:nextIdx])
		if err := applyUpdateClause(item, keyword, clause, ctx); err != nil {
			return err
		}
		remaining = remaining[nextIdx:]
	}
	return nil
}

func applyUpdateClause(item map[string]types.AttributeValue, keyword, clause string, ctx expressionContext) error {
	parts := splitCommaParts(clause)
	for _, p := range parts {
		switch keyword {
		case "SET":
			a := strings.SplitN(p, "=", 2)
			if len(a) != 2 {
				return fmt.Errorf("ValidationException: invalid SET expression")
			}
			name := ctx.resolveName(a[0])
			v, err := ctx.resolveValue(a[1])
			if err != nil {
				return err
			}
			item[name] = v
		case "REMOVE":
			delete(item, ctx.resolveName(p))
		case "ADD":
			a := strings.Fields(strings.TrimSpace(p))
			if len(a) != 2 {
				return fmt.Errorf("ValidationException: invalid ADD expression")
			}
			name := ctx.resolveName(a[0])
			inc, err := ctx.resolveValue(a[1])
			if err != nil {
				return err
			}
			updated, err := addAttributeValue(item[name], inc)
			if err != nil {
				return err
			}
			item[name] = updated
		case "DELETE":
			a := strings.Fields(strings.TrimSpace(p))
			if len(a) != 2 {
				return fmt.Errorf("ValidationException: invalid DELETE expression")
			}
			name := ctx.resolveName(a[0])
			v, err := ctx.resolveValue(a[1])
			if err != nil {
				return err
			}
			updated, err := deleteFromSet(item[name], v)
			if err != nil {
				return err
			}
			if updated == nil {
				delete(item, name)
			} else {
				item[name] = updated
			}
		}
	}
	return nil
}

func splitByKeyword(expr, keyword string) []string {
	return strings.Split(expr, " "+keyword+" ")
}

func splitCommaParts(clause string) []string {
	raw := strings.Split(clause, ",")
	out := make([]string, 0, len(raw))
	for _, part := range raw {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func attributeValueEqual(a, b types.AttributeValue) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		if !ok {
			return false
		}
		ar, ok := new(big.Rat).SetString(av.Value)
		if !ok {
			return false
		}
		br, ok := new(big.Rat).SetString(bv.Value)
		return ok && ar.Cmp(br) == 0
	case *types.AttributeValueMemberBOOL:
		bv, ok := b.(*types.AttributeValueMemberBOOL)
		return ok && av.Value == bv.Value
	default:
		return false
	}
}

func addAttributeValue(current, add types.AttributeValue) (types.AttributeValue, error) {
	inc, ok := add.(*types.AttributeValueMemberN)
	if !ok {
		return nil, fmt.Errorf("ValidationException: ADD currently supports Number values")
	}
	incRat, ok := new(big.Rat).SetString(inc.Value)
	if !ok {
		return nil, fmt.Errorf("ValidationException: invalid numeric value %q", inc.Value)
	}
	if current == nil {
		return &types.AttributeValueMemberN{Value: inc.Value}, nil
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
	currentSS, okCurrent := current.(*types.AttributeValueMemberSS)
	removeSS, okRemove := remove.(*types.AttributeValueMemberSS)
	if okCurrent && okRemove {
		remaining := make([]string, 0, len(currentSS.Value))
		toDelete := make(map[string]struct{}, len(removeSS.Value))
		for _, v := range removeSS.Value {
			toDelete[v] = struct{}{}
		}
		for _, v := range currentSS.Value {
			if _, ok := toDelete[v]; !ok {
				remaining = append(remaining, v)
			}
		}
		if len(remaining) == 0 {
			return nil, nil
		}
		return &types.AttributeValueMemberSS{Value: remaining}, nil
	}
	if current == nil {
		return nil, nil
	}
	return nil, fmt.Errorf("ValidationException: DELETE currently supports String Set values")
}
