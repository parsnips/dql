package storage

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Key struct {
	PK string
	SK string
}

type TableSchema struct {
	PartitionKey string
	SortKey      string
}

type ConditionCheck func(existing map[string]types.AttributeValue) (bool, error)

type Engine interface {
	CreateTable(name string, schema TableSchema) error
	DeleteTable(name string) error
	PutItem(table string, item map[string]types.AttributeValue) (map[string]types.AttributeValue, error)
	PutItemConditional(table string, item map[string]types.AttributeValue, condition ConditionCheck) (map[string]types.AttributeValue, bool, error)
	GetItem(table string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error)
	DeleteItem(table string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error)
	DeleteItemConditional(table string, key map[string]types.AttributeValue, condition ConditionCheck) (map[string]types.AttributeValue, bool, error)
	UpdateItemConditional(table string, key map[string]types.AttributeValue, condition ConditionCheck, update func(item map[string]types.AttributeValue) error) (map[string]types.AttributeValue, map[string]types.AttributeValue, bool, error)
	Query(table string, in QueryInput) (QueryOutput, error)
	Scan(table string, in ScanInput) (ScanOutput, error)
}

type QueryInput struct {
	KeyConditionExpression    string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	ExclusiveStartKey         map[string]types.AttributeValue
	Limit                     int32
	ScanIndexForward          bool
	Select                    types.Select
}

type QueryOutput struct {
	Items            []map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
	LastEvaluatedKey map[string]types.AttributeValue
}

type ScanInput struct {
	ExclusiveStartKey map[string]types.AttributeValue
	Limit             int32
	Select            types.Select
}

type ScanOutput struct {
	Items            []map[string]types.AttributeValue
	Count            int32
	ScannedCount     int32
	LastEvaluatedKey map[string]types.AttributeValue
}

type MemoryEngine struct {
	mu      sync.RWMutex
	tables  map[string]*memTable
	schemas map[string]TableSchema
}

type memTable struct {
	items map[Key]map[string]types.AttributeValue
}

func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{tables: map[string]*memTable{}, schemas: map[string]TableSchema{}}
}

func (m *MemoryEngine) CreateTable(name string, schema TableSchema) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tables[name]; ok {
		return fmt.Errorf("ResourceInUseException: table already exists: %s", name)
	}
	m.tables[name] = &memTable{items: map[Key]map[string]types.AttributeValue{}}
	m.schemas[name] = schema
	return nil
}

func (m *MemoryEngine) DeleteTable(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tables[name]; !ok {
		return fmt.Errorf("ResourceNotFoundException: table not found: %s", name)
	}
	delete(m.tables, name)
	delete(m.schemas, name)
	return nil
}

func (m *MemoryEngine) PutItem(table string, item map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	old, _, err := m.PutItemConditional(table, item, nil)
	return old, err
}

func (m *MemoryEngine) PutItemConditional(table string, item map[string]types.AttributeValue, condition ConditionCheck) (map[string]types.AttributeValue, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return nil, false, err
	}
	k, err := makeKey(item, schema)
	if err != nil {
		return nil, false, err
	}
	old := cloneItem(t.items[k])
	if condition != nil {
		ok, err := condition(cloneItem(old))
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return old, false, nil
		}
	}
	t.items[k] = cloneItem(item)
	return old, true, nil
}

func (m *MemoryEngine) GetItem(table string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return nil, err
	}
	k, err := makeKey(key, schema)
	if err != nil {
		return nil, err
	}
	return cloneItem(t.items[k]), nil
}

func (m *MemoryEngine) DeleteItem(table string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	old, _, err := m.DeleteItemConditional(table, key, nil)
	return old, err
}

func (m *MemoryEngine) DeleteItemConditional(table string, key map[string]types.AttributeValue, condition ConditionCheck) (map[string]types.AttributeValue, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return nil, false, err
	}
	k, err := makeKey(key, schema)
	if err != nil {
		return nil, false, err
	}
	old := cloneItem(t.items[k])
	if condition != nil {
		ok, err := condition(cloneItem(old))
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return old, false, nil
		}
	}
	delete(t.items, k)
	return old, true, nil
}

func (m *MemoryEngine) UpdateItemConditional(table string, key map[string]types.AttributeValue, condition ConditionCheck, update func(item map[string]types.AttributeValue) error) (map[string]types.AttributeValue, map[string]types.AttributeValue, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return nil, nil, false, err
	}
	k, err := makeKey(key, schema)
	if err != nil {
		return nil, nil, false, err
	}

	base := cloneItem(t.items[k])
	if base == nil {
		base = cloneItem(key)
	}
	old := cloneItem(base)

	if condition != nil {
		ok, err := condition(cloneItem(old))
		if err != nil {
			return nil, nil, false, err
		}
		if !ok {
			return old, nil, false, nil
		}
	}

	working := cloneItem(base)
	if update != nil {
		if err := update(working); err != nil {
			return nil, nil, false, err
		}
	}
	newKey, err := makeKey(working, schema)
	if err != nil {
		return nil, nil, false, err
	}
	t.items[newKey] = cloneItem(working)
	return old, cloneItem(working), true, nil
}

func (m *MemoryEngine) Query(table string, in QueryInput) (QueryOutput, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return QueryOutput{}, err
	}
	cond, err := parseKeyCondition(in.KeyConditionExpression, schema, in.ExpressionAttributeNames, in.ExpressionAttributeValues)
	if err != nil {
		return QueryOutput{}, err
	}

	type row struct {
		key  Key
		item map[string]types.AttributeValue
	}
	rows := make([]row, 0, len(t.items))
	for k, item := range t.items {
		if !attributeEqual(item[schema.PartitionKey], cond.pkValue) {
			continue
		}
		if schema.SortKey != "" {
			if ok, err := cond.matchesSort(item[schema.SortKey]); err != nil || !ok {
				if err != nil {
					return QueryOutput{}, err
				}
				continue
			}
		}
		rows = append(rows, row{key: k, item: cloneItem(item)})
	}
	sort.Slice(rows, func(i, j int) bool {
		cmp := strings.Compare(rows[i].key.SK, rows[j].key.SK)
		if cmp == 0 {
			cmp = strings.Compare(rows[i].key.PK, rows[j].key.PK)
		}
		if !in.ScanIndexForward {
			return cmp > 0
		}
		return cmp < 0
	})

	start := 0
	if len(in.ExclusiveStartKey) > 0 {
		exclusive, err := makeKey(in.ExclusiveStartKey, schema)
		if err != nil {
			return QueryOutput{}, err
		}
		for i, r := range rows {
			if r.key == exclusive {
				start = i + 1
				break
			}
		}
	}

	limited := rows[start:]
	var last map[string]types.AttributeValue
	if in.Limit > 0 && int(in.Limit) < len(limited) {
		limited = limited[:in.Limit]
		last = keyFromItem(limited[len(limited)-1].item, schema)
	}

	out := QueryOutput{Count: int32(len(limited)), ScannedCount: int32(len(rows)), LastEvaluatedKey: last}
	if in.Select != types.SelectCount {
		out.Items = make([]map[string]types.AttributeValue, 0, len(limited))
		for _, r := range limited {
			out.Items = append(out.Items, r.item)
		}
	}
	return out, nil
}

func (m *MemoryEngine) Scan(table string, in ScanInput) (ScanOutput, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return ScanOutput{}, err
	}
	type row struct {
		key  Key
		item map[string]types.AttributeValue
	}
	rows := make([]row, 0, len(t.items))
	for k, item := range t.items {
		rows = append(rows, row{key: k, item: cloneItem(item)})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].key.PK == rows[j].key.PK {
			return rows[i].key.SK < rows[j].key.SK
		}
		return rows[i].key.PK < rows[j].key.PK
	})

	start := 0
	if len(in.ExclusiveStartKey) > 0 {
		exclusive, err := makeKey(in.ExclusiveStartKey, schema)
		if err != nil {
			return ScanOutput{}, err
		}
		for i, r := range rows {
			if r.key == exclusive {
				start = i + 1
				break
			}
		}
	}

	limited := rows[start:]
	var last map[string]types.AttributeValue
	if in.Limit > 0 && int(in.Limit) < len(limited) {
		limited = limited[:in.Limit]
		last = keyFromItem(limited[len(limited)-1].item, schema)
	}

	out := ScanOutput{Count: int32(len(limited)), ScannedCount: int32(len(rows)), LastEvaluatedKey: last}
	if in.Select != types.SelectCount {
		out.Items = make([]map[string]types.AttributeValue, 0, len(limited))
		for _, r := range limited {
			out.Items = append(out.Items, r.item)
		}
	}
	return out, nil
}

func (m *MemoryEngine) getTableLocked(name string) (*memTable, TableSchema, error) {
	t, ok := m.tables[name]
	if !ok {
		return nil, TableSchema{}, fmt.Errorf("ResourceNotFoundException: table not found: %s", name)
	}
	return t, m.schemas[name], nil
}

func makeKey(item map[string]types.AttributeValue, schema TableSchema) (Key, error) {
	pk, ok := item[schema.PartitionKey]
	if !ok {
		return Key{}, fmt.Errorf("ValidationException: missing partition key %s", schema.PartitionKey)
	}
	pkS, err := scalarKey(pk)
	if err != nil {
		return Key{}, err
	}
	k := Key{PK: pkS}
	if schema.SortKey != "" {
		sk, ok := item[schema.SortKey]
		if !ok {
			return Key{}, fmt.Errorf("ValidationException: missing sort key %s", schema.SortKey)
		}
		skS, err := scalarKey(sk)
		if err != nil {
			return Key{}, err
		}
		k.SK = skS
	}
	return k, nil
}

func scalarKey(av types.AttributeValue) (string, error) {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return "S:" + v.Value, nil
	case *types.AttributeValueMemberN:
		return "N:" + v.Value, nil
	case *types.AttributeValueMemberB:
		return "B:" + string(v.Value), nil
	default:
		return "", fmt.Errorf("ValidationException: key must be scalar")
	}
}

func cloneItem(in map[string]types.AttributeValue) map[string]types.AttributeValue {
	if in == nil {
		return nil
	}
	out := make(map[string]types.AttributeValue, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type keyCondition struct {
	pkValue types.AttributeValue
	skOp    string
	skOne   types.AttributeValue
	skTwo   types.AttributeValue
}

func parseKeyCondition(expr string, schema TableSchema, names map[string]string, values map[string]types.AttributeValue) (keyCondition, error) {
	if expr == "" {
		return keyCondition{}, fmt.Errorf("ValidationException: KeyConditionExpression is required")
	}
	resolved := expr
	for k, v := range names {
		resolved = strings.ReplaceAll(resolved, k, v)
	}
	parts := strings.SplitN(resolved, "AND", 2)
	pkPart := strings.TrimSpace(parts[0])
	toks := strings.Fields(pkPart)
	if len(toks) != 3 || toks[0] != schema.PartitionKey || toks[1] != "=" {
		return keyCondition{}, fmt.Errorf("ValidationException: partition key equality is required")
	}
	pk, ok := values[toks[2]]
	if !ok {
		return keyCondition{}, fmt.Errorf("ValidationException: missing expression value %s", toks[2])
	}
	out := keyCondition{pkValue: pk}
	if len(parts) == 1 {
		return out, nil
	}
	skPart := strings.TrimSpace(parts[1])
	if strings.HasPrefix(skPart, "begins_with(") && strings.HasSuffix(skPart, ")") {
		inner := strings.TrimSuffix(strings.TrimPrefix(skPart, "begins_with("), ")")
		args := strings.Split(inner, ",")
		if len(args) != 2 {
			return keyCondition{}, fmt.Errorf("ValidationException: invalid begins_with expression")
		}
		if strings.TrimSpace(args[0]) != schema.SortKey {
			return keyCondition{}, fmt.Errorf("ValidationException: invalid sort key in begins_with")
		}
		v := strings.TrimSpace(args[1])
		out.skOp = "begins_with"
		out.skOne = values[v]
		if out.skOne == nil {
			return keyCondition{}, fmt.Errorf("ValidationException: missing expression value %s", v)
		}
		return out, nil
	}
	toks = strings.Fields(skPart)
	if len(toks) == 5 && toks[1] == "BETWEEN" && toks[3] == "AND" {
		if toks[0] != schema.SortKey {
			return keyCondition{}, fmt.Errorf("ValidationException: invalid sort key in BETWEEN")
		}
		out.skOp = "BETWEEN"
		out.skOne = values[toks[2]]
		out.skTwo = values[toks[4]]
		if out.skOne == nil || out.skTwo == nil {
			return keyCondition{}, fmt.Errorf("ValidationException: missing expression value")
		}
		return out, nil
	}
	if len(toks) != 3 || toks[0] != schema.SortKey {
		return keyCondition{}, fmt.Errorf("ValidationException: invalid sort key expression")
	}
	if toks[1] != "=" && toks[1] != "<" && toks[1] != "<=" && toks[1] != ">" && toks[1] != ">=" {
		return keyCondition{}, fmt.Errorf("ValidationException: unsupported operator %s", toks[1])
	}
	out.skOp = toks[1]
	out.skOne = values[toks[2]]
	if out.skOne == nil {
		return keyCondition{}, fmt.Errorf("ValidationException: missing expression value %s", toks[2])
	}
	return out, nil
}

func (k keyCondition) matchesSort(av types.AttributeValue) (bool, error) {
	if k.skOp == "" {
		return true, nil
	}
	cmp, err := compareAttributeValues(av, k.skOne)
	if err != nil {
		return false, err
	}
	switch k.skOp {
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
	case "BETWEEN":
		cmp2, err := compareAttributeValues(av, k.skTwo)
		if err != nil {
			return false, err
		}
		return cmp >= 0 && cmp2 <= 0, nil
	case "begins_with":
		a, okA := av.(*types.AttributeValueMemberS)
		b, okB := k.skOne.(*types.AttributeValueMemberS)
		if !okA || !okB {
			return false, fmt.Errorf("ValidationException: begins_with supports String sort keys only")
		}
		return strings.HasPrefix(a.Value, b.Value), nil
	default:
		return false, fmt.Errorf("ValidationException: unsupported sort key operator")
	}
}

func attributeEqual(a, b types.AttributeValue) bool {
	cmp, err := compareAttributeValues(a, b)
	return err == nil && cmp == 0
}

func compareAttributeValues(a, b types.AttributeValue) (int, error) {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		if !ok {
			return 0, fmt.Errorf("ValidationException: incompatible key types")
		}
		return strings.Compare(av.Value, bv.Value), nil
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		if !ok {
			return 0, fmt.Errorf("ValidationException: incompatible key types")
		}
		aNum, ok := new(big.Rat).SetString(av.Value)
		if !ok {
			return 0, fmt.Errorf("ValidationException: invalid numeric key %q", av.Value)
		}
		bNum, ok := new(big.Rat).SetString(bv.Value)
		if !ok {
			return 0, fmt.Errorf("ValidationException: invalid numeric key %q", bv.Value)
		}
		return aNum.Cmp(bNum), nil
	case *types.AttributeValueMemberB:
		bv, ok := b.(*types.AttributeValueMemberB)
		if !ok {
			return 0, fmt.Errorf("ValidationException: incompatible key types")
		}
		return bytes.Compare(av.Value, bv.Value), nil
	default:
		return 0, fmt.Errorf("ValidationException: key must be scalar")
	}
}

func keyFromItem(item map[string]types.AttributeValue, schema TableSchema) map[string]types.AttributeValue {
	out := map[string]types.AttributeValue{schema.PartitionKey: item[schema.PartitionKey]}
	if schema.SortKey != "" {
		out[schema.SortKey] = item[schema.SortKey]
	}
	return out
}
