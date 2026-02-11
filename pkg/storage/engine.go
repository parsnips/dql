package storage

import (
	"fmt"
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

type Engine interface {
	CreateTable(name string, schema TableSchema) error
	DeleteTable(name string) error
	PutItem(table string, item map[string]types.AttributeValue) (map[string]types.AttributeValue, error)
	GetItem(table string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error)
	DeleteItem(table string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error)
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
	m.mu.Lock()
	defer m.mu.Unlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return nil, err
	}
	k, err := makeKey(item, schema)
	if err != nil {
		return nil, err
	}
	old := cloneItem(t.items[k])
	t.items[k] = cloneItem(item)
	return old, nil
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
	m.mu.Lock()
	defer m.mu.Unlock()
	t, schema, err := m.getTableLocked(table)
	if err != nil {
		return nil, err
	}
	k, err := makeKey(key, schema)
	if err != nil {
		return nil, err
	}
	old := cloneItem(t.items[k])
	delete(t.items, k)
	return old, nil
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
