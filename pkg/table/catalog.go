package table

import (
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Definition struct {
	Name           string
	PartitionKey   string
	SortKey        string
	AttributeTypes map[string]types.ScalarAttributeType
	Status         types.TableStatus
	StreamSpec     *types.StreamSpecification
	GlobalIndexes  map[string]types.GlobalSecondaryIndexDescription
}

type Catalog struct {
	mu     sync.RWMutex
	tables map[string]Definition
}

func NewCatalog() *Catalog {
	return &Catalog{tables: map[string]Definition{}}
}

func (c *Catalog) Create(def Definition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[def.Name]; ok {
		return fmt.Errorf("ResourceInUseException: table already exists: %s", def.Name)
	}
	def.Status = types.TableStatusActive
	if def.GlobalIndexes == nil {
		def.GlobalIndexes = map[string]types.GlobalSecondaryIndexDescription{}
	}
	c.tables[def.Name] = def
	return nil
}

func (c *Catalog) Update(name string, mut func(*Definition) error) (Definition, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	def, ok := c.tables[name]
	if !ok {
		return Definition{}, fmt.Errorf("ResourceNotFoundException: table not found: %s", name)
	}
	if err := mut(&def); err != nil {
		return Definition{}, err
	}
	c.tables[name] = def
	return def, nil
}

func (c *Catalog) Get(name string) (Definition, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	def, ok := c.tables[name]
	return def, ok
}

func (c *Catalog) Delete(name string) (Definition, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	def, ok := c.tables[name]
	if !ok {
		return Definition{}, fmt.Errorf("ResourceNotFoundException: table not found: %s", name)
	}
	delete(c.tables, name)
	return def, nil
}

func (c *Catalog) List(limit int, start string) ([]string, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	sort.Strings(names)

	startIdx := 0
	if start != "" {
		for i, n := range names {
			if n == start {
				startIdx = i + 1
				break
			}
		}
	}
	if startIdx >= len(names) {
		return []string{}, ""
	}
	if limit <= 0 || startIdx+limit >= len(names) {
		return names[startIdx:], ""
	}
	return names[startIdx : startIdx+limit], names[startIdx+limit-1]
}
