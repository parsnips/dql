package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/twisp/dql/pkg/storage"
	"github.com/twisp/dql/pkg/table"
)

type Server struct {
	catalog *table.Catalog
	engine  storage.Engine
}

type checksumResponseWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newChecksumResponseWriter() *checksumResponseWriter {
	return &checksumResponseWriter{header: make(http.Header)}
}

func (w *checksumResponseWriter) Header() http.Header {
	return w.header
}

func (w *checksumResponseWriter) Write(p []byte) (int, error) {
	return w.body.Write(p)
}

func (w *checksumResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *checksumResponseWriter) flushTo(dst http.ResponseWriter) {
	for key, values := range w.header {
		for _, value := range values {
			dst.Header().Add(key, value)
		}
	}
	dst.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE(w.body.Bytes())), 10))
	if w.status == 0 {
		w.status = http.StatusOK
	}
	dst.WriteHeader(w.status)
	if w.body.Len() > 0 {
		_, _ = dst.Write(w.body.Bytes())
	}
}

func NewServer(catalog *table.Catalog, engine storage.Engine) *Server {
	return &Server{catalog: catalog, engine: engine}
}
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.handle) }

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	rw := newChecksumResponseWriter()
	defer rw.flushTo(w)

	if r.Body != nil {
		defer r.Body.Close()
	}
	if r.Method != http.MethodPost {
		writeError(rw, http.StatusMethodNotAllowed, "ValidationException", "unsupported method")
		return
	}
	target := r.Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	if len(parts) != 2 || parts[0] != "DynamoDB_20120810" {
		writeError(rw, http.StatusBadRequest, "ValidationException", "Invalid X-Amz-Target")
		return
	}
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(rw, http.StatusBadRequest, "ValidationException", "unable to read request")
		return
	}
	rw.Header().Set("Content-Type", "application/x-amz-json-1.0")
	switch parts[1] {
	case "CreateTable":
		s.createTable(rw, payload)
	case "DescribeTable":
		s.describeTable(rw, payload)
	case "ListTables":
		s.listTables(rw, payload)
	case "DeleteTable":
		s.deleteTable(rw, payload)
	case "UpdateTable":
		s.updateTable(rw, payload)
	case "PutItem":
		s.putItem(rw, payload)
	case "GetItem":
		s.getItem(rw, payload)
	case "DeleteItem":
		s.deleteItem(rw, payload)
	case "UpdateItem":
		s.updateItem(rw, payload)
	case "Query":
		s.query(rw, payload)
	case "Scan":
		s.scan(rw, payload)
	default:
		writeError(rw, http.StatusBadRequest, "ValidationException", fmt.Sprintf("Unknown operation %s", parts[1]))
	}
}

func (s *Server) createTable(w http.ResponseWriter, payload []byte) {
	var in struct {
		AttributeDefinitions []types.AttributeDefinition `json:"AttributeDefinitions"`
		TableName            string                      `json:"TableName"`
		KeySchema            []types.KeySchemaElement    `json:"KeySchema"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	if in.TableName == "" || len(in.KeySchema) == 0 {
		writeError(w, 400, "ValidationException", "missing table definition")
		return
	}
	schema := storage.TableSchema{}
	def := table.Definition{Name: in.TableName, AttributeTypes: map[string]types.ScalarAttributeType{}}
	for _, ad := range in.AttributeDefinitions {
		def.AttributeTypes[*ad.AttributeName] = ad.AttributeType
	}
	for _, ks := range in.KeySchema {
		if ks.KeyType == types.KeyTypeHash {
			schema.PartitionKey, def.PartitionKey = *ks.AttributeName, *ks.AttributeName
		}
		if ks.KeyType == types.KeyTypeRange {
			schema.SortKey, def.SortKey = *ks.AttributeName, *ks.AttributeName
		}
	}
	if schema.PartitionKey == "" {
		writeError(w, 400, "ValidationException", "partition key required")
		return
	}
	if err := s.catalog.Create(def); err != nil {
		writeTyped(w, err)
		return
	}
	if err := s.engine.CreateTable(in.TableName, schema); err != nil {
		writeTyped(w, err)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"TableDescription": map[string]any{"TableName": in.TableName, "TableStatus": types.TableStatusActive}})
}

func (s *Server) describeTable(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	def, ok := s.catalog.Get(in.TableName)
	if !ok {
		writeError(w, 400, "ResourceNotFoundException", "Cannot do operations on a non-existent table")
		return
	}
	keySchema := []map[string]any{{"AttributeName": def.PartitionKey, "KeyType": types.KeyTypeHash}}
	if def.SortKey != "" {
		keySchema = append(keySchema, map[string]any{"AttributeName": def.SortKey, "KeyType": types.KeyTypeRange})
	}
	attrDefs := make([]map[string]any, 0, len(def.AttributeTypes))
	for k, v := range def.AttributeTypes {
		attrDefs = append(attrDefs, map[string]any{"AttributeName": k, "AttributeType": v})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"Table": map[string]any{"TableName": def.Name, "TableStatus": def.Status, "KeySchema": keySchema, "AttributeDefinitions": attrDefs}})
}

func (s *Server) listTables(w http.ResponseWriter, payload []byte) {
	var in struct {
		Limit             int32  `json:"Limit"`
		ExclusiveStartTbl string `json:"ExclusiveStartTableName"`
	}
	_ = json.Unmarshal(payload, &in)
	names, last := s.catalog.List(int(in.Limit), in.ExclusiveStartTbl)
	resp := map[string]any{"TableNames": names}
	if last != "" {
		resp["LastEvaluatedTableName"] = last
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) deleteTable(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	def, err := s.catalog.Delete(in.TableName)
	if err != nil {
		writeTyped(w, err)
		return
	}
	_ = s.engine.DeleteTable(in.TableName)
	_ = json.NewEncoder(w).Encode(map[string]any{"TableDescription": map[string]any{"TableName": def.Name, "TableStatus": types.TableStatusActive}})
}

func (s *Server) updateTable(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	if in.TableName == "" {
		writeError(w, 400, "ValidationException", "TableName is required")
		return
	}
	def, ok := s.catalog.Get(in.TableName)
	if !ok {
		writeError(w, 400, "ResourceNotFoundException", "Cannot do operations on a non-existent table")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"TableDescription": map[string]any{"TableName": def.Name, "TableStatus": def.Status}})
}

func (s *Server) putItem(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName    string            `json:"TableName"`
		Item         json.RawMessage   `json:"Item"`
		ReturnValues types.ReturnValue `json:"ReturnValues"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	item, err := attributevalue.UnmarshalMapJSON(in.Item)
	if err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	old, err := s.engine.PutItem(in.TableName, item)
	if err != nil {
		writeTyped(w, err)
		return
	}
	if in.ReturnValues == types.ReturnValueAllOld && old != nil {
		writeItemEnvelope(w, "Attributes", old)
		return
	}
	_, _ = w.Write([]byte("{}"))
}

func (s *Server) getItem(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName string          `json:"TableName"`
		Key       json.RawMessage `json:"Key"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	key, err := attributevalue.UnmarshalMapJSON(in.Key)
	if err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	item, err := s.engine.GetItem(in.TableName, key)
	if err != nil {
		writeTyped(w, err)
		return
	}
	if item == nil {
		_, _ = w.Write([]byte("{}"))
		return
	}
	writeItemEnvelope(w, "Item", item)
}

func (s *Server) deleteItem(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName    string            `json:"TableName"`
		Key          json.RawMessage   `json:"Key"`
		ReturnValues types.ReturnValue `json:"ReturnValues"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	key, err := attributevalue.UnmarshalMapJSON(in.Key)
	if err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	old, err := s.engine.DeleteItem(in.TableName, key)
	if err != nil {
		writeTyped(w, err)
		return
	}
	if in.ReturnValues == types.ReturnValueAllOld && old != nil {
		writeItemEnvelope(w, "Attributes", old)
		return
	}
	_, _ = w.Write([]byte("{}"))
}

func (s *Server) updateItem(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName        string                     `json:"TableName"`
		Key              json.RawMessage            `json:"Key"`
		AttributeUpdates map[string]json.RawMessage `json:"AttributeUpdates"`
		ReturnValues     types.ReturnValue          `json:"ReturnValues"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	key, err := attributevalue.UnmarshalMapJSON(in.Key)
	if err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	item, err := s.engine.GetItem(in.TableName, key)
	if err != nil {
		writeTyped(w, err)
		return
	}
	if item == nil {
		item = key
	}
	old := cloneMap(item)
	for name, raw := range in.AttributeUpdates {
		var upd struct {
			Action types.AttributeAction `json:"Action"`
			Value  json.RawMessage       `json:"Value"`
		}
		if err := json.Unmarshal(raw, &upd); err != nil {
			writeError(w, 400, "ValidationException", err.Error())
			return
		}
		if upd.Action == types.AttributeActionDelete {
			delete(item, name)
			continue
		}
		if len(upd.Value) > 0 {
			m, err := attributevalue.UnmarshalMapJSON([]byte(`{"v":` + string(upd.Value) + `}`))
			if err != nil {
				writeError(w, 400, "ValidationException", err.Error())
				return
			}
			item[name] = m["v"]
		}
	}
	if _, err := s.engine.PutItem(in.TableName, item); err != nil {
		writeTyped(w, err)
		return
	}
	if in.ReturnValues == types.ReturnValueAllOld {
		writeItemEnvelope(w, "Attributes", old)
		return
	}
	if in.ReturnValues == types.ReturnValueAllNew {
		writeItemEnvelope(w, "Attributes", item)
		return
	}
	_, _ = w.Write([]byte("{}"))
}

func (s *Server) query(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName                 string            `json:"TableName"`
		KeyConditionExpression    string            `json:"KeyConditionExpression"`
		ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues json.RawMessage   `json:"ExpressionAttributeValues"`
		ExclusiveStartKey         json.RawMessage   `json:"ExclusiveStartKey"`
		Limit                     int32             `json:"Limit"`
		ScanIndexForward          *bool             `json:"ScanIndexForward"`
		Select                    types.Select      `json:"Select"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	values, err := unmarshalOptionalMap(in.ExpressionAttributeValues)
	if err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	startKey, err := unmarshalOptionalMap(in.ExclusiveStartKey)
	if err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	scanForward := true
	if in.ScanIndexForward != nil {
		scanForward = *in.ScanIndexForward
	}
	out, err := s.engine.Query(in.TableName, storage.QueryInput{
		KeyConditionExpression:    in.KeyConditionExpression,
		ExpressionAttributeNames:  in.ExpressionAttributeNames,
		ExpressionAttributeValues: values,
		ExclusiveStartKey:         startKey,
		Limit:                     in.Limit,
		ScanIndexForward:          scanForward,
		Select:                    in.Select,
	})
	if err != nil {
		writeTyped(w, err)
		return
	}
	writeCollectionEnvelope(w, out.Items, out.Count, out.ScannedCount, out.LastEvaluatedKey)
}

func (s *Server) scan(w http.ResponseWriter, payload []byte) {
	var in struct {
		TableName         string          `json:"TableName"`
		ExclusiveStartKey json.RawMessage `json:"ExclusiveStartKey"`
		Limit             int32           `json:"Limit"`
		Select            types.Select    `json:"Select"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	startKey, err := unmarshalOptionalMap(in.ExclusiveStartKey)
	if err != nil {
		writeError(w, 400, "ValidationException", err.Error())
		return
	}
	out, err := s.engine.Scan(in.TableName, storage.ScanInput{
		ExclusiveStartKey: startKey,
		Limit:             in.Limit,
		Select:            in.Select,
	})
	if err != nil {
		writeTyped(w, err)
		return
	}
	writeCollectionEnvelope(w, out.Items, out.Count, out.ScannedCount, out.LastEvaluatedKey)
}

func writeItemEnvelope(w http.ResponseWriter, key string, item map[string]types.AttributeValue) {
	b, err := attributevalue.MarshalMapJSON(item)
	if err != nil {
		writeError(w, 500, "InternalServerError", err.Error())
		return
	}
	_, _ = w.Write([]byte("{\"" + key + "\":" + string(b) + "}"))
}

func cloneMap(in map[string]types.AttributeValue) map[string]types.AttributeValue {
	out := make(map[string]types.AttributeValue, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func unmarshalOptionalMap(raw json.RawMessage) (map[string]types.AttributeValue, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	return attributevalue.UnmarshalMapJSON(raw)
}

func writeCollectionEnvelope(w http.ResponseWriter, items []map[string]types.AttributeValue, count, scannedCount int32, last map[string]types.AttributeValue) {
	resp := map[string]any{"Count": count, "ScannedCount": scannedCount}
	if len(items) > 0 {
		respItems := make([]json.RawMessage, 0, len(items))
		for _, item := range items {
			b, err := attributevalue.MarshalMapJSON(item)
			if err != nil {
				writeError(w, 500, "InternalServerError", err.Error())
				return
			}
			respItems = append(respItems, b)
		}
		resp["Items"] = respItems
	}
	if len(last) > 0 {
		b, err := attributevalue.MarshalMapJSON(last)
		if err != nil {
			writeError(w, 500, "InternalServerError", err.Error())
			return
		}
		resp["LastEvaluatedKey"] = json.RawMessage(b)
	}
	if _, ok := resp["Items"]; !ok {
		resp["Items"] = []json.RawMessage{}
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func writeTyped(w http.ResponseWriter, err error) {
	msg := err.Error()
	parts := strings.SplitN(msg, ":", 2)
	if len(parts) == 2 && strings.HasSuffix(parts[0], "Exception") {
		writeError(w, 400, strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		return
	}
	writeError(w, 500, "InternalServerError", msg)
}
