package integration

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/twisp/dql/internal/testutil"
)

func TestUnknownOperationValidationShape(t *testing.T) {
	h := testutil.NewHarness(t)
	req, err := http.NewRequest(http.MethodPost, h.Server.URL, bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Nope")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	resp, err := h.Server.Client().Do(req)
	if err != nil {
		t.Fatalf("raw call failed: %v", err)
	}
	defer resp.Body.Close()
	if got, want := resp.StatusCode, 400; got != want {
		t.Fatalf("status got=%d want=%d", got, want)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body failed: %v", err)
	}
	if got, want := resp.Header.Get("X-Amz-Crc32"), strconv.FormatUint(uint64(crc32.ChecksumIEEE(body)), 10); got != want {
		t.Fatalf("x-amz-crc32 got=%q want=%q", got, want)
	}
}

func TestTableLifecycleAndCoreCRUD(t *testing.T) {
	ctx := context.Background()
	h := testutil.NewHarness(t)
	client := h.Client

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("phase1_items"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	listOut, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(10)})
	if err != nil {
		t.Fatalf("list tables failed: %v", err)
	}
	if len(listOut.TableNames) != 1 || listOut.TableNames[0] != "phase1_items" {
		t.Fatalf("unexpected table names: %#v", listOut.TableNames)
	}

	descOut, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("phase1_items")})
	if err != nil {
		t.Fatalf("describe failed: %v", err)
	}
	if descOut.Table == nil || aws.ToString(descOut.Table.TableName) != "phase1_items" {
		t.Fatalf("unexpected describe output: %#v", descOut.Table)
	}

	updateTableOut, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("phase1_items"),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
	})
	if err != nil {
		t.Fatalf("update table failed: %v", err)
	}
	if updateTableOut.TableDescription == nil || aws.ToString(updateTableOut.TableDescription.TableName) != "phase1_items" {
		t.Fatalf("unexpected update table output: %#v", updateTableOut.TableDescription)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("phase1_items"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk":   &types.AttributeValueMemberS{Value: "user#1"},
			"name": &types.AttributeValueMemberS{Value: "alice"},
			"age":  &types.AttributeValueMemberN{Value: "41"},
		},
	})
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("phase1_items"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
	})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if _, ok := getOut.Item["name"]; !ok {
		t.Fatalf("missing name attribute: %#v", getOut.Item)
	}

	upd, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("phase1_items"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		AttributeUpdates: map[string]types.AttributeValueUpdate{
			"name": {Action: types.AttributeActionPut, Value: &types.AttributeValueMemberS{Value: "alice-updated"}},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if v, ok := upd.Attributes["name"].(*types.AttributeValueMemberS); !ok || v.Value != "alice-updated" {
		t.Fatalf("unexpected update attrs: %#v", upd.Attributes)
	}

	del, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String("phase1_items"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, ok := del.Attributes["name"]; !ok {
		t.Fatalf("delete should return ALL_OLD attrs")
	}

	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("phase1_items")})
	if err != nil {
		t.Fatalf("delete table failed: %v", err)
	}

	_, err = client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("phase1_items")})
	var nfe *types.ResourceNotFoundException
	if !errors.As(err, &nfe) {
		t.Fatalf("expected resource not found, got: %v", err)
	}
}

func TestQueryAndScanRoundTrip(t *testing.T) {
	ctx := context.Background()
	h := testutil.NewHarness(t)
	client := h.Client

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("phase1_qs"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	items := []map[string]types.AttributeValue{
		{"pk": &types.AttributeValueMemberS{Value: "tenant#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}, "name": &types.AttributeValueMemberS{Value: "a"}},
		{"pk": &types.AttributeValueMemberS{Value: "tenant#1"}, "sk": &types.AttributeValueMemberN{Value: "2"}, "name": &types.AttributeValueMemberS{Value: "b"}},
		{"pk": &types.AttributeValueMemberS{Value: "tenant#1"}, "sk": &types.AttributeValueMemberN{Value: "3"}, "name": &types.AttributeValueMemberS{Value: "c"}},
		{"pk": &types.AttributeValueMemberS{Value: "tenant#2"}, "sk": &types.AttributeValueMemberN{Value: "1"}, "name": &types.AttributeValueMemberS{Value: "d"}},
	}
	for _, item := range items {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("phase1_qs"), Item: item})
		if err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	queryOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("phase1_qs"),
		KeyConditionExpression: aws.String("#pk = :pk AND #sk >= :min"),
		ExpressionAttributeNames: map[string]string{
			"#pk": "pk",
			"#sk": "sk",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":  &types.AttributeValueMemberS{Value: "tenant#1"},
			":min": &types.AttributeValueMemberN{Value: "2"},
		},
		ScanIndexForward: aws.Bool(false),
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if got, want := len(queryOut.Items), 2; got != want {
		t.Fatalf("query items got=%d want=%d", got, want)
	}
	if got := queryOut.Items[0]["sk"].(*types.AttributeValueMemberN).Value; got != "3" {
		t.Fatalf("expected descending sort, first sk=%s", got)
	}

	paged, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("phase1_qs"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant#1"},
		},
		Limit: aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("paged query failed: %v", err)
	}
	if got, want := len(paged.Items), 2; got != want {
		t.Fatalf("paged query items got=%d want=%d", got, want)
	}
	if len(paged.LastEvaluatedKey) == 0 {
		t.Fatalf("expected LastEvaluatedKey for paged query")
	}

	next, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("phase1_qs"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant#1"},
		},
		ExclusiveStartKey: paged.LastEvaluatedKey,
	})
	if err != nil {
		t.Fatalf("next page query failed: %v", err)
	}
	if got, want := len(next.Items), 1; got != want {
		t.Fatalf("next page items got=%d want=%d", got, want)
	}

	scanOut, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("phase1_qs")})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if got, want := len(scanOut.Items), 4; got != want {
		t.Fatalf("scan items got=%d want=%d", got, want)
	}

	scanCount, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("phase1_qs"), Select: types.SelectCount})
	if err != nil {
		t.Fatalf("scan count failed: %v", err)
	}
	if got, want := scanCount.Count, int32(4); got != want {
		t.Fatalf("scan count got=%d want=%d", got, want)
	}
	if len(scanCount.Items) != 0 {
		t.Fatalf("expected no items for Select=COUNT")
	}
}

func TestConditionExpressionAndUpdateExpression(t *testing.T) {
	ctx := context.Background()
	h := testutil.NewHarness(t)
	client := h.Client

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("phase1_expr"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("phase1_expr"),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "t#1"},
			"sk":      &types.AttributeValueMemberS{Value: "u#1"},
			"counter": &types.AttributeValueMemberN{Value: "1"},
			"tags":    &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
		},
	})
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String("phase1_expr"),
		ConditionExpression: aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": "pk",
		},
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "t#1"},
			"sk": &types.AttributeValueMemberS{Value: "u#1"},
		},
	})
	var cfe *types.ConditionalCheckFailedException
	if !errors.As(err, &cfe) {
		t.Fatalf("expected conditional failure, got: %v", err)
	}

	upd, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String("phase1_expr"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "t#1"},
			"sk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		ConditionExpression: aws.String("#counter = :current"),
		UpdateExpression:    aws.String("SET #name = :name REMOVE #old ADD #counter :inc DELETE #tags :deleteTags"),
		ExpressionAttributeNames: map[string]string{
			"#counter": "counter",
			"#name":    "name",
			"#old":     "old_attr",
			"#tags":    "tags",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":current":    &types.AttributeValueMemberN{Value: "1"},
			":name":       &types.AttributeValueMemberS{Value: "alice"},
			":inc":        &types.AttributeValueMemberN{Value: "2"},
			":deleteTags": &types.AttributeValueMemberSS{Value: []string{"a", "x"}},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("update expression failed: %v", err)
	}
	if got := upd.Attributes["counter"].(*types.AttributeValueMemberN).Value; got != "3" {
		t.Fatalf("counter got=%s want=3", got)
	}
	if _, ok := upd.Attributes["old_attr"]; ok {
		t.Fatalf("old_attr should be removed")
	}
	if got := upd.Attributes["name"].(*types.AttributeValueMemberS).Value; got != "alice" {
		t.Fatalf("name got=%s want=alice", got)
	}
	if got := upd.Attributes["tags"].(*types.AttributeValueMemberSS).Value; len(got) != 2 {
		t.Fatalf("tags should contain 2 items after delete, got=%v", got)
	}

	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String("phase1_expr"),
		ConditionExpression: aws.String("attribute_exists(#missing)"),
		ExpressionAttributeNames: map[string]string{
			"#missing": "does_not_exist",
		},
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "t#1"},
			"sk": &types.AttributeValueMemberS{Value: "u#1"},
		},
	})
	if !errors.As(err, &cfe) {
		t.Fatalf("expected conditional failure on delete, got: %v", err)
	}
}
