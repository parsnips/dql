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
