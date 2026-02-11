package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/twisp/dql/internal/testutil"
)

type scenarioResult struct {
	TableNames []string
	GotItem    map[string]types.AttributeValue
	Updated    map[string]types.AttributeValue
	Deleted    map[string]types.AttributeValue
}

func TestDifferentialDynamoDBLocalAndDQLCoreCRUD(t *testing.T) {
	ctx := context.Background()
	dql := testutil.NewHarness(t)
	ddbLocal := testutil.NewDynamoDBLocalHarness(t)

	dqlResult := runCoreCRUDScenario(t, ctx, dql.Client, "phase1_diff_dql")
	ddbResult := runCoreCRUDScenario(t, ctx, ddbLocal.Client, "phase1_diff_local")

	if got, want := dqlResult.TableNames, ddbResult.TableNames; len(got) != len(want) {
		t.Fatalf("table list cardinality mismatch dql=%v dynamodb-local=%v", got, want)
	}

	assertAttributeMapEqual(t, dqlResult.GotItem, ddbResult.GotItem, "GetItem")
	assertAttributeMapEqual(t, dqlResult.Updated, ddbResult.Updated, "UpdateItem ReturnValues")
	assertAttributeMapEqual(t, dqlResult.Deleted, ddbResult.Deleted, "DeleteItem ReturnValues")

	dqlErr := unknownOperationResponse(t, dql.Endpoint, dql.HTTPClient)
	ddbErr := unknownOperationResponse(t, ddbLocal.Endpoint, ddbLocal.HTTPClient)
	if dqlErr.StatusCode != ddbErr.StatusCode {
		t.Fatalf("unknown op status mismatch dql=%d dynamodb-local=%d", dqlErr.StatusCode, ddbErr.StatusCode)
	}
	if dqlErr.Type == "" || ddbErr.Type == "" {
		t.Fatalf("expected __type in unknown-op errors, got dql=%q dynamodb-local=%q", dqlErr.Type, ddbErr.Type)
	}
}

func runCoreCRUDScenario(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string) scenarioResult {
	t.Helper()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
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
		t.Fatalf("create table %q failed: %v", tableName, err)
	}

	listOut, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(100)})
	if err != nil {
		t.Fatalf("list tables for %q failed: %v", tableName, err)
	}
	sort.Strings(listOut.TableNames)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk":   &types.AttributeValueMemberS{Value: "user#1"},
			"name": &types.AttributeValueMemberS{Value: "alice"},
			"age":  &types.AttributeValueMemberN{Value: "41"},
		},
	})
	if err != nil {
		t.Fatalf("put item into %q failed: %v", tableName, err)
	}

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
	})
	if err != nil {
		t.Fatalf("get item from %q failed: %v", tableName, err)
	}

	updOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		AttributeUpdates: map[string]types.AttributeValueUpdate{
			"name": {
				Action: types.AttributeActionPut,
				Value:  &types.AttributeValueMemberS{Value: "alice-updated"},
			},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("update item in %q failed: %v", tableName, err)
	}

	delOut, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		t.Fatalf("delete item in %q failed: %v", tableName, err)
	}

	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	if err != nil {
		t.Fatalf("delete table %q failed: %v", tableName, err)
	}

	return scenarioResult{
		TableNames: listOut.TableNames,
		GotItem:    getOut.Item,
		Updated:    updOut.Attributes,
		Deleted:    delOut.Attributes,
	}
}

func assertAttributeMapEqual(t *testing.T, got, want map[string]types.AttributeValue, operation string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s attribute count mismatch got=%d want=%d", operation, len(got), len(want))
	}
	for key, gotAttr := range got {
		wantAttr, ok := want[key]
		if !ok {
			t.Fatalf("%s missing attribute %q in comparator", operation, key)
		}
		if !attributeValueEqual(gotAttr, wantAttr) {
			t.Fatalf("%s mismatch for %q: got=%#v want=%#v", operation, key, gotAttr, wantAttr)
		}
	}
}

func attributeValueEqual(a, b types.AttributeValue) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		return ok && av.Value == bv.Value
	default:
		return false
	}
}

type rawError struct {
	StatusCode int
	Type       string
}

func unknownOperationResponse(t *testing.T, endpoint string, client *http.Client) rawError {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Nope")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unknown operation request failed: %v", err)
	}
	defer resp.Body.Close()

	var body struct {
		Type string `json:"__type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response failed: %v", err)
	}

	return rawError{StatusCode: resp.StatusCode, Type: body.Type}
}
