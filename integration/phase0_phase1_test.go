package integration

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
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
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String("gsi1"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("update table failed: %v", err)
	}
	if updateTableOut.TableDescription == nil || aws.ToString(updateTableOut.TableDescription.TableName) != "phase1_items" {
		t.Fatalf("unexpected update table output: %#v", updateTableOut.TableDescription)
	}
	if updateTableOut.TableDescription.StreamSpecification == nil || !aws.ToBool(updateTableOut.TableDescription.StreamSpecification.StreamEnabled) {
		t.Fatalf("expected stream to be enabled: %#v", updateTableOut.TableDescription.StreamSpecification)
	}
	if len(updateTableOut.TableDescription.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("expected one gsi in update response: %#v", updateTableOut.TableDescription.GlobalSecondaryIndexes)
	}
	if got := updateTableOut.TableDescription.GlobalSecondaryIndexes[0].IndexStatus; got != types.IndexStatusActive {
		t.Fatalf("unexpected gsi status: %v", got)
	}

	descOut, err = client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("phase1_items")})
	if err != nil {
		t.Fatalf("describe after update failed: %v", err)
	}
	if descOut.Table == nil || descOut.Table.StreamSpecification == nil || !aws.ToBool(descOut.Table.StreamSpecification.StreamEnabled) {
		t.Fatalf("expected stream spec to persist: %#v", descOut.Table)
	}
	if len(descOut.Table.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("expected gsi to persist after update: %#v", descOut.Table.GlobalSecondaryIndexes)
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
		{"pk": &types.AttributeValueMemberS{Value: "tenant#1"}, "sk": &types.AttributeValueMemberN{Value: "10"}, "name": &types.AttributeValueMemberS{Value: "e"}},
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
	if got, want := len(queryOut.Items), 3; got != want {
		t.Fatalf("query items got=%d want=%d", got, want)
	}
	desc := []string{"10", "3", "2"}
	for i, want := range desc {
		if got := queryOut.Items[i]["sk"].(*types.AttributeValueMemberN).Value; got != want {
			t.Fatalf("descending order mismatch at %d: got=%s want=%s", i, got, want)
		}
	}

	ascending, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("phase1_qs"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant#1"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("ascending query failed: %v", err)
	}
	ordered := []string{"1", "2", "3", "10"}
	for i, want := range ordered {
		if got := ascending.Items[i]["sk"].(*types.AttributeValueMemberN).Value; got != want {
			t.Fatalf("ascending order mismatch at %d: got=%s want=%s", i, got, want)
		}
	}

	type sortCase struct {
		name   string
		expr   string
		values map[string]types.AttributeValue
		want   []string
	}
	sortCases := []sortCase{
		{name: "eq", expr: "pk = :pk AND sk = :eq", values: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "tenant#1"}, ":eq": &types.AttributeValueMemberN{Value: "2"}}, want: []string{"2"}},
		{name: "lt", expr: "pk = :pk AND sk < :v", values: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "tenant#1"}, ":v": &types.AttributeValueMemberN{Value: "3"}}, want: []string{"1", "2"}},
		{name: "lte", expr: "pk = :pk AND sk <= :v", values: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "tenant#1"}, ":v": &types.AttributeValueMemberN{Value: "3"}}, want: []string{"1", "2", "3"}},
		{name: "gt", expr: "pk = :pk AND sk > :v", values: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "tenant#1"}, ":v": &types.AttributeValueMemberN{Value: "2"}}, want: []string{"3", "10"}},
		{name: "gte", expr: "pk = :pk AND sk >= :v", values: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "tenant#1"}, ":v": &types.AttributeValueMemberN{Value: "2"}}, want: []string{"2", "3", "10"}},
		{name: "between", expr: "pk = :pk AND sk BETWEEN :low AND :high", values: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "tenant#1"}, ":low": &types.AttributeValueMemberN{Value: "2"}, ":high": &types.AttributeValueMemberN{Value: "10"}}, want: []string{"2", "3", "10"}},
	}
	for _, tc := range sortCases {
		t.Run("sort_"+tc.name, func(t *testing.T) {
			out, err := client.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String("phase1_qs"),
				KeyConditionExpression:    aws.String(tc.expr),
				ExpressionAttributeValues: tc.values,
			})
			if err != nil {
				t.Fatalf("query %s failed: %v", tc.name, err)
			}
			if got, want := len(out.Items), len(tc.want); got != want {
				t.Fatalf("query %s len got=%d want=%d", tc.name, got, want)
			}
			for i, want := range tc.want {
				if got := out.Items[i]["sk"].(*types.AttributeValueMemberN).Value; got != want {
					t.Fatalf("query %s item %d got=%s want=%s", tc.name, i, got, want)
				}
			}
		})
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
	if got, want := len(next.Items), 2; got != want {
		t.Fatalf("next page items got=%d want=%d", got, want)
	}

	queryBeginsWithTable := "phase1_qs_str"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(queryBeginsWithTable),
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
		t.Fatalf("create begins_with table failed: %v", err)
	}
	for _, item := range []map[string]types.AttributeValue{
		{"pk": &types.AttributeValueMemberS{Value: "tenant#1"}, "sk": &types.AttributeValueMemberS{Value: "ord#1"}},
		{"pk": &types.AttributeValueMemberS{Value: "tenant#1"}, "sk": &types.AttributeValueMemberS{Value: "ord#2"}},
		{"pk": &types.AttributeValueMemberS{Value: "tenant#1"}, "sk": &types.AttributeValueMemberS{Value: "inv#1"}},
	} {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(queryBeginsWithTable), Item: item})
		if err != nil {
			t.Fatalf("put begins_with item failed: %v", err)
		}
	}
	beginsWithOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(queryBeginsWithTable),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "tenant#1"},
			":prefix": &types.AttributeValueMemberS{Value: "ord#"},
		},
	})
	if err != nil {
		t.Fatalf("begins_with query failed: %v", err)
	}
	if got, want := len(beginsWithOut.Items), 2; got != want {
		t.Fatalf("begins_with items got=%d want=%d", got, want)
	}

	scanOut, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("phase1_qs")})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if got, want := len(scanOut.Items), 5; got != want {
		t.Fatalf("scan items got=%d want=%d", got, want)
	}

	scanCount, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("phase1_qs"), Select: types.SelectCount})
	if err != nil {
		t.Fatalf("scan count failed: %v", err)
	}
	if got, want := scanCount.Count, int32(5); got != want {
		t.Fatalf("scan count got=%d want=%d", got, want)
	}
	if len(scanCount.Items) != 0 {
		t.Fatalf("expected no items for Select=COUNT")
	}

	queryCount, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("phase1_qs"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "tenant#1"},
		},
		Select: types.SelectCount,
	})
	if err != nil {
		t.Fatalf("query count failed: %v", err)
	}
	if got, want := queryCount.Count, int32(4); got != want {
		t.Fatalf("query count got=%d want=%d", got, want)
	}
	if len(queryCount.Items) != 0 {
		t.Fatalf("expected no query items for Select=COUNT")
	}

	scanFiltered, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:            aws.String("phase1_qs"),
		FilterExpression:     aws.String("#name = :name"),
		ProjectionExpression: aws.String("#pk, #name"),
		ExpressionAttributeNames: map[string]string{
			"#pk":   "pk",
			"#name": "name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name": &types.AttributeValueMemberS{Value: "b"},
		},
	})
	if err != nil {
		t.Fatalf("scan filtered failed: %v", err)
	}
	if got, want := scanFiltered.ScannedCount, int32(5); got != want {
		t.Fatalf("scan filtered scanned count got=%d want=%d", got, want)
	}
	if got, want := scanFiltered.Count, int32(1); got != want {
		t.Fatalf("scan filtered count got=%d want=%d", got, want)
	}
	if got, want := len(scanFiltered.Items), 1; got != want {
		t.Fatalf("scan filtered items got=%d want=%d", got, want)
	}
	if _, ok := scanFiltered.Items[0]["pk"]; !ok {
		t.Fatalf("expected projected pk field")
	}
	if _, ok := scanFiltered.Items[0]["name"]; !ok {
		t.Fatalf("expected projected name field")
	}
	if _, ok := scanFiltered.Items[0]["sk"]; ok {
		t.Fatalf("did not expect non-projected sk field")
	}

	queryProjected, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("phase1_qs"),
		KeyConditionExpression: aws.String("pk = :pk"),
		FilterExpression:       aws.String("#name = :name"),
		ProjectionExpression:   aws.String("#name"),
		ExpressionAttributeNames: map[string]string{
			"#name": "name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":   &types.AttributeValueMemberS{Value: "tenant#1"},
			":name": &types.AttributeValueMemberS{Value: "c"},
		},
	})
	if err != nil {
		t.Fatalf("query filtered/projection failed: %v", err)
	}
	if got, want := queryProjected.Count, int32(1); got != want {
		t.Fatalf("query filtered count got=%d want=%d", got, want)
	}
	if got, want := len(queryProjected.Items), 1; got != want {
		t.Fatalf("query projected items got=%d want=%d", got, want)
	}
	if _, ok := queryProjected.Items[0]["name"]; !ok {
		t.Fatalf("expected projected query name field")
	}
	if _, ok := queryProjected.Items[0]["sk"]; ok {
		t.Fatalf("did not expect projected query sk field")
	}
}

func TestBatchWriteAndBatchGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	h := testutil.NewHarness(t)
	client := h.Client

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("phase1_batch"),
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

	batchWriteOut, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"phase1_batch": {
				{
					PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
						"pk":   &types.AttributeValueMemberS{Value: "tenant#1"},
						"sk":   &types.AttributeValueMemberS{Value: "item#1"},
						"name": &types.AttributeValueMemberS{Value: "one"},
					}},
				},
				{
					PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
						"pk":   &types.AttributeValueMemberS{Value: "tenant#1"},
						"sk":   &types.AttributeValueMemberS{Value: "item#2"},
						"name": &types.AttributeValueMemberS{Value: "two"},
					}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("batch write put failed: %v", err)
	}
	if got := len(batchWriteOut.UnprocessedItems); got != 0 {
		t.Fatalf("expected no unprocessed items, got: %#v", batchWriteOut.UnprocessedItems)
	}

	batchGetOut, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"phase1_batch": {
				Keys: []map[string]types.AttributeValue{
					{
						"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
						"sk": &types.AttributeValueMemberS{Value: "item#1"},
					},
					{
						"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
						"sk": &types.AttributeValueMemberS{Value: "item#2"},
					},
					{
						"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
						"sk": &types.AttributeValueMemberS{Value: "item#missing"},
					},
				},
				ProjectionExpression: aws.String("#pk,#name"),
				ExpressionAttributeNames: map[string]string{
					"#pk":   "pk",
					"#name": "name",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("batch get failed: %v", err)
	}
	if got := len(batchGetOut.UnprocessedKeys); got != 0 {
		t.Fatalf("expected no unprocessed keys, got: %#v", batchGetOut.UnprocessedKeys)
	}
	items := batchGetOut.Responses["phase1_batch"]
	if got, want := len(items), 2; got != want {
		t.Fatalf("unexpected batch get size got=%d want=%d", got, want)
	}
	for _, item := range items {
		if _, ok := item["pk"]; !ok {
			t.Fatalf("expected pk in projected item: %#v", item)
		}
		if _, ok := item["name"]; !ok {
			t.Fatalf("expected name in projected item: %#v", item)
		}
		if _, ok := item["sk"]; ok {
			t.Fatalf("did not expect sk in projected item: %#v", item)
		}
	}

	batchWriteOut, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"phase1_batch": {
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
					"sk": &types.AttributeValueMemberS{Value: "item#1"},
				}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("batch write delete failed: %v", err)
	}
	if got := len(batchWriteOut.UnprocessedItems); got != 0 {
		t.Fatalf("expected no unprocessed items after delete, got: %#v", batchWriteOut.UnprocessedItems)
	}

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("phase1_batch"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "item#1"},
		},
	})
	if err != nil {
		t.Fatalf("get after batch delete failed: %v", err)
	}
	if len(getOut.Item) != 0 {
		t.Fatalf("expected deleted item to be missing, got: %#v", getOut.Item)
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

func TestConditionalPutItemIsAtomic(t *testing.T) {
	ctx := context.Background()
	h := testutil.NewHarness(t)
	client := h.Client

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("phase1_conditional_atomic"),
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

	const workers = 64
	const rounds = 8

	for round := 0; round < rounds; round++ {
		var successes int32
		errCh := make(chan error, workers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(workers)

		for i := 0; i < workers; i++ {
			go func(worker int) {
				defer wg.Done()
				<-start

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:           aws.String("phase1_conditional_atomic"),
					ConditionExpression: aws.String("attribute_not_exists(#pk)"),
					ExpressionAttributeNames: map[string]string{
						"#pk": "pk",
					},
					Item: map[string]types.AttributeValue{
						"pk":     &types.AttributeValueMemberS{Value: "tenant#1"},
						"sk":     &types.AttributeValueMemberS{Value: "item#" + strconv.Itoa(round)},
						"writer": &types.AttributeValueMemberN{Value: strconv.Itoa(worker)},
					},
				})
				if err == nil {
					atomic.AddInt32(&successes, 1)
					return
				}

				var cfe *types.ConditionalCheckFailedException
				if errors.As(err, &cfe) {
					return
				}
				errCh <- err
			}(i)
		}

		close(start)
		wg.Wait()
		close(errCh)

		for callErr := range errCh {
			t.Fatalf("unexpected put error in round %d: %v", round, callErr)
		}

		if got := atomic.LoadInt32(&successes); got != 1 {
			t.Fatalf("conditional put should be atomic in round %d: successes=%d want=1", round, got)
		}
	}
}

func TestConditionalUpdateItemIsAtomic(t *testing.T) {
	ctx := context.Background()
	h := testutil.NewHarness(t)
	client := h.Client

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("phase1_conditional_update_atomic"),
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

	const workers = 64
	const rounds = 8

	for round := 0; round < rounds; round++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("phase1_conditional_update_atomic"),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "tenant#1"},
				"sk":      &types.AttributeValueMemberS{Value: "counter#" + strconv.Itoa(round)},
				"counter": &types.AttributeValueMemberN{Value: "0"},
			},
		})
		if err != nil {
			t.Fatalf("seed item failed in round %d: %v", round, err)
		}

		var successes int32
		errCh := make(chan error, workers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(workers)

		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				<-start

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String("phase1_conditional_update_atomic"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
						"sk": &types.AttributeValueMemberS{Value: "counter#" + strconv.Itoa(round)},
					},
					ConditionExpression: aws.String("#counter = :zero"),
					UpdateExpression:    aws.String("ADD #counter :one"),
					ExpressionAttributeNames: map[string]string{
						"#counter": "counter",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":zero": &types.AttributeValueMemberN{Value: "0"},
						":one":  &types.AttributeValueMemberN{Value: "1"},
					},
				})
				if err == nil {
					atomic.AddInt32(&successes, 1)
					return
				}

				var cfe *types.ConditionalCheckFailedException
				if errors.As(err, &cfe) {
					return
				}
				errCh <- err
			}()
		}

		close(start)
		wg.Wait()
		close(errCh)

		for callErr := range errCh {
			t.Fatalf("unexpected update error in round %d: %v", round, callErr)
		}

		if got := atomic.LoadInt32(&successes); got != 1 {
			t.Fatalf("conditional update should be atomic in round %d: successes=%d want=1", round, got)
		}

		gotItem, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("phase1_conditional_update_atomic"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
				"sk": &types.AttributeValueMemberS{Value: "counter#" + strconv.Itoa(round)},
			},
		})
		if err != nil {
			t.Fatalf("get item failed in round %d: %v", round, err)
		}
		if got := gotItem.Item["counter"].(*types.AttributeValueMemberN).Value; got != "1" {
			t.Fatalf("counter should be 1 in round %d, got=%s", round, got)
		}
	}
}
