package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/twisp/dql/internal/testutil"
)

type scenarioResult struct {
	TableNames            []string
	UpdateTableSnapshot   tableSnapshot
	EnabledTableSnapshot  tableSnapshot
	DisabledTableSnapshot tableSnapshot
	GotItem               map[string]types.AttributeValue
	Updated               map[string]types.AttributeValue
	Deleted               map[string]types.AttributeValue
	ExprUpdate            map[string]types.AttributeValue
	ExprAdvancedUpdate    map[string]types.AttributeValue
	ExprSetAddUpdate      map[string]types.AttributeValue
	ExprSetDelUpdate      map[string]types.AttributeValue
	UpdatedOldReturn      map[string]types.AttributeValue
	UpdatedNewReturn      map[string]types.AttributeValue
	CondErr               string
}

type batchScenarioResult struct {
	PutUnprocessedCount    int
	GetUnprocessedKeyCount int
	DeleteUnprocessedCount int
	ProjectedItems         []map[string]types.AttributeValue
	DeletedItem            map[string]types.AttributeValue
}

type tableSnapshot struct {
	StreamEnabled  bool
	StreamViewType types.StreamViewType
	GSINames       []string
	GSIStatuses    map[string]types.IndexStatus
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
	assertTableSnapshotParity(t, dqlResult.UpdateTableSnapshot, ddbResult.UpdateTableSnapshot, "UpdateTable response")
	assertTableSnapshotParity(t, dqlResult.EnabledTableSnapshot, ddbResult.EnabledTableSnapshot, "DescribeTable after stream/GSI enable")
	assertTableSnapshotParity(t, dqlResult.DisabledTableSnapshot, ddbResult.DisabledTableSnapshot, "DescribeTable after stream disable")

	assertAttributeMapEqual(t, dqlResult.GotItem, ddbResult.GotItem, "GetItem")
	assertAttributeMapEqual(t, dqlResult.Updated, ddbResult.Updated, "UpdateItem ReturnValues")
	assertAttributeMapEqual(t, dqlResult.Deleted, ddbResult.Deleted, "DeleteItem ReturnValues")
	assertAttributeMapEqual(t, dqlResult.ExprUpdate, ddbResult.ExprUpdate, "UpdateItem UpdateExpression ReturnValues")
	assertAttributeMapEqual(t, dqlResult.ExprAdvancedUpdate, ddbResult.ExprAdvancedUpdate, "UpdateItem advanced ConditionExpression parity")
	assertAttributeMapEqual(t, dqlResult.ExprSetAddUpdate, ddbResult.ExprSetAddUpdate, "UpdateItem ADD NS/BS parity")
	assertAttributeMapEqual(t, dqlResult.ExprSetDelUpdate, ddbResult.ExprSetDelUpdate, "UpdateItem DELETE NS/BS parity")
	assertAttributeMapEqual(t, dqlResult.UpdatedOldReturn, ddbResult.UpdatedOldReturn, "UpdateItem ReturnValues=UPDATED_OLD")
	assertAttributeMapEqual(t, dqlResult.UpdatedNewReturn, ddbResult.UpdatedNewReturn, "UpdateItem ReturnValues=UPDATED_NEW")
	if dqlResult.CondErr != ddbResult.CondErr {
		t.Fatalf("conditional error mismatch dql=%q dynamodb-local=%q", dqlResult.CondErr, ddbResult.CondErr)
	}

	dqlErr := unknownOperationResponse(t, dql.Endpoint, dql.HTTPClient)
	ddbErr := unknownOperationResponse(t, ddbLocal.Endpoint, ddbLocal.HTTPClient)
	if dqlErr.StatusCode != ddbErr.StatusCode {
		t.Fatalf("unknown op status mismatch dql=%d dynamodb-local=%d", dqlErr.StatusCode, ddbErr.StatusCode)
	}
	if dqlErr.Type == "" || ddbErr.Type == "" {
		t.Fatalf("expected __type in unknown-op errors, got dql=%q dynamodb-local=%q", dqlErr.Type, ddbErr.Type)
	}
}

func TestDifferentialDynamoDBLocalAndDQLBatchWriteAndBatchGet(t *testing.T) {
	ctx := context.Background()
	dql := testutil.NewHarness(t)
	ddbLocal := testutil.NewDynamoDBLocalHarness(t)

	dqlResult := runBatchWriteGetScenario(t, ctx, dql.Client, "phase1_diff_batch_dql")
	ddbResult := runBatchWriteGetScenario(t, ctx, ddbLocal.Client, "phase1_diff_batch_local")

	if dqlResult.PutUnprocessedCount != ddbResult.PutUnprocessedCount {
		t.Fatalf("batch write put unprocessed mismatch dql=%d dynamodb-local=%d", dqlResult.PutUnprocessedCount, ddbResult.PutUnprocessedCount)
	}
	if dqlResult.GetUnprocessedKeyCount != ddbResult.GetUnprocessedKeyCount {
		t.Fatalf("batch get unprocessed keys mismatch dql=%d dynamodb-local=%d", dqlResult.GetUnprocessedKeyCount, ddbResult.GetUnprocessedKeyCount)
	}
	if dqlResult.DeleteUnprocessedCount != ddbResult.DeleteUnprocessedCount {
		t.Fatalf("batch write delete unprocessed mismatch dql=%d dynamodb-local=%d", dqlResult.DeleteUnprocessedCount, ddbResult.DeleteUnprocessedCount)
	}
	assertAttributeMapSliceEqualUnordered(t, dqlResult.ProjectedItems, ddbResult.ProjectedItems, "BatchGetItem projected responses")
	assertAttributeMapEqual(t, dqlResult.DeletedItem, ddbResult.DeletedItem, "GetItem after BatchWriteItem delete")
}

func runBatchWriteGetScenario(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string) batchScenarioResult {
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

	putOut, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: {
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
		t.Fatalf("batch write put for %q failed: %v", tableName, err)
	}

	getOut, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
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
		t.Fatalf("batch get for %q failed: %v", tableName, err)
	}

	delOut, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: {
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
					"sk": &types.AttributeValueMemberS{Value: "item#1"},
				}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("batch write delete for %q failed: %v", tableName, err)
	}

	deletedGetOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#1"},
			"sk": &types.AttributeValueMemberS{Value: "item#1"},
		},
	})
	if err != nil {
		t.Fatalf("get deleted item for %q failed: %v", tableName, err)
	}

	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	if err != nil {
		t.Fatalf("delete table %q failed: %v", tableName, err)
	}

	return batchScenarioResult{
		PutUnprocessedCount:    countUnprocessedWriteRequests(putOut.UnprocessedItems),
		GetUnprocessedKeyCount: countUnprocessedKeys(getOut.UnprocessedKeys),
		DeleteUnprocessedCount: countUnprocessedWriteRequests(delOut.UnprocessedItems),
		ProjectedItems:         getOut.Responses[tableName],
		DeletedItem:            deletedGetOut.Item,
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

	updateOut, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
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
		t.Fatalf("update table (enable stream/create gsi) for %q failed: %v", tableName, err)
	}

	enabledSnap := waitForTableSnapshot(
		t, ctx, client, tableName,
		func(s tableSnapshot) bool {
			return s.StreamEnabled &&
				s.StreamViewType == types.StreamViewTypeNewImage &&
				containsString(s.GSINames, "gsi1")
		},
		"stream enabled + gsi present",
	)

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		},
	})
	if err != nil {
		t.Fatalf("update table (disable stream) for %q failed: %v", tableName, err)
	}

	disabledSnap := waitForTableSnapshot(
		t, ctx, client, tableName,
		func(s tableSnapshot) bool {
			return !s.StreamEnabled && containsString(s.GSINames, "gsi1")
		},
		"stream disabled + gsi retained",
	)

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

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk":      &types.AttributeValueMemberS{Value: "user#1"},
			"counter": &types.AttributeValueMemberN{Value: "1"},
			"tags":    &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
			"scores":  &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
			"blobs":   &types.AttributeValueMemberBS{Value: [][]byte{[]byte("x"), []byte("y")}},
		},
	})
	if err != nil {
		t.Fatalf("seed expression item in %q failed: %v", tableName, err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": "pk",
		},
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
	})
	condErr := ""
	if err != nil {
		var cfe *types.ConditionalCheckFailedException
		if !errors.As(err, &cfe) {
			t.Fatalf("expected conditional check failed in %q, got: %v", tableName, err)
		}
		condErr = cfe.ErrorCode()
	} else {
		t.Fatalf("expected conditional failure in %q", tableName)
	}

	exprOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		ConditionExpression: aws.String("#counter = :current"),
		UpdateExpression:    aws.String("SET #name = :name REMOVE #old ADD #counter :inc DELETE #tags :drop"),
		ExpressionAttributeNames: map[string]string{
			"#counter": "counter",
			"#name":    "name",
			"#old":     "old_attr",
			"#tags":    "tags",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":current": &types.AttributeValueMemberN{Value: "1"},
			":name":    &types.AttributeValueMemberS{Value: "alice"},
			":inc":     &types.AttributeValueMemberN{Value: "2"},
			":drop":    &types.AttributeValueMemberSS{Value: []string{"a", "x"}},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("expression update in %q failed: %v", tableName, err)
	}

	advancedExprOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		ConditionExpression: aws.String("begins_with(#name, :prefix) AND contains(#tags, :tag) AND size(#tags) >= :min AND (NOT (#counter BETWEEN :one AND :two) OR #counter IN (:three, :ten))"),
		UpdateExpression:    aws.String("SET #status = :active"),
		ExpressionAttributeNames: map[string]string{
			"#name":    "name",
			"#tags":    "tags",
			"#counter": "counter",
			"#status":  "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":prefix": &types.AttributeValueMemberS{Value: "al"},
			":tag":    &types.AttributeValueMemberS{Value: "b"},
			":min":    &types.AttributeValueMemberN{Value: "2"},
			":one":    &types.AttributeValueMemberN{Value: "1"},
			":two":    &types.AttributeValueMemberN{Value: "2"},
			":three":  &types.AttributeValueMemberN{Value: "3"},
			":ten":    &types.AttributeValueMemberN{Value: "10"},
			":active": &types.AttributeValueMemberS{Value: "active"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("advanced expression update in %q failed: %v", tableName, err)
	}

	setAddExprOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		UpdateExpression: aws.String("ADD #scores :addScores, #blobs :addBlobs"),
		ExpressionAttributeNames: map[string]string{
			"#scores": "scores",
			"#blobs":  "blobs",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":addScores": &types.AttributeValueMemberNS{Value: []string{"2", "3"}},
			":addBlobs":  &types.AttributeValueMemberBS{Value: [][]byte{[]byte("y"), []byte("z")}},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("set ADD expression update in %q failed: %v", tableName, err)
	}

	setDelExprOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		UpdateExpression: aws.String("DELETE #scores :delScores, #blobs :delBlobs"),
		ExpressionAttributeNames: map[string]string{
			"#scores": "scores",
			"#blobs":  "blobs",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":delScores": &types.AttributeValueMemberNS{Value: []string{"1"}},
			":delBlobs":  &types.AttributeValueMemberBS{Value: [][]byte{[]byte("x")}},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("set DELETE expression update in %q failed: %v", tableName, err)
	}

	updatedOldOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		UpdateExpression: aws.String("SET #name = :nameV2 REMOVE #status ADD #counter :inc"),
		ExpressionAttributeNames: map[string]string{
			"#name":    "name",
			"#status":  "status",
			"#counter": "counter",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":nameV2": &types.AttributeValueMemberS{Value: "alice-v2"},
			":inc":    &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueUpdatedOld,
	})
	if err != nil {
		t.Fatalf("updated-old return update in %q failed: %v", tableName, err)
	}

	updatedNewOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "tenant#2"},
			"sk": &types.AttributeValueMemberS{Value: "user#1"},
		},
		UpdateExpression: aws.String("SET #name = :nameV3 ADD #counter :inc"),
		ExpressionAttributeNames: map[string]string{
			"#name":    "name",
			"#counter": "counter",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":nameV3": &types.AttributeValueMemberS{Value: "alice-v3"},
			":inc":    &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if err != nil {
		t.Fatalf("updated-new return update in %q failed: %v", tableName, err)
	}

	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	if err != nil {
		t.Fatalf("delete table %q failed: %v", tableName, err)
	}

	return scenarioResult{
		TableNames:            listOut.TableNames,
		UpdateTableSnapshot:   tableSnapshotFromDescription(updateOut.TableDescription),
		EnabledTableSnapshot:  enabledSnap,
		DisabledTableSnapshot: disabledSnap,
		GotItem:               getOut.Item,
		Updated:               updOut.Attributes,
		Deleted:               delOut.Attributes,
		ExprUpdate:            exprOut.Attributes,
		ExprAdvancedUpdate:    advancedExprOut.Attributes,
		ExprSetAddUpdate:      setAddExprOut.Attributes,
		ExprSetDelUpdate:      setDelExprOut.Attributes,
		UpdatedOldReturn:      updatedOldOut.Attributes,
		UpdatedNewReturn:      updatedNewOut.Attributes,
		CondErr:               condErr,
	}
}

func tableSnapshotFromDescription(desc *types.TableDescription) tableSnapshot {
	snap := tableSnapshot{GSIStatuses: map[string]types.IndexStatus{}}
	if desc == nil {
		return snap
	}
	if desc.StreamSpecification != nil {
		snap.StreamEnabled = aws.ToBool(desc.StreamSpecification.StreamEnabled)
		snap.StreamViewType = desc.StreamSpecification.StreamViewType
	}
	for _, gsi := range desc.GlobalSecondaryIndexes {
		name := aws.ToString(gsi.IndexName)
		if name == "" {
			continue
		}
		snap.GSINames = append(snap.GSINames, name)
		snap.GSIStatuses[name] = gsi.IndexStatus
	}
	sort.Strings(snap.GSINames)
	return snap
}

func waitForTableSnapshot(
	t *testing.T,
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
	ready func(tableSnapshot) bool,
	stateLabel string,
) tableSnapshot {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	last := tableSnapshot{GSIStatuses: map[string]types.IndexStatus{}}
	for {
		descOut, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
		if err == nil {
			last = tableSnapshotFromDescription(descOut.Table)
			if ready(last) {
				return last
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s for %q; last snapshot=%#v", stateLabel, tableName, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func assertTableSnapshotParity(t *testing.T, got, want tableSnapshot, operation string) {
	t.Helper()
	if got.StreamEnabled != want.StreamEnabled {
		t.Fatalf("%s stream enabled mismatch got=%v want=%v", operation, got.StreamEnabled, want.StreamEnabled)
	}
	if got.StreamEnabled && got.StreamViewType != want.StreamViewType {
		t.Fatalf("%s stream view mismatch got=%v want=%v", operation, got.StreamViewType, want.StreamViewType)
	}
	if len(got.GSINames) != len(want.GSINames) {
		t.Fatalf("%s gsi count mismatch got=%v want=%v", operation, got.GSINames, want.GSINames)
	}
	for i := range got.GSINames {
		if got.GSINames[i] != want.GSINames[i] {
			t.Fatalf("%s gsi name mismatch got=%v want=%v", operation, got.GSINames, want.GSINames)
		}
		name := got.GSINames[i]
		if gotStatus := got.GSIStatuses[name]; gotStatus == "" {
			t.Fatalf("%s missing gsi status in dql for index %q", operation, name)
		}
		if wantStatus := want.GSIStatuses[name]; wantStatus == "" {
			t.Fatalf("%s missing gsi status in dynamodb-local for index %q", operation, name)
		}
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

func assertAttributeMapSliceEqualUnordered(t *testing.T, got, want []map[string]types.AttributeValue, operation string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s item count mismatch got=%d want=%d", operation, len(got), len(want))
	}
	used := make([]bool, len(want))
	for i, gotItem := range got {
		matched := false
		for j, wantItem := range want {
			if used[j] {
				continue
			}
			if attributeMapEqual(gotItem, wantItem) {
				used[j] = true
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("%s unmatched item at got[%d]=%#v", operation, i, gotItem)
		}
	}
}

func attributeMapEqual(got, want map[string]types.AttributeValue) bool {
	if len(got) != len(want) {
		return false
	}
	for key, gotAttr := range got {
		wantAttr, ok := want[key]
		if !ok {
			return false
		}
		if !attributeValueEqual(gotAttr, wantAttr) {
			return false
		}
	}
	return true
}

func attributeValueEqual(a, b types.AttributeValue) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberBOOL:
		bv, ok := b.(*types.AttributeValueMemberBOOL)
		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberB:
		bv, ok := b.(*types.AttributeValueMemberB)
		return ok && bytes.Equal(av.Value, bv.Value)
	case *types.AttributeValueMemberSS:
		bv, ok := b.(*types.AttributeValueMemberSS)
		if !ok || len(av.Value) != len(bv.Value) {
			return false
		}
		return sortedStringsEqual(av.Value, bv.Value)
	case *types.AttributeValueMemberNS:
		bv, ok := b.(*types.AttributeValueMemberNS)
		if !ok || len(av.Value) != len(bv.Value) {
			return false
		}
		return sortedStringsEqual(av.Value, bv.Value)
	case *types.AttributeValueMemberBS:
		bv, ok := b.(*types.AttributeValueMemberBS)
		if !ok || len(av.Value) != len(bv.Value) {
			return false
		}
		return sortedBinaryEqual(av.Value, bv.Value)
	default:
		return false
	}
}

func sortedStringsEqual(left, right []string) bool {
	l := append([]string{}, left...)
	r := append([]string{}, right...)
	sort.Strings(l)
	sort.Strings(r)
	for i := range l {
		if l[i] != r[i] {
			return false
		}
	}
	return true
}

func countUnprocessedWriteRequests(unprocessed map[string][]types.WriteRequest) int {
	total := 0
	for _, requests := range unprocessed {
		total += len(requests)
	}
	return total
}

func countUnprocessedKeys(unprocessed map[string]types.KeysAndAttributes) int {
	total := 0
	for _, keys := range unprocessed {
		total += len(keys.Keys)
	}
	return total
}

func sortedBinaryEqual(left, right [][]byte) bool {
	l := make([]string, len(left))
	r := make([]string, len(right))
	for i := range left {
		l[i] = string(left[i])
	}
	for i := range right {
		r[i] = string(right[i])
	}
	sort.Strings(l)
	sort.Strings(r)
	for i := range l {
		if l[i] != r[i] {
			return false
		}
	}
	return true
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
