package api

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestEvaluateConditionExpressionComparisonOperators(t *testing.T) {
	ctx := newExpressionContext(map[string]string{"#counter": "counter"}, map[string]types.AttributeValue{
		":one": &types.AttributeValueMemberN{Value: "1"},
		":two": &types.AttributeValueMemberN{Value: "2"},
	})
	item := map[string]types.AttributeValue{
		"counter": &types.AttributeValueMemberN{Value: "1"},
	}

	matched, err := evaluateConditionExpression("#counter = :one", item, ctx)
	if err != nil {
		t.Fatalf("equals expression returned error: %v", err)
	}
	if !matched {
		t.Fatalf("equals expression should match")
	}

	matched, err = evaluateConditionExpression("#counter <> :two", item, ctx)
	if err != nil {
		t.Fatalf("not-equals expression returned error: %v", err)
	}
	if !matched {
		t.Fatalf("not-equals expression should match")
	}

	for _, expr := range []string{
		"#counter >= :one",
		"#counter <= :one",
		"#counter > :one",
		"#counter < :one",
	} {
		_, err = evaluateConditionExpression(expr, item, ctx)
		if err == nil {
			t.Fatalf("expected validation error for expression %q", expr)
		}
		if !strings.Contains(err.Error(), "unsupported ConditionExpression") {
			t.Fatalf("expected unsupported ConditionExpression error for %q, got %q", expr, err)
		}
	}
}
