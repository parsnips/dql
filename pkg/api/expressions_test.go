package api

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestEvaluateConditionExpressionLogicalAndFunctions(t *testing.T) {
	ctx := newExpressionContext(map[string]string{"#name": "name", "#tags": "tags", "#counter": "counter"}, map[string]types.AttributeValue{
		":prefix": &types.AttributeValueMemberS{Value: "al"},
		":tag":    &types.AttributeValueMemberS{Value: "red"},
		":min":    &types.AttributeValueMemberN{Value: "2"},
		":one":    &types.AttributeValueMemberN{Value: "1"},
		":two":    &types.AttributeValueMemberN{Value: "2"},
		":ten":    &types.AttributeValueMemberN{Value: "10"},
	})
	item := map[string]types.AttributeValue{
		"name":    &types.AttributeValueMemberS{Value: "alex"},
		"tags":    &types.AttributeValueMemberSS{Value: []string{"red", "blue"}},
		"counter": &types.AttributeValueMemberN{Value: "2"},
	}

	expr := "begins_with(#name, :prefix) AND contains(#tags, :tag) AND size(#tags) >= :min"
	matched, err := evaluateConditionExpression(expr, item, ctx)
	if err != nil {
		t.Fatalf("evaluateConditionExpression returned error: %v", err)
	}
	if !matched {
		t.Fatalf("expected expression to match")
	}

	expr = "NOT (#counter BETWEEN :one AND :two) OR #counter IN (:ten, :two)"
	matched, err = evaluateConditionExpression(expr, item, ctx)
	if err != nil {
		t.Fatalf("evaluateConditionExpression returned error: %v", err)
	}
	if !matched {
		t.Fatalf("expected OR/NOT/BETWEEN/IN expression to match")
	}
}

func TestEvaluateConditionExpressionMalformed(t *testing.T) {
	ctx := newExpressionContext(nil, map[string]types.AttributeValue{":v": &types.AttributeValueMemberN{Value: "1"}})
	_, err := evaluateConditionExpression("(attribute_exists(a)", map[string]types.AttributeValue{"a": &types.AttributeValueMemberN{Value: "1"}}, ctx)
	if err == nil || !strings.Contains(err.Error(), "ValidationException") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestEvaluateConditionExpressionMissingItemCases(t *testing.T) {
	ctx := newExpressionContext(map[string]string{"#pk": "pk"}, nil)
	matched, err := evaluateConditionExpression("attribute_not_exists(#pk)", nil, ctx)
	if err != nil {
		t.Fatalf("evaluateConditionExpression returned error: %v", err)
	}
	if !matched {
		t.Fatalf("expected attribute_not_exists to match missing item")
	}
}

func TestApplyUpdateExpressionSetParity(t *testing.T) {
	ctx := newExpressionContext(map[string]string{"#ss": "ss", "#ns": "ns", "#bs": "bs"}, map[string]types.AttributeValue{
		":addss": &types.AttributeValueMemberSS{Value: []string{"b", "c"}},
		":addns": &types.AttributeValueMemberNS{Value: []string{"2", "3"}},
		":addbs": &types.AttributeValueMemberBS{Value: [][]byte{[]byte("y"), []byte("z")}},
		":delss": &types.AttributeValueMemberSS{Value: []string{"a"}},
		":delns": &types.AttributeValueMemberNS{Value: []string{"1"}},
		":delbs": &types.AttributeValueMemberBS{Value: [][]byte{[]byte("x")}},
	})
	item := map[string]types.AttributeValue{
		"ss": &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
		"ns": &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
		"bs": &types.AttributeValueMemberBS{Value: [][]byte{[]byte("x"), []byte("y")}},
	}

	err := applyUpdateExpression(item, "ADD #ss :addss, #ns :addns, #bs :addbs DELETE #ss :delss, #ns :delns, #bs :delbs", ctx)
	if err != nil {
		t.Fatalf("applyUpdateExpression returned error: %v", err)
	}

	ss := item["ss"].(*types.AttributeValueMemberSS)
	if len(ss.Value) != 2 || ss.Value[0] != "b" || ss.Value[1] != "c" {
		t.Fatalf("unexpected SS result: %#v", ss.Value)
	}
	ns := item["ns"].(*types.AttributeValueMemberNS)
	if len(ns.Value) != 2 || ns.Value[0] != "2" || ns.Value[1] != "3" {
		t.Fatalf("unexpected NS result: %#v", ns.Value)
	}
	bs := item["bs"].(*types.AttributeValueMemberBS)
	if len(bs.Value) != 2 || string(bs.Value[0]) != "y" || string(bs.Value[1]) != "z" {
		t.Fatalf("unexpected BS result: %#v", bs.Value)
	}
}

func TestUpdatedAttributes(t *testing.T) {
	oldItem := map[string]types.AttributeValue{
		"a": &types.AttributeValueMemberN{Value: "1"},
		"b": &types.AttributeValueMemberS{Value: "x"},
	}
	newItem := map[string]types.AttributeValue{
		"a": &types.AttributeValueMemberN{Value: "2"},
		"c": &types.AttributeValueMemberBOOL{Value: true},
	}
	oldChanged, newChanged := updatedAttributes(oldItem, newItem)
	if len(oldChanged) != 2 || len(newChanged) != 2 {
		t.Fatalf("unexpected updated attributes: old=%v new=%v", oldChanged, newChanged)
	}
	if _, ok := oldChanged["b"]; !ok {
		t.Fatalf("expected removed attr in UPDATED_OLD")
	}
	if _, ok := newChanged["c"]; !ok {
		t.Fatalf("expected added attr in UPDATED_NEW")
	}
}
