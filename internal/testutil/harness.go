package testutil

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/twisp/dql/pkg/api"
	"github.com/twisp/dql/pkg/storage"
	"github.com/twisp/dql/pkg/table"
)

type Harness struct {
	Server *httptest.Server
	Client *dynamodb.Client
}

func NewHarness(t *testing.T) *Harness {
	t.Helper()
	cat := table.NewCatalog()
	eng := storage.NewMemoryEngine()
	srv := httptest.NewServer(api.NewServer(cat, eng).Handler())
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
	return &Harness{Server: srv, Client: client}
}
