package testutil

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twisp/dql/pkg/api"
	"github.com/twisp/dql/pkg/storage"
	"github.com/twisp/dql/pkg/table"
)

type Harness struct {
	Server     *httptest.Server
	Endpoint   string
	HTTPClient *http.Client
	Client     *dynamodb.Client
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
	return &Harness{Server: srv, Endpoint: srv.URL, HTTPClient: srv.Client(), Client: client}
}

func NewDynamoDBLocalHarness(t *testing.T) *Harness {
	t.Helper()

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "amazon/dynamodb-local:latest",
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor:   wait.ForListeningPort("8000/tcp"),
	}

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("docker unavailable for dynamodb-local differential test: %v", err)
	}
	t.Cleanup(func() {
		_ = ctr.Terminate(ctx)
	})

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("resolve dynamodb-local host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "8000/tcp")
	if err != nil {
		t.Fatalf("resolve dynamodb-local port: %v", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
	client := newClientForEndpoint(t, endpoint)

	return &Harness{Endpoint: endpoint, HTTPClient: http.DefaultClient, Client: client}
}

func newClientForEndpoint(t *testing.T, endpoint string) *dynamodb.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}
