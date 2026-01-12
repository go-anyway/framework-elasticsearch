package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOptions_DefaultValues(t *testing.T) {
	opts := Options{
		Addresses: []string{"http://localhost:9200"},
	}

	if opts.MaxRetries != 0 {
		t.Errorf("MaxRetries = %v, want 0 (default)", opts.MaxRetries)
	}
	if opts.DialTimeout != 0 {
		t.Errorf("DialTimeout = %v, want 0", opts.DialTimeout)
	}
	if opts.ReadTimeout != 0 {
		t.Errorf("ReadTimeout = %v, want 0", opts.ReadTimeout)
	}
	if opts.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0", opts.WriteTimeout)
	}
	if opts.EnableTLS {
		t.Errorf("EnableTLS = %v, want false", opts.EnableTLS)
	}
	if opts.EnableTrace {
		t.Errorf("EnableTrace = %v, want false", opts.EnableTrace)
	}
}

func TestOptions_WithTimeouts(t *testing.T) {
	opts := Options{
		Addresses:    []string{"http://localhost:9200"},
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxRetries:   5,
	}

	if opts.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %v, want 5s", opts.DialTimeout)
	}
	if opts.ReadTimeout != 10*time.Second {
		t.Errorf("ReadTimeout = %v, want 10s", opts.ReadTimeout)
	}
	if opts.WriteTimeout != 10*time.Second {
		t.Errorf("WriteTimeout = %v, want 10s", opts.WriteTimeout)
	}
	if opts.MaxRetries != 5 {
		t.Errorf("MaxRetries = %v, want 5", opts.MaxRetries)
	}
}

func TestOptions_WithCredentials(t *testing.T) {
	opts := Options{
		Addresses: []string{"http://localhost:9200"},
		Username:  "elastic",
		Password:  "password",
	}

	if opts.Username != "elastic" {
		t.Errorf("Username = %v, want 'elastic'", opts.Username)
	}
	if opts.Password != "password" {
		t.Errorf("Password = %v, want 'password'", opts.Password)
	}
}

func TestOptions_WithAPIKey(t *testing.T) {
	opts := Options{
		Addresses: []string{"http://localhost:9200"},
		APIKey:    "test-api-key",
	}

	if opts.APIKey != "test-api-key" {
		t.Errorf("APIKey = %v, want 'test-api-key'", opts.APIKey)
	}
}

func TestOptions_WithCloudID(t *testing.T) {
	opts := Options{
		Addresses: []string{"http://localhost:9200"},
		CloudID:   "test-cloud-id",
	}

	if opts.CloudID != "test-cloud-id" {
		t.Errorf("CloudID = %v, want 'test-cloud-id'", opts.CloudID)
	}
}

func TestOptions_WithTLS(t *testing.T) {
	opts := Options{
		Addresses: []string{"https://localhost:9200"},
		EnableTLS: true,
		CACert:    "/path/to/ca.crt",
	}

	if !opts.EnableTLS {
		t.Error("EnableTLS should be true")
	}
	if opts.CACert != "/path/to/ca.crt" {
		t.Errorf("CACert = %v, want '/path/to/ca.crt'", opts.CACert)
	}
}

func TestElasticsearchClient_EnableTraceField(t *testing.T) {
	tests := []struct {
		name        string
		enableTrace bool
	}{
		{"trace disabled", false},
		{"trace enabled", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ElasticsearchClient{
				EnableTrace: tt.enableTrace,
			}

			if client.EnableTrace != tt.enableTrace {
				t.Errorf("EnableTrace = %v, want %v", client.EnableTrace, tt.enableTrace)
			}
		})
	}
}

func TestElasticsearchClient_NilClient(t *testing.T) {
	client := &ElasticsearchClient{}

	if client.IsConnected() {
		t.Error("IsConnected() should return false for nil client")
	}

	ctx := context.Background()
	err := client.Ping(ctx)
	if err == nil {
		t.Error("Ping() with nil client should return error")
	}
	if err.Error() != "elasticsearch client is not initialized" {
		t.Errorf("error message = %v, want 'elasticsearch client is not initialized'", err.Error())
	}
}

func TestNewElasticsearch_NilOptions(t *testing.T) {
	client, err := NewElasticsearch(nil)

	if err == nil {
		t.Error("NewElasticsearch(nil) should return error")
	}
	if client != nil {
		t.Error("NewElasticsearch(nil) should return nil client")
	}
	if err.Error() != "elasticsearch options cannot be nil" {
		t.Errorf("error message = %v, want 'elasticsearch options cannot be nil'", err.Error())
	}
}

func TestNewElasticsearch_EmptyAddresses(t *testing.T) {
	opts := &Options{
		Addresses: []string{},
	}

	client, err := NewElasticsearch(opts)

	if err == nil {
		t.Error("NewElasticsearch with empty addresses should return error")
	}
	if client != nil {
		t.Error("NewElasticsearch with empty addresses should return nil client")
	}
	if err.Error() != "elasticsearch addresses cannot be empty" {
		t.Errorf("error message = %v, want 'elasticsearch addresses cannot be empty'", err.Error())
	}
}

func TestNewElasticsearch_ConnectionFailure(t *testing.T) {
	opts := &Options{
		Addresses:   []string{"http://invalid-address:9200"},
		DialTimeout: 100 * time.Millisecond,
		MaxRetries:  1,
	}

	client, err := NewElasticsearch(opts)

	if err == nil {
		t.Error("NewElasticsearch with invalid address should return error")
	}
	if client != nil {
		t.Error("NewElasticsearch with invalid address should return nil client")
	}
}

func TestIndex_BytesBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "PUT" && r.URL.Path == "/test-index/_doc/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"_index":"test-index","_id":"doc-1","result":"created"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	body := []byte(`{"title":"Test Document","content":"This is a test"}`)
	err = client.Index(context.Background(), "test-index", "doc-1", body)
	if err != nil {
		t.Errorf("Index() error = %v", err)
	}
}

func TestIndex_JSONMarshalError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	ch := make(chan int)
	err = client.Index(context.Background(), "test-index", "doc-1", ch)
	if err == nil {
		t.Error("Index() with unmarshallable body should return error")
	}
}

func TestGet_InvalidResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "GET" && r.URL.Path == "/test-index/_doc/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`invalid json`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	_, err = client.Get(context.Background(), "test-index", "doc-1")
	if err == nil {
		t.Error("Get() with invalid JSON response should return error")
	}
}

func TestDeleteIndex_ErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "DELETE" && r.URL.Path == "/test-index" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":{"type":"internal_server_error","reason":"Unknown error"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	err = client.DeleteIndex(context.Background(), "test-index")
	if err == nil {
		t.Error("DeleteIndex() with 500 should return error")
	}
}

func TestSearch_JSONMarshalError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	ch := make(map[string]interface{})
	ch["query"] = make(chan int)
	_, err = client.Search(context.Background(), "test-index", ch)
	if err == nil {
		t.Error("Search() with unmarshallable query should return error")
	}
}

func TestBulk_EmptyBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/_bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"errors":false,"items":[]}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	err = client.Bulk(context.Background(), "")
	if err != nil {
		t.Errorf("Bulk() with empty body error = %v", err)
	}
}

func TestCreateIndex_JSONMarshalError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	ch := make(map[string]interface{})
	ch["settings"] = make(chan int)
	err = client.CreateIndex(context.Background(), "test-index", ch)
	if err == nil {
		t.Error("CreateIndex() with unmarshallable settings should return error")
	}
}

func TestUpdate_StringBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/test-index/_update/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"result":"updated"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	body := `{"title":"Updated Title"}`
	err = client.Update(context.Background(), "test-index", "doc-1", body)
	if err != nil {
		t.Errorf("Update() with string body error = %v", err)
	}
}

func TestUpdate_BytesBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/test-index/_update/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"result":"updated"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	body := []byte(`{"title":"Updated Title"}`)
	err = client.Update(context.Background(), "test-index", "doc-1", body)
	if err != nil {
		t.Errorf("Update() with bytes body error = %v", err)
	}
}

func TestUpdate_JSONMarshalError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	ch := make(chan int)
	err = client.Update(context.Background(), "test-index", "doc-1", ch)
	if err == nil {
		t.Error("Update() with unmarshallable body should return error")
	}
}

func TestUpdateByQuery_NilScript(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/test-index/_update_by_query" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"updated":5,"batches":1}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	query := map[string]interface{}{
		"match_all": map[string]interface{}{},
	}
	result, err := client.UpdateByQuery(context.Background(), "test-index", query, nil)
	if err != nil {
		t.Errorf("UpdateByQuery() with nil script error = %v", err)
	}
	if result == nil {
		t.Error("UpdateByQuery() returned nil result")
	}
}

func TestUpdateByQuery_JSONMarshalError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	query := make(map[string]interface{})
	query["match_all"] = make(chan int)
	script := map[string]interface{}{
		"source": "ctx._source.status = 'processed'",
	}
	_, err = client.UpdateByQuery(context.Background(), "test-index", query, script)
	if err == nil {
		t.Error("UpdateByQuery() with unmarshallable query should return error")
	}
}

func TestUpdateByQuery_InvalidScript(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	query := map[string]interface{}{
		"match_all": map[string]interface{}{},
	}
	script := make(map[string]interface{})
	script["source"] = make(chan int)
	_, err = client.UpdateByQuery(context.Background(), "test-index", query, script)
	if err == nil {
		t.Error("UpdateByQuery() with unmarshallable script should return error")
	}
}

func TestClose_NilClient(t *testing.T) {
	client := &ElasticsearchClient{}

	err := client.Close()
	if err != nil {
		t.Errorf("Close() with nil client error = %v", err)
	}
}

func TestGetClient_NilClient(t *testing.T) {
	client := &ElasticsearchClient{}

	nativeClient := client.GetClient()
	if nativeClient != nil {
		t.Error("GetClient() with nil inner client should return nil")
	}
}

func TestOptions_Addresses(t *testing.T) {
	tests := []struct {
		name      string
		addresses []string
		wantLen   int
	}{
		{"single address", []string{"http://localhost:9200"}, 1},
		{"multiple addresses", []string{"http://localhost:9200", "http://localhost:9201"}, 2},
		{"empty addresses", []string{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := Options{
				Addresses: tt.addresses,
			}

			if len(opts.Addresses) != tt.wantLen {
				t.Errorf("len(Addresses) = %v, want %v", len(opts.Addresses), tt.wantLen)
			}
		})
	}
}

func TestClose_ReturnsNil(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Close() should return nil, got: %v", err)
	}
}

func TestGetClient_ReturnsClient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	nativeClient := client.GetClient()
	if nativeClient == nil {
		t.Error("GetClient() should return non-nil client")
	}
}

func TestIndex_MapBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "PUT" && r.URL.Path == "/test-index/_doc/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"_index":"test-index","_id":"doc-1","result":"created"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	body := map[string]interface{}{
		"title":   "Test Document",
		"content": "This is a test",
	}
	err = client.Index(context.Background(), "test-index", "doc-1", body)
	if err != nil {
		t.Errorf("Index() with map body error = %v", err)
	}
}

func TestGet_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "GET" && r.URL.Path == "/test-index/_doc/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"_index":"test-index","_id":"doc-1","_source":{"title":"Test Document"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	result, err := client.Get(context.Background(), "test-index", "doc-1")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}
	if result == nil {
		t.Error("Get() returned nil result")
	}
}

func TestDelete_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "DELETE" && r.URL.Path == "/test-index/_doc/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"result":"deleted"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	err = client.Delete(context.Background(), "test-index", "doc-1")
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}
}

func TestSearch_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/test-index/_search" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			body, _ := json.Marshal(map[string]interface{}{
				"hits": map[string]interface{}{
					"total": map[string]interface{}{
						"value": 1,
					},
					"hits": []map[string]interface{}{
						{
							"_source": map[string]interface{}{
								"title": "Test",
							},
						},
					},
				},
			})
			w.Write(body)
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"title": "test",
			},
		},
	}
	result, err := client.Search(context.Background(), "test-index", query)
	if err != nil {
		t.Errorf("Search() error = %v", err)
	}
	if result == nil {
		t.Error("Search() returned nil result")
	}
}

func TestBulk_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/_bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"errors":false,"items":[{"index":{"result":"created"}}]}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	body := `{"index":{"_index":"test-index","_id":"doc-1"}}
{"title":"Test"}
`
	err = client.Bulk(context.Background(), body)
	if err != nil {
		t.Errorf("Bulk() error = %v", err)
	}
}

func TestCreateIndex_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "PUT" && r.URL.Path == "/test-index" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"acknowledged":true,"shards_acknowledged":true,"index":"test-index"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	settings := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	}
	err = client.CreateIndex(context.Background(), "test-index", settings)
	if err != nil {
		t.Errorf("CreateIndex() error = %v", err)
	}
}

func TestDeleteIndex_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "DELETE" && r.URL.Path == "/test-index" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"acknowledged":true}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	err = client.DeleteIndex(context.Background(), "test-index")
	if err != nil {
		t.Errorf("DeleteIndex() error = %v", err)
	}
}

func TestExistsIndex_True(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "HEAD" && r.URL.Path == "/test-index" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	exists, err := client.ExistsIndex(context.Background(), "test-index")
	if err != nil {
		t.Errorf("ExistsIndex() error = %v", err)
	}
	if !exists {
		t.Error("ExistsIndex() should return true")
	}
}

func TestExistsIndex_False(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "HEAD" && r.URL.Path == "/test-index" {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	exists, err := client.ExistsIndex(context.Background(), "test-index")
	if err != nil {
		t.Errorf("ExistsIndex() error = %v", err)
	}
	if exists {
		t.Error("ExistsIndex() should return false")
	}
}

func TestUpdate_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/test-index/_update/doc-1" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"result":"updated"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	doc := map[string]interface{}{
		"title": "Updated Title",
	}
	err = client.Update(context.Background(), "test-index", "doc-1", doc)
	if err != nil {
		t.Errorf("Update() error = %v", err)
	}
}

func TestUpdateByQuery_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" && r.URL.Path == "/test-index/_update_by_query" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"updated":5,"batches":1}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	query := map[string]interface{}{
		"match": map[string]interface{}{
			"status": "pending",
		},
	}
	script := map[string]interface{}{
		"source": "ctx._source.status = 'processed'",
		"lang":   "painless",
	}
	result, err := client.UpdateByQuery(context.Background(), "test-index", query, script)
	if err != nil {
		t.Errorf("UpdateByQuery() error = %v", err)
	}
	if result == nil {
		t.Error("UpdateByQuery() returned nil result")
	}
}

func TestIndexWithTrace(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "PUT" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"result":"created"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
		EnableTrace: true,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	doc := map[string]interface{}{
		"title": "Test Document",
	}
	err = client.Index(context.Background(), "test-index", "doc-1", doc)
	if err != nil {
		t.Errorf("IndexWithTrace() error = %v", err)
	}
}

func TestGetWithTrace(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"_source":{"title":"Test"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
		EnableTrace: true,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	result, err := client.Get(context.Background(), "test-index", "doc-1")
	if err != nil {
		t.Errorf("GetWithTrace() error = %v", err)
	}
	if result == nil {
		t.Error("GetWithTrace() returned nil result")
	}
}

func TestDeleteWithTrace(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "DELETE" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"result":"deleted"}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
		EnableTrace: true,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	err = client.Delete(context.Background(), "test-index", "doc-1")
	if err != nil {
		t.Errorf("DeleteWithTrace() error = %v", err)
	}
}

func TestSearchWithTrace(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			body, _ := json.Marshal(map[string]interface{}{
				"hits": map[string]interface{}{
					"total": map[string]interface{}{
						"value": 1,
					},
					"hits": []map[string]interface{}{
						{
							"_source": map[string]interface{}{
								"title": "Test",
							},
						},
					},
				},
			})
			w.Write(body)
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
		EnableTrace: true,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"title": "test",
			},
		},
	}
	result, err := client.Search(context.Background(), "test-index", query)
	if err != nil {
		t.Errorf("SearchWithTrace() error = %v", err)
	}
	if result == nil {
		t.Error("SearchWithTrace() returned nil result")
	}
}

func TestBulkWithTrace(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		} else if r.Method == "POST" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"errors":false,"items":[{"index":{"result":"created"}}]}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL},
		DialTimeout: 10 * time.Second,
		EnableTrace: true,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}

	body := `{"index":{"_index":"test-index","_id":"doc-1"}}
{"title":"Test"}
`
	err = client.Bulk(context.Background(), body)
	if err != nil {
		t.Errorf("BulkWithTrace() error = %v", err)
	}
}

func TestNewElasticsearch_WithMultipleAddresses(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"test-node","cluster_name":"test-cluster","version":{"number":"8.0.0","build_date":"2023-01-01T00:00:00.000000000Z","build_snapshot":false,"lucene_version":"9.0.0"}}`))
		}
	}))
	defer ts.Close()

	opts := &Options{
		Addresses:   []string{ts.URL, "http://localhost:9201"},
		DialTimeout: 10 * time.Second,
	}

	client, err := NewElasticsearch(opts)
	if err != nil {
		t.Errorf("NewElasticsearch() with multiple addresses error = %v", err)
	}
	if client == nil {
		t.Error("NewElasticsearch() with multiple addresses returned nil client")
	}
}
