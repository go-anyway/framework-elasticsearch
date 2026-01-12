// Copyright 2025 zampo.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// @contact  zampo3380@gmail.com

package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// ElasticsearchClient Elasticsearch 客户端接口
type ElasticsearchClient struct {
	client      *elasticsearch.Client
	EnableTrace bool // 是否启用追踪
}

// NewElasticsearch 根据给定的选项创建一个新的 Elasticsearch 客户端实例
func NewElasticsearch(opts *Options) (*ElasticsearchClient, error) {
	if opts == nil {
		return nil, fmt.Errorf("elasticsearch options cannot be nil")
	}

	if len(opts.Addresses) == 0 {
		return nil, fmt.Errorf("elasticsearch addresses cannot be empty")
	}

	// 构建配置
	cfg := elasticsearch.Config{
		Addresses: opts.Addresses,
	}

	// 设置认证
	if opts.Username != "" && opts.Password != "" {
		cfg.Username = opts.Username
		cfg.Password = opts.Password
	} else if opts.APIKey != "" {
		cfg.APIKey = opts.APIKey
	}

	// 设置 Cloud ID
	if opts.CloudID != "" {
		cfg.CloudID = opts.CloudID
	}

	// 注意：elasticsearch 客户端的超时配置需要通过 Transport 设置
	// 这里我们使用默认的 Transport，超时配置在请求级别处理

	// 设置最大重试次数
	if opts.MaxRetries > 0 {
		cfg.MaxRetries = opts.MaxRetries
	} else {
		cfg.MaxRetries = 3 // 默认重试 3 次
	}

	// 如果启用了追踪，则添加追踪功能
	// 追踪功能在 elasticsearch_trace.go 中实现
	_ = opts.EnableTrace // 避免空分支警告

	// 创建客户端
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
	defer cancel()
	res, err := client.Info(client.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch info error: %s", res.String())
	}

	esClient := &ElasticsearchClient{
		client:      client,
		EnableTrace: opts.EnableTrace,
	}

	return esClient, nil
}

// Close 关闭 Elasticsearch 客户端连接
func (c *ElasticsearchClient) Close() error {
	// Elasticsearch 客户端不需要显式关闭
	// 但我们可以在这里做一些清理工作
	return nil
}

// GetClient 获取原生客户端（用于高级操作）
func (c *ElasticsearchClient) GetClient() *elasticsearch.Client {
	return c.client
}

// IsConnected 检查连接是否正常
func (c *ElasticsearchClient) IsConnected() bool {
	if c.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Ping(ctx) == nil
}

// Ping 检查连接是否正常
func (c *ElasticsearchClient) Ping(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("elasticsearch client is not initialized")
	}
	res, err := c.client.Ping(c.client.Ping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to ping elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch ping error: %s", res.String())
	}

	return nil
}

// Index 索引文档（自动处理追踪）
func (c *ElasticsearchClient) Index(ctx context.Context, index string, documentID string, body interface{}) error {
	return executeWithTrace(
		ctx,
		"index",
		index,
		documentID,
		c.EnableTrace,
		func(ctx context.Context) error {
			return c.index(ctx, index, documentID, body)
		},
	)
}

// index 内部索引文档方法
func (c *ElasticsearchClient) index(ctx context.Context, index string, documentID string, body interface{}) error {
	var bodyBytes []byte
	var err error

	switch v := body.(type) {
	case string:
		bodyBytes = []byte(v)
	case []byte:
		bodyBytes = v
	default:
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}
	}

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: documentID,
		Body:       strings.NewReader(string(bodyBytes)),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to index document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch index error: %s", res.String())
	}

	return nil
}

// Get 获取文档（自动处理追踪）
func (c *ElasticsearchClient) Get(ctx context.Context, index string, documentID string) (map[string]interface{}, error) {
	return queryWithTrace(
		ctx,
		"get",
		index,
		c.EnableTrace,
		func(ctx context.Context) (map[string]interface{}, error) {
			return c.get(ctx, index, documentID)
		},
	)
}

// get 内部获取文档方法
func (c *ElasticsearchClient) get(ctx context.Context, index string, documentID string) (map[string]interface{}, error) {
	req := esapi.GetRequest{
		Index:      index,
		DocumentID: documentID,
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, fmt.Errorf("document not found")
		}
		return nil, fmt.Errorf("elasticsearch get error: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

// Delete 删除文档（自动处理追踪）
func (c *ElasticsearchClient) Delete(ctx context.Context, index string, documentID string) error {
	return executeWithTrace(
		ctx,
		"delete",
		index,
		documentID,
		c.EnableTrace,
		func(ctx context.Context) error {
			return c.delete(ctx, index, documentID)
		},
	)
}

// delete 内部删除文档方法
func (c *ElasticsearchClient) delete(ctx context.Context, index string, documentID string) error {
	req := esapi.DeleteRequest{
		Index:      index,
		DocumentID: documentID,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return fmt.Errorf("document not found")
		}
		return fmt.Errorf("elasticsearch delete error: %s", res.String())
	}

	return nil
}

// Search 搜索文档（自动处理追踪）
func (c *ElasticsearchClient) Search(ctx context.Context, index string, query map[string]interface{}) (map[string]interface{}, error) {
	return queryWithTrace(
		ctx,
		"search",
		index,
		c.EnableTrace,
		func(ctx context.Context) (map[string]interface{}, error) {
			return c.search(ctx, index, query)
		},
	)
}

// executeQueryRequest 执行查询请求的通用方法
func (c *ElasticsearchClient) executeQueryRequest(ctx context.Context, index string, query map[string]interface{}, reqFunc func([]string, *strings.Reader) esapi.Request, operation string) (map[string]interface{}, error) {
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	req := reqFunc([]string{index}, strings.NewReader(string(queryBytes)))

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to %s: %w", operation, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch %s error: %s", operation, res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

// search 内部搜索文档方法
func (c *ElasticsearchClient) search(ctx context.Context, index string, query map[string]interface{}) (map[string]interface{}, error) {
	return c.executeQueryRequest(ctx, index, query, func(indices []string, body *strings.Reader) esapi.Request {
		return esapi.SearchRequest{
			Index: indices,
			Body:  body,
		}
	}, "search")
}

// Bulk 批量操作（自动处理追踪）
func (c *ElasticsearchClient) Bulk(ctx context.Context, body string) error {
	return executeWithTrace(
		ctx,
		"bulk",
		"",
		"",
		c.EnableTrace,
		func(ctx context.Context) error {
			return c.bulk(ctx, body)
		},
	)
}

// bulk 内部批量操作方法
func (c *ElasticsearchClient) bulk(ctx context.Context, body string) error {
	req := esapi.BulkRequest{
		Body:    strings.NewReader(body),
		Refresh: "true",
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to bulk: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch bulk error: %s", res.String())
	}

	return nil
}

// CreateIndex 创建索引
func (c *ElasticsearchClient) CreateIndex(ctx context.Context, index string, settings map[string]interface{}) error {
	settingsBytes, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}

	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  strings.NewReader(string(settingsBytes)),
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch create index error: %s", res.String())
	}

	return nil
}

// DeleteIndex 删除索引
func (c *ElasticsearchClient) DeleteIndex(ctx context.Context, index string) error {
	req := esapi.IndicesDeleteRequest{
		Index: []string{index},
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch delete index error: %s", res.String())
	}

	return nil
}

// ExistsIndex 检查索引是否存在
func (c *ElasticsearchClient) ExistsIndex(ctx context.Context, index string) (bool, error) {
	req := esapi.IndicesExistsRequest{
		Index: []string{index},
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return false, fmt.Errorf("failed to check index: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return false, nil
	}

	if res.IsError() {
		return false, fmt.Errorf("elasticsearch exists index error: %s", res.String())
	}

	return true, nil
}

// Update 更新文档
func (c *ElasticsearchClient) Update(ctx context.Context, index string, documentID string, body interface{}) error {
	var bodyBytes []byte
	var err error

	switch v := body.(type) {
	case string:
		bodyBytes = []byte(v)
	case []byte:
		bodyBytes = v
	default:
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}
	}

	// 构建更新请求体（需要包装在 doc 字段中）
	updateBody := map[string]interface{}{
		"doc": json.RawMessage(bodyBytes),
	}
	updateBodyBytes, err := json.Marshal(updateBody)
	if err != nil {
		return fmt.Errorf("failed to marshal update body: %w", err)
	}

	req := esapi.UpdateRequest{
		Index:      index,
		DocumentID: documentID,
		Body:       strings.NewReader(string(updateBodyBytes)),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to update document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return fmt.Errorf("document not found")
		}
		return fmt.Errorf("elasticsearch update error: %s", res.String())
	}

	return nil
}

// UpdateByQuery 根据查询更新文档
func (c *ElasticsearchClient) UpdateByQuery(ctx context.Context, index string, query map[string]interface{}, script map[string]interface{}) (map[string]interface{}, error) {
	// 构建更新查询请求体
	updateQuery := map[string]interface{}{
		"query": query,
	}
	if script != nil {
		updateQuery["script"] = script
	}

	queryBytes, err := json.Marshal(updateQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal update query: %w", err)
	}

	req := esapi.UpdateByQueryRequest{
		Index: []string{index},
		Body:  strings.NewReader(string(queryBytes)),
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to update by query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch update by query error: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

// Count 统计文档数量
func (c *ElasticsearchClient) Count(ctx context.Context, index string, query map[string]interface{}) (int64, error) {
	var queryBytes []byte
	var err error

	if query != nil {
		queryBytes, err = json.Marshal(query)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal query: %w", err)
		}
	}

	req := esapi.CountRequest{
		Index: []string{index},
	}
	if len(queryBytes) > 0 {
		req.Body = strings.NewReader(string(queryBytes))
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return 0, fmt.Errorf("failed to count: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("elasticsearch count error: %s", res.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	// 提取 count 值
	if count, ok := result["count"].(float64); ok {
		return int64(count), nil
	}

	return 0, fmt.Errorf("invalid count response format")
}

// DeleteByQuery 根据查询删除文档
func (c *ElasticsearchClient) DeleteByQuery(ctx context.Context, index string, query map[string]interface{}) (map[string]interface{}, error) {
	return c.executeQueryRequest(ctx, index, query, func(indices []string, body *strings.Reader) esapi.Request {
		return esapi.DeleteByQueryRequest{
			Index: indices,
			Body:  body,
		}
	}, "delete by query")
}
