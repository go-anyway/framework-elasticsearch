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
	"fmt"
	"time"

	pkgConfig "github.com/go-anyway/framework-config"
)

// Config Elasticsearch 配置结构体（用于从配置文件创建）
type Config struct {
	Enabled      bool               `yaml:"enabled" env:"ELASTICSEARCH_ENABLED" default:"true"`
	Addresses    []string           `yaml:"addresses" env:"ELASTICSEARCH_ADDRESSES" required:"true"`
	Username     string             `yaml:"username" env:"ELASTICSEARCH_USERNAME"`
	Password     string             `yaml:"password" env:"ELASTICSEARCH_PASSWORD"`
	CloudID      string             `yaml:"cloud_id" env:"ELASTICSEARCH_CLOUD_ID"`
	APIKey       string             `yaml:"api_key" env:"ELASTICSEARCH_API_KEY"`
	EnableTLS    bool               `yaml:"enable_tls" env:"ELASTICSEARCH_ENABLE_TLS" default:"false"`
	CACert       string             `yaml:"ca_cert" env:"ELASTICSEARCH_CA_CERT"`
	DialTimeout  pkgConfig.Duration `yaml:"dial_timeout" env:"ELASTICSEARCH_DIAL_TIMEOUT" default:"30s"`
	ReadTimeout  pkgConfig.Duration `yaml:"read_timeout" env:"ELASTICSEARCH_READ_TIMEOUT" default:"30s"`
	WriteTimeout pkgConfig.Duration `yaml:"write_timeout" env:"ELASTICSEARCH_WRITE_TIMEOUT" default:"30s"`
	MaxRetries   int                `yaml:"max_retries" env:"ELASTICSEARCH_MAX_RETRIES" default:"3"`
	EnableTrace  bool               `yaml:"enable_trace" env:"ELASTICSEARCH_ENABLE_TRACE" default:"true"`
}

// Validate 验证 Elasticsearch 配置
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("elasticsearch config cannot be nil")
	}
	if !c.Enabled {
		return nil // 如果未启用，不需要验证
	}
	if len(c.Addresses) == 0 {
		return fmt.Errorf("elasticsearch addresses cannot be empty")
	}
	for i, addr := range c.Addresses {
		if addr == "" {
			return fmt.Errorf("elasticsearch addresses[%d] cannot be empty", i)
		}
	}
	return nil
}

// ToOptions 转换为 Options
func (c *Config) ToOptions() (*Options, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if !c.Enabled {
		return nil, fmt.Errorf("elasticsearch is not enabled")
	}

	dialTimeout := c.DialTimeout.Duration()
	if dialTimeout == 0 {
		dialTimeout = 30 * time.Second
	}
	readTimeout := c.ReadTimeout.Duration()
	if readTimeout == 0 {
		readTimeout = 30 * time.Second
	}
	writeTimeout := c.WriteTimeout.Duration()
	if writeTimeout == 0 {
		writeTimeout = 30 * time.Second
	}

	return &Options{
		Addresses:    c.Addresses,
		Username:     c.Username,
		Password:     c.Password,
		CloudID:      c.CloudID,
		APIKey:       c.APIKey,
		EnableTLS:    c.EnableTLS,
		CACert:       c.CACert,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		MaxRetries:   c.MaxRetries,
		EnableTrace:  c.EnableTrace,
	}, nil
}

// DialTimeoutDuration 返回 time.Duration 类型的 DialTimeout
func (c *Config) DialTimeoutDuration() time.Duration {
	return c.DialTimeout.Duration()
}

// ReadTimeoutDuration 返回 time.Duration 类型的 ReadTimeout
func (c *Config) ReadTimeoutDuration() time.Duration {
	return c.ReadTimeout.Duration()
}

// WriteTimeoutDuration 返回 time.Duration 类型的 WriteTimeout
func (c *Config) WriteTimeoutDuration() time.Duration {
	return c.WriteTimeout.Duration()
}

// Options 结构体定义了 Elasticsearch 连接器的配置选项（内部使用）
type Options struct {
	Addresses    []string      // Elasticsearch 地址列表（如 ["http://localhost:9200"]）
	Username     string        // 用户名（可选）
	Password     string        // 密码（可选）
	CloudID      string        // Elastic Cloud ID（可选）
	APIKey       string        // API Key（可选）
	EnableTLS    bool          // 是否启用 TLS
	CACert       string        // CA 证书路径（可选）
	DialTimeout  time.Duration // 连接超时
	ReadTimeout  time.Duration // 读取超时
	WriteTimeout time.Duration // 写入超时
	MaxRetries   int           // 最大重试次数
	EnableTrace  bool          // 是否启用查询追踪，用于记录查询执行时间
}
