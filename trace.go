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
	"time"

	"github.com/go-anyway/framework-log"
	pkgtrace "github.com/go-anyway/framework-trace"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// executeWithTrace 带追踪的操作执行包装器
func executeWithTrace(
	ctx context.Context,
	operation string,
	index string,
	documentID string,
	enableTrace bool,
	handler func(context.Context) error,
) error {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "elasticsearch.operation",
			trace.WithAttributes(
				attribute.String("db.system", "elasticsearch"),
				attribute.String("db.name", index),
				attribute.String("db.operation", operation),
				attribute.String("db.document_id", documentID),
			),
		)
		defer span.End()
	}

	// 执行操作
	err := handler(ctx)
	duration := time.Since(startTime)

	// 处理结果
	if err != nil {
		log.FromContext(ctx).Error("Elasticsearch operation failed",
			zap.String("operation", operation),
			zap.String("index", index),
			zap.String("document_id", documentID),
			zap.Duration("duration", duration),
			zap.Error(err),
		)

		// 更新追踪状态
		if enableTrace && span != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
			span.SetAttributes(
				attribute.String("db.status", "error"),
				attribute.String("db.error", err.Error()),
			)
		}
	} else {
		log.FromContext(ctx).Info("Elasticsearch operation success",
			zap.String("operation", operation),
			zap.String("index", index),
			zap.String("document_id", documentID),
			zap.Duration("duration", duration),
		)

		// 更新追踪状态
		if enableTrace && span != nil {
			span.SetStatus(codes.Ok, "")
			span.SetAttributes(
				attribute.String("db.status", "success"),
				attribute.Float64("db.duration_ms", float64(duration.Milliseconds())),
			)
		}
	}

	return err
}

// queryWithTrace 带追踪的查询操作包装器
func queryWithTrace(
	ctx context.Context,
	operation string,
	index string,
	enableTrace bool,
	handler func(context.Context) (map[string]interface{}, error),
) (map[string]interface{}, error) {
	startTime := time.Now()
	var zero map[string]interface{}

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "elasticsearch.operation",
			trace.WithAttributes(
				attribute.String("db.system", "elasticsearch"),
				attribute.String("db.name", index),
				attribute.String("db.operation", operation),
			),
		)
		defer span.End()
	}

	// 执行操作
	result, err := handler(ctx)
	duration := time.Since(startTime)

	// 处理结果
	if err != nil {
		log.FromContext(ctx).Error("Elasticsearch operation failed",
			zap.String("operation", operation),
			zap.String("index", index),
			zap.Duration("duration", duration),
			zap.Error(err),
		)

		// 更新追踪状态
		if enableTrace && span != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
			span.SetAttributes(
				attribute.String("db.status", "error"),
				attribute.String("db.error", err.Error()),
			)
		}

		return zero, err
	}

	log.FromContext(ctx).Info("Elasticsearch operation success",
		zap.String("operation", operation),
		zap.String("index", index),
		zap.Duration("duration", duration),
	)

	// 更新追踪状态
	if enableTrace && span != nil {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(
			attribute.String("db.status", "success"),
			attribute.Float64("db.duration_ms", float64(duration.Milliseconds())),
		)
	}

	return result, nil
}
