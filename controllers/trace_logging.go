package controllers

import (
	"context"
	"fmt"
	"sync/atomic"

	rabbitmqv1beta1 "github.com/rabbitmq/cluster-operator/v2/api/v1beta1"
	"github.com/rabbitmq/cluster-operator/v2/internal/tracelogger"
)

type traceIDKey struct{}

func (r *RabbitmqClusterReconciler) nextTraceID(namespace, name string) string {
	seq := atomic.AddUint64(&r.traceCounter, 1)
	return fmt.Sprintf("%s/%s/%d", namespace, name, seq)
}

func withTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, id)
}

func traceIDFrom(ctx context.Context) string {
	if val := ctx.Value(traceIDKey{}); val != nil {
		if id, ok := val.(string); ok {
			return id
		}
	}
	return ""
}

func traceDetails(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster, extra map[string]interface{}) map[string]interface{} {
	details := map[string]interface{}{}
	if rmq != nil {
		details["name"] = rmq.Name
		details["namespace"] = rmq.Namespace
		details["generation"] = rmq.GetGeneration()
	}
	if id := traceIDFrom(ctx); id != "" {
		details["reconcileId"] = id
	}
	for k, v := range extra {
		details[k] = v
	}
	return details
}

func (r *RabbitmqClusterReconciler) logTrace(ctx context.Context, eventType, podName string, rmq *rabbitmqv1beta1.RabbitmqCluster, extra map[string]interface{}) {
	if !tracelogger.IsEnabled() {
		return
	}
	namespace := ""
	if rmq != nil {
		namespace = rmq.Namespace
	} else if extra != nil {
		if ns, ok := extra["namespace"].(string); ok {
			namespace = ns
		}
	}
	tracelogger.LogTrace(eventType, podName, namespace, traceDetails(ctx, rmq, extra))
}
