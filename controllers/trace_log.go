package controllers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"

	rabbitmqv1beta1 "github.com/rabbitmq/cluster-operator/v2/api/v1beta1"
)

type traceContextKey struct{}

type traceState struct {
	id          string
	seq         int
	reconcileID string
	namespace   string
	name        string
	generation  int64
	specFlat    map[string]any
}

var (
	traceFileOnce   sync.Once
	traceFilePath   string
	traceFile       *os.File
	traceFileErr    error
	traceWriter     *bufio.Writer
	traceFirst      bool
	traceFileMu     sync.Mutex
	traceCloseOnce  sync.Once
	traceSignalOnce sync.Once
)

func newTraceState(rmq *rabbitmqv1beta1.RabbitmqCluster, specSnapshot map[string]any) *traceState {
	reconcileID := fmt.Sprintf("%s/%s#%d", rmq.Namespace, rmq.Name, rmq.GetGeneration())
	traceID := fmt.Sprintf("%s/%s-%d", rmq.Namespace, rmq.Name, time.Now().UnixNano())
	return &traceState{
		id:          traceID,
		reconcileID: reconcileID,
		namespace:   rmq.Namespace,
		name:        rmq.Name,
		generation:  rmq.GetGeneration(),
		specFlat:    flattenModelSpec(specSnapshot),
	}
}

func withTrace(ctx context.Context, trace *traceState) context.Context {
	return context.WithValue(ctx, traceContextKey{}, trace)
}

func traceFromContext(ctx context.Context) (*traceState, bool) {
	trace, ok := ctx.Value(traceContextKey{}).(*traceState)
	return trace, ok && trace != nil
}

func emitTraceEvent(ctx context.Context, logger logr.Logger, eventType string, details map[string]any, podName string) {
	trace, ok := traceFromContext(ctx)
	if !ok {
		return
	}
	trace.seq++
	if details == nil {
		details = map[string]any{}
	}
	if _, exists := details["reconcileId"]; !exists {
		details["reconcileId"] = trace.reconcileID
	}
	if _, exists := details["traceId"]; !exists {
		details["traceId"] = trace.id
	}
	if _, exists := details["stepSeq"]; !exists {
		details["stepSeq"] = trace.seq
	}
	if _, exists := details["generation"]; !exists {
		details["generation"] = trace.generation
	}
	if trace.specFlat != nil {
		for key, value := range trace.specFlat {
			if _, exists := details[key]; !exists {
				details[key] = value
			}
		}
	}

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	event := map[string]any{
		"timestamp": timestamp,
		"ts":        timestamp,
		"level":     "info",
		"msg":       eventType,
		"trace":     true,
		"details":   details,
		"namespace": trace.namespace,
		"name":      trace.name,
		"pod":       podName,
	}

	logger.Info(eventType,
		"trace", true,
		"details", details,
		"namespace", trace.namespace,
		"name", trace.name,
		"pod", podName,
	)

	writeTraceEvent(logger, event)
}

func modelSpecFromCluster(rmq *rabbitmqv1beta1.RabbitmqCluster) map[string]any {
	plugins := pluginListFromSpec(rmq)
	advancedConfig := advancedConfigFromSpec(rmq)

	replicas := int32(0)
	if rmq.Spec.Replicas != nil {
		replicas = *rmq.Spec.Replicas
	}

	return map[string]any{
		"replicas":                  replicas,
		"tlsEnabled":                rmq.TLSEnabled(),
		"additionalPlugins":         plugins,
		"advancedConfig":            advancedConfig,
		"skipPostDeploySteps":       rmq.Spec.SkipPostDeploySteps,
		"autoEnableAllFeatureFlags": rmq.Spec.AutoEnableAllFeatureFlags,
	}
}

func flattenModelSpec(spec map[string]any) map[string]any {
	flat := map[string]any{}
	if spec == nil {
		return flat
	}

	if replicas, ok := spec["replicas"]; ok {
		flat["specReplicas"] = replicas
	}
	if tlsEnabled, ok := spec["tlsEnabled"]; ok {
		flat["tlsEnabled"] = tlsEnabled
	}
	if skipPostDeploy, ok := spec["skipPostDeploySteps"]; ok {
		flat["skipPostDeploySteps"] = skipPostDeploy
	}
	if autoEnable, ok := spec["autoEnableAllFeatureFlags"]; ok {
		flat["autoEnableAllFeatureFlags"] = autoEnable
	}

	pluginsCount := 0
	if plugins, ok := spec["additionalPlugins"].([]string); ok {
		pluginsCount = len(plugins)
		for i, plugin := range plugins {
			flat[fmt.Sprintf("additionalPlugin_%d", i)] = plugin
		}
	}
	flat["additionalPluginsCount"] = pluginsCount

	advancedCount := 0
	if advanced, ok := spec["advancedConfig"].(map[string]string); ok {
		keys := make([]string, 0, len(advanced))
		for key := range advanced {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		advancedCount = len(keys)
		for i, key := range keys {
			flat[fmt.Sprintf("advancedConfigKey_%d", i)] = key
			flat[fmt.Sprintf("advancedConfigValue_%d", i)] = advanced[key]
		}
	}
	flat["advancedConfigCount"] = advancedCount

	return flat
}

func pluginListFromSpec(rmq *rabbitmqv1beta1.RabbitmqCluster) []string {
	plugins := make([]string, 0, len(rmq.Spec.Rabbitmq.AdditionalPlugins))
	for _, p := range rmq.Spec.Rabbitmq.AdditionalPlugins {
		plugins = append(plugins, string(p))
	}
	return plugins
}

func advancedConfigFromSpec(rmq *rabbitmqv1beta1.RabbitmqCluster) map[string]string {
	advanced := map[string]string{}
	if rmq.Spec.Rabbitmq.AdvancedConfig != "" {
		advanced["advanced.config"] = rmq.Spec.Rabbitmq.AdvancedConfig
	}
	return advanced
}

func statefulSetStatusDetails(stsName string, stsNamespace string, stsAnnotations map[string]string, templateAnnotations map[string]string, specReplicas *int32, statusReplicas int32, statusReady int32, statusAvailable int32, statusCurrent int32, statusUpdated int32, currentRevision string, updateRevision string) map[string]any {
	replicas := int32(0)
	if specReplicas != nil {
		replicas = *specReplicas
	}

	return map[string]any{
		"stsName":                 stsName,
		"stsNamespace":            stsNamespace,
		"stsSpecReplicas":         replicas,
		"statusReplicas":          statusReplicas,
		"statusReadyReplicas":     statusReady,
		"statusAvailableReplicas": statusAvailable,
		"statusCurrentReplicas":   statusCurrent,
		"statusUpdatedReplicas":   statusUpdated,
		"currentReplicas":         statusCurrent,
		"readyReplicas":           statusReady,
		"availableReplicas":       statusAvailable,
		"updatedReplicas":         statusUpdated,
		"statusCurrentRevision":   currentRevision,
		"statusUpdateRevision":    updateRevision,
		"stsAnnotations":          stsAnnotations,
		"podTemplateAnnotations":  templateAnnotations,
	}
}

func filterAnnotations(input map[string]string, keys ...string) map[string]string {
	if len(keys) == 0 {
		return map[string]string{}
	}
	out := map[string]string{}
	for _, key := range keys {
		if input == nil {
			continue
		}
		if val, ok := input[key]; ok {
			out[key] = val
		}
	}
	return out
}

func keysOfStringMap(input map[string]string) []string {
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	return keys
}

func writeTraceEvent(logger logr.Logger, event map[string]any) {
	ensureTraceFile(logger)
	if traceFile == nil {
		return
	}

	traceFileMu.Lock()
	defer traceFileMu.Unlock()

	if traceWriter == nil {
		return
	}

	if !traceFirst {
		if _, err := traceWriter.WriteString(",\n"); err != nil {
			logger.Error(err, "failed to write trace separator", "tracePath", traceFilePath)
			return
		}
	}
	traceFirst = false

	encoder := json.NewEncoder(traceWriter)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(event); err != nil {
		logger.Error(err, "failed to write trace event", "tracePath", traceFilePath)
		return
	}
	if err := traceWriter.Flush(); err != nil {
		logger.Error(err, "failed to flush trace file", "tracePath", traceFilePath)
	}
}

func ensureTraceFile(logger logr.Logger) {
	traceFileOnce.Do(func() {
		traceFilePath = os.Getenv("TRACE_LOG_PATH")
		if traceFilePath == "" {
			traceFilePath = fmt.Sprintf("/tmp/profile/operator-trace-%s.json", time.Now().Format("20060102-150405.000"))
		}

		f, err := os.OpenFile(traceFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			traceFileErr = err
			logger.Error(err, "failed to open trace log file", "tracePath", traceFilePath)
			return
		}
		traceFile = f
		traceWriter = bufio.NewWriter(traceFile)
		traceFirst = true
		if _, err := traceWriter.WriteString("{\"events\":[\n"); err != nil {
			traceFileErr = err
			logger.Error(err, "failed to initialize trace file", "tracePath", traceFilePath)
			return
		}
		if err := traceWriter.Flush(); err != nil {
			traceFileErr = err
			logger.Error(err, "failed to flush trace file", "tracePath", traceFilePath)
			return
		}
		startTraceSignalHandler(logger)
		logger.Info("trace file initialized", "tracePath", traceFilePath)
	})

	if traceFileErr != nil {
		logger.Error(traceFileErr, "trace file unavailable", "tracePath", traceFilePath)
	}
}

func startTraceSignalHandler(logger logr.Logger) {
	traceSignalOnce.Do(func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-signals
			closeTraceFile(logger)
		}()
	})
}

func closeTraceFile(logger logr.Logger) {
	traceCloseOnce.Do(func() {
		traceFileMu.Lock()
		defer traceFileMu.Unlock()
		if traceWriter != nil {
			if _, err := traceWriter.WriteString("]}\n"); err != nil {
				logger.Error(err, "failed to finalize trace file", "tracePath", traceFilePath)
			}
			if err := traceWriter.Flush(); err != nil {
				logger.Error(err, "failed to flush trace file", "tracePath", traceFilePath)
			}
		}
		if traceFile != nil {
			if err := traceFile.Close(); err != nil {
				logger.Error(err, "failed to close trace file", "tracePath", traceFilePath)
			}
		}
	})
}
