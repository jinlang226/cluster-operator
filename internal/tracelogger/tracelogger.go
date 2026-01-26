package tracelogger

// Trace Logger for P Model Validation
//
// This package provides structured trace logging for the RabbitMQ Cluster Operator.
// Trace events are logged in JSON format and can be validated against P specifications.
//
// Usage:
//   1. Initialize trace logging in main.go:
//      tracelogger.InitTraceLogging("/tmp/operator-trace.json")
//   2. Log events in reconciliation code:
//      tracelogger.LogTrace("PodScheduled", podName, namespace, nil)
//   3. Close the logger on shutdown:
//      defer tracelogger.CloseTraceLogging()

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// TraceEvent represents a single operator event for P model validation.
type TraceEvent struct {
	Timestamp string                 `json:"timestamp"` // RFC3339Nano
	Level     string                 `json:"level"`     // Always "info"
	Msg       string                 `json:"msg"`       // Event type (e.g., "PodScheduled")
	Namespace string                 `json:"namespace"`
	Pod       string                 `json:"pod,omitempty"`
	Name      string                 `json:"name,omitempty"` // Optional cluster name
	Details   map[string]interface{} `json:"details,omitempty"`
}

// TraceLogger manages trace event logging.
type TraceLogger struct {
	mu       sync.Mutex
	enabled  bool
	encoder  *json.Encoder
	file     *os.File
	eventLog []TraceEvent // In-memory buffer for testing/debugging
}

var (
	once         sync.Once
	globalLogger *TraceLogger
)

// InitTraceLogging initializes the global trace logger.
// Set filepath to "memory" to use in-memory buffer (for testing).
// Set filepath to "" to disable trace logging.
func InitTraceLogging(filepath string) error {
	var initErr error
	once.Do(func() {
		globalLogger = &TraceLogger{
			enabled:  filepath != "",
			eventLog: make([]TraceEvent, 0, 1000),
		}

		if filepath == "" {
			return
		}
		if filepath == "memory" {
			return
		}

		var err error
		globalLogger.file, err = os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			globalLogger.enabled = false
			initErr = err
			return
		}

		fmt.Fprintf(globalLogger.file, "{\"events\":[\n")
		globalLogger.encoder = json.NewEncoder(globalLogger.file)
	})
	return initErr
}

// IsEnabled returns whether trace logging is enabled.
func IsEnabled() bool {
	return globalLogger != nil && globalLogger.enabled
}

// LogTrace logs a trace event.
func LogTrace(eventType, podName, namespace string, details map[string]interface{}) {
	if globalLogger == nil || !globalLogger.enabled {
		return
	}

	enrichedDetails := map[string]interface{}{}
	for k, v := range details {
		enrichedDetails[k] = v
	}
	if path, line, fn := callerInfo(); path != "" {
		relPath := trimToRepoRoot(path)
		enrichedDetails["sourcePath"] = relPath
		enrichedDetails["sourceFile"] = filepath.Base(relPath)
		enrichedDetails["sourceLine"] = line
		if fn != "" {
			enrichedDetails["sourceFunc"] = fn
		}
	}

	event := TraceEvent{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     "info",
		Msg:       eventType,
		Namespace: namespace,
		Pod:       podName,
		Details:   enrichedDetails,
	}

	if enrichedDetails != nil {
		if name, ok := enrichedDetails["name"].(string); ok && name != "" {
			event.Name = name
		} else if cluster, ok := enrichedDetails["cluster"].(string); ok && cluster != "" {
			event.Name = cluster
		}
	}

	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	globalLogger.eventLog = append(globalLogger.eventLog, event)

	if globalLogger.encoder != nil {
		if len(globalLogger.eventLog) > 1 {
			fmt.Fprintf(globalLogger.file, ",\n")
		}
		_ = globalLogger.encoder.Encode(event)
	}
}

func callerInfo() (string, int, string) {
	pcs := make([]uintptr, 16)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if !isTraceLoggerFrame(frame) {
			return frame.File, frame.Line, frame.Function
		}
		if !more {
			break
		}
	}
	return "", 0, ""
}

func trimToRepoRoot(path string) string {
	normalized := filepath.ToSlash(path)
	const marker = "/cluster-operator/"
	if idx := strings.LastIndex(normalized, marker); idx != -1 {
		return normalized[idx+len(marker):]
	}
	return path
}

func isTraceLoggerFrame(frame runtime.Frame) bool {
	path := filepath.ToSlash(frame.File)
	if strings.Contains(path, "/internal/tracelogger/") {
		return true
	}
	if strings.HasSuffix(path, "/controllers/trace_logging.go") {
		return true
	}
	return false
}

// CloseTraceLogging closes the trace log file.
func CloseTraceLogging() error {
	if globalLogger == nil || !globalLogger.enabled || globalLogger.file == nil {
		return nil
	}

	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	if globalLogger.file == nil {
		return nil
	}

	fmt.Fprintf(globalLogger.file, "]}\n")
	err := globalLogger.file.Close()
	globalLogger.file = nil
	globalLogger.encoder = nil
	globalLogger.enabled = false
	return err
}

// GetEvents returns all logged events (for testing).
func GetEvents() []TraceEvent {
	if globalLogger == nil {
		return nil
	}
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	events := make([]TraceEvent, len(globalLogger.eventLog))
	copy(events, globalLogger.eventLog)
	return events
}

// ResetEvents clears the in-memory event log (for testing).
func ResetEvents() {
	if globalLogger == nil {
		return
	}
	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()
	globalLogger.eventLog = globalLogger.eventLog[:0]
}

// Convenience functions for common event types.
func LogPodScheduled(podName, namespace string) {
	LogTrace("PodScheduled", podName, namespace, nil)
}

func LogPodReady(podName, namespace string, ip string) {
	LogTrace("PodReady", podName, namespace, map[string]interface{}{"ip": ip})
}

func LogPodDeleted(podName, namespace string) {
	LogTrace("PodDeleted", podName, namespace, nil)
}

func LogPodCrashed(podName, namespace string, reason string) {
	LogTrace("PodCrashed", podName, namespace, map[string]interface{}{"reason": reason})
}

func LogPodRestarted(podName, namespace string) {
	LogTrace("PodRestarted", podName, namespace, nil)
}

func LogHealthCheck(podName, namespace string, healthy bool) {
	LogTrace("HealthCheck", podName, namespace, map[string]interface{}{"healthy": healthy})
}

func LogEnableFeatureFlags(podName, namespace string) {
	LogTrace("EnableFeatureFlags", podName, namespace, nil)
}

func LogSetPlugins(podName, namespace string, plugins []string) {
	LogTrace("SetPlugins", podName, namespace, map[string]interface{}{"plugins": plugins})
}

func LogQueueRebalance(podName, namespace string) {
	LogTrace("QueueRebalance", podName, namespace, nil)
}

func LogStatefulSetCreated(namespace string, replicas int32) {
	LogTrace("StatefulSetCreated", "", namespace, map[string]interface{}{"replicas": replicas})
}

func LogStatefulSetScaled(namespace string, oldReplicas, newReplicas int32) {
	LogTrace("StatefulSetScaled", "", namespace, map[string]interface{}{
		"oldReplicas": oldReplicas,
		"newReplicas": newReplicas,
	})
}
