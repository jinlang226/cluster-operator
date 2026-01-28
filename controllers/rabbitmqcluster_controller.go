/*
RabbitMQ Cluster Operator

Copyright 2020 VMware, Inc. All Rights Reserved.

This product is licensed to you under the Mozilla Public license, Version 2.0 (the "License").  You may not use this product except in compliance with the Mozilla Public License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/rabbitmq/cluster-operator/v2/internal/metadata"
	"github.com/rabbitmq/cluster-operator/v2/internal/resource"
	"github.com/rabbitmq/cluster-operator/v2/internal/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	clientretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"k8s.io/apimachinery/pkg/runtime"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rabbitmqv1beta1 "github.com/rabbitmq/cluster-operator/v2/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	apiGVStr = rabbitmqv1beta1.GroupVersion.String()
)

const (
	ownerKey                 = ".metadata.controller"
	ownerKind                = "RabbitmqCluster"
	pauseReconciliationLabel = "rabbitmq.com/pauseReconciliation"
)

// RabbitmqClusterReconciler reconciles a RabbitmqCluster object
type RabbitmqClusterReconciler struct {
	client.Client
	APIReader               client.Reader
	Scheme                  *runtime.Scheme
	Namespace               string
	Recorder                record.EventRecorder
	ClusterConfig           *rest.Config
	Clientset               *kubernetes.Clientset
	PodExecutor             PodExecutor
	DefaultRabbitmqImage    string
	DefaultUserUpdaterImage string
	DefaultImagePullSecrets string
	ControlRabbitmqImage    bool
	traceCounter            uint64
}

// the rbac rule requires an empty row at the end to render
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=pods,verbs=update;get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=rabbitmq.com,resources=rabbitmqclusters,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=rabbitmq.com,resources=rabbitmqclusters/status,verbs=get;update
// +kubebuilder:rbac:groups=rabbitmq.com,resources=rabbitmqclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=get;create;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=roles,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="discovery.k8s.io",resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=endpoints,verbs=get;watch;list
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=rolebindings,verbs=get;list;watch;create;update

func (r *RabbitmqClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	traceID := r.nextTraceID(req.Namespace, req.Name)
	ctx = withTraceID(ctx, traceID)
	logger := ctrl.LoggerFrom(ctx)

	rabbitmqCluster, err := r.getRabbitmqCluster(ctx, req.NamespacedName)

	if client.IgnoreNotFound(err) != nil {
		r.logTrace(ctx, "ReconcileError", "", nil, map[string]interface{}{
			"namespace": req.Namespace,
			"name":      req.Name,
			"error":     err.Error(),
		})
		return ctrl.Result{}, err
	} else if k8serrors.IsNotFound(err) {
		// No need to requeue if the resource no longer exists
		r.logTrace(ctx, "ReconcileNotFound", "", nil, map[string]interface{}{
			"namespace": req.Namespace,
			"name":      req.Name,
		})
		return ctrl.Result{}, nil
	}

	r.logTrace(ctx, "ReconcileStart", "", rabbitmqCluster, map[string]interface{}{
		"request": req.NamespacedName.String(),
	})
	specDetails := map[string]interface{}{
		"specReplicas":              derefReplicas(rabbitmqCluster.Spec.Replicas),
		"image":                     rabbitmqCluster.Spec.Image,
		"serviceType":               string(rabbitmqCluster.Spec.Service.Type),
		"skipPostDeploySteps":       rabbitmqCluster.Spec.SkipPostDeploySteps,
		"autoEnableAllFeatureFlags": rabbitmqCluster.Spec.AutoEnableAllFeatureFlags,
		"disableNonTLSListeners":    rabbitmqCluster.DisableNonTLSListeners(),
		"tlsEnabled":                rabbitmqCluster.TLSEnabled(),
		"secretTLSEnabled":          rabbitmqCluster.SecretTLSEnabled(),
		"mutualTLSEnabled":          rabbitmqCluster.MutualTLSEnabled(),
		"vaultEnabled":              rabbitmqCluster.VaultEnabled(),
		"externalSecretEnabled":     rabbitmqCluster.ExternalSecretEnabled(),
	}
	if rabbitmqCluster.Spec.Persistence.Storage != nil {
		specDetails["persistenceStorage"] = rabbitmqCluster.Spec.Persistence.Storage.String()
	}
	if rabbitmqCluster.Spec.Persistence.StorageClassName != nil {
		specDetails["persistenceStorageClass"] = *rabbitmqCluster.Spec.Persistence.StorageClassName
	}
	if rabbitmqCluster.Spec.Persistence.EmptyDir != nil {
		specDetails["persistenceEmptyDirMedium"] = string(rabbitmqCluster.Spec.Persistence.EmptyDir.Medium)
		if rabbitmqCluster.Spec.Persistence.EmptyDir.SizeLimit != nil {
			specDetails["persistenceEmptyDirSizeLimit"] = rabbitmqCluster.Spec.Persistence.EmptyDir.SizeLimit.String()
		}
	}
	if rabbitmqCluster.Spec.TerminationGracePeriodSeconds != nil {
		specDetails["terminationGracePeriodSeconds"] = *rabbitmqCluster.Spec.TerminationGracePeriodSeconds
	}
	if rabbitmqCluster.Spec.DelayStartSeconds != nil {
		specDetails["delayStartSeconds"] = *rabbitmqCluster.Spec.DelayStartSeconds
	}
	if rabbitmqCluster.Spec.TLS.SecretName != "" {
		specDetails["tlsSecretName"] = rabbitmqCluster.Spec.TLS.SecretName
	}
	if rabbitmqCluster.Spec.TLS.CaSecretName != "" {
		specDetails["tlsCaSecretName"] = rabbitmqCluster.Spec.TLS.CaSecretName
	}
	if len(rabbitmqCluster.Spec.Rabbitmq.AdditionalPlugins) > 0 {
		specDetails["additionalPlugins"] = rabbitmqCluster.Spec.Rabbitmq.AdditionalPlugins
	}
	r.logTrace(ctx, "SpecObserved", "", rabbitmqCluster, specDetails)

	// Check if the resource has been marked for deletion
	if !rabbitmqCluster.DeletionTimestamp.IsZero() {
		logger.Info("Deleting")
		r.logTrace(ctx, "ReconcileDeletion", "", rabbitmqCluster, nil)
		return ctrl.Result{}, r.prepareForDeletion(ctx, rabbitmqCluster)
	}

	// exit if pause reconciliation label is set to true
	if v, ok := rabbitmqCluster.Labels[pauseReconciliationLabel]; ok && v == "true" {
		logger.Info("Not reconciling RabbitmqCluster")
		r.Recorder.Event(rabbitmqCluster, corev1.EventTypeWarning,
			"PausedReconciliation", fmt.Sprintf("label '%s' is set to true", pauseReconciliationLabel))

		r.logTrace(ctx, "ReconcilePaused", "", rabbitmqCluster, map[string]interface{}{
			"label": pauseReconciliationLabel,
		})

		rabbitmqCluster.Status.SetCondition(status.NoWarnings, corev1.ConditionFalse, "reconciliation paused")
		if writerErr := r.Status().Update(ctx, rabbitmqCluster); writerErr != nil {
			logger.Error(writerErr, "Error trying to Update NoWarnings condition state")
		}
		return ctrl.Result{}, nil
	}

	if requeueAfter, err := r.reconcileOperatorDefaults(ctx, rabbitmqCluster); err != nil || requeueAfter > 0 {
		if err != nil {
			r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
				"stage": "operatorDefaults",
				"error": err.Error(),
			})
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}

	// Ensure the resource have a deletion marker
	if err := r.addFinalizerIfNeeded(ctx, rabbitmqCluster); err != nil {
		r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
			"stage": "addFinalizer",
			"error": err.Error(),
		})
		return ctrl.Result{}, err
	}

	if requeueAfter, err := r.updateStatusConditions(ctx, rabbitmqCluster); err != nil || requeueAfter > 0 {
		if err != nil {
			r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
				"stage": "updateStatusConditions",
				"error": err.Error(),
			})
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}

	tlsErr := r.reconcileTLS(ctx, rabbitmqCluster)
	if errors.Is(tlsErr, errDisableNonTLSConfig) {
		return ctrl.Result{}, nil
	} else if tlsErr != nil {
		r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
			"stage": "reconcileTLS",
			"error": tlsErr.Error(),
		})
		return ctrl.Result{}, tlsErr
	}

	// if the secret already exists, ensure it has the labels necessary for being in the controller's cache
	// otherwise, our attempt to create it will fail (CreateOrUpdate only checks for the existence of the resource in the cache)
	defaultUserSecret := &corev1.Secret{}
	err = r.APIReader.Get(ctx, types.NamespacedName{Namespace: rabbitmqCluster.Namespace, Name: rabbitmqCluster.ChildResourceName("default-user")}, defaultUserSecret)
	if err == nil {
		if v, ok := defaultUserSecret.Labels["app.kubernetes.io/part-of"]; !ok || v != "rabbitmq" {
			defaultUserSecret.Labels = metadata.GetLabels(rabbitmqCluster.Name, rabbitmqCluster.Labels)
			_ = r.Update(ctx, defaultUserSecret)
		}
	}

	sts, err := r.statefulSet(ctx, rabbitmqCluster)
	// The StatefulSet may not have been created by this point, so ignore Not Found errors
	if client.IgnoreNotFound(err) != nil {
		r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
			"stage": "getStatefulSet",
			"error": err.Error(),
		})
		return ctrl.Result{}, err
	}
	if sts != nil {
		details := map[string]interface{}{
			"resourceName":      sts.Name,
			"statusReplicas":    sts.Status.Replicas,
			"currentReplicas":   sts.Status.CurrentReplicas,
			"readyReplicas":     sts.Status.ReadyReplicas,
			"availableReplicas": sts.Status.AvailableReplicas,
			"updatedReplicas":   sts.Status.UpdatedReplicas,
			"currentRevision":   sts.Status.CurrentRevision,
			"updateRevision":    sts.Status.UpdateRevision,
			"resourceVersion":   sts.ResourceVersion,
		}
		if sts.Spec.Replicas != nil {
			details["specReplicas"] = *sts.Spec.Replicas
		}
		r.logTrace(ctx, "StatefulSetStatusObserved", "", rabbitmqCluster, details)
	} else if k8serrors.IsNotFound(err) {
		r.logTrace(ctx, "StatefulSetNotFound", "", rabbitmqCluster, map[string]interface{}{
			"resourceName": rabbitmqCluster.ChildResourceName("server"),
		})
	}
	if sts != nil && statefulSetNeedsQueueRebalance(sts, rabbitmqCluster) {
		r.logTrace(ctx, "QueueRebalanceNeeded", "", rabbitmqCluster, map[string]interface{}{
			"reason":          "statefulSetBeingUpdated",
			"currentRevision": sts.Status.CurrentRevision,
			"updateRevision":  sts.Status.UpdateRevision,
		})
		if err := r.markForQueueRebalance(ctx, rabbitmqCluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("Start reconciling")

	// FIXME: marshalling is expensive. We are marshalling only for the sake of logging.
	// 	Implement Stringer interface instead
	instanceSpec, err := json.Marshal(rabbitmqCluster.Spec)
	if err != nil {
		logger.Error(err, "Failed to marshal cluster spec")
	}
	logger.V(1).Info("RabbitmqCluster", "spec", string(instanceSpec))

	resourceBuilder := resource.RabbitmqResourceBuilder{
		Instance: rabbitmqCluster,
		Scheme:   r.Scheme,
	}

	builders := resourceBuilder.ResourceBuilders()

	for _, builder := range builders {
		resource, err := builder.Build()
		if err != nil {
			return ctrl.Result{}, err
		}

		// only StatefulSetBuilder returns true
		if builder.UpdateMayRequireStsRecreate() {
			sts := resource.(*appsv1.StatefulSet)

			current, err := r.statefulSet(ctx, rabbitmqCluster)
			if client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}

			// only checks for scale down if statefulSet is created
			// else continue to CreateOrUpdate()
			if !k8serrors.IsNotFound(err) {
				if err := builder.Update(sts); err != nil {
					return ctrl.Result{}, err
				}
				if ScaleToZero(current, sts) {
					err := r.saveReplicasBeforeZero(ctx, rabbitmqCluster, current)
					if err != nil {
						return ctrl.Result{}, err
					}
				} else {
					if r.scaleDown(ctx, rabbitmqCluster, current, sts) {
						// return when cluster scale down detected; unsupported operation
						return ctrl.Result{}, nil
					}
				}
				if ScaleFromZero(current, sts) {
					if r.scaleFromZeroToBeforeReplicasConfigured(ctx, rabbitmqCluster, sts) {
						// return when cluster scale down from zero detected; unsupported operation
						return ctrl.Result{}, nil
					}
					r.removeReplicasBeforeZeroAnnotationIfExists(ctx, rabbitmqCluster)
				}
			}

			// The PVCs for the StatefulSet may require expanding
			if err = r.reconcilePVC(ctx, rabbitmqCluster, sts); err != nil {
				r.setReconcileSuccess(ctx, rabbitmqCluster, corev1.ConditionFalse, "FailedReconcilePVC", err.Error())
				return ctrl.Result{}, err
			}

		}
		var operationResult controllerutil.OperationResult
		err = clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
			var apiError error
			operationResult, apiError = controllerutil.CreateOrUpdate(ctx, r.Client, resource, func() error {
				return builder.Update(resource)
			})
			return apiError
		})
		r.logAndRecordOperationResult(ctx, logger, rabbitmqCluster, resource, operationResult, err)
		if err != nil {
			r.setReconcileSuccess(ctx, rabbitmqCluster, corev1.ConditionFalse, "Error", err.Error())
			return ctrl.Result{}, err
		}

		if err = r.annotateIfNeeded(ctx, logger, builder, operationResult, rabbitmqCluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	if requeueAfter, err := r.restartStatefulSetIfNeeded(ctx, logger, rabbitmqCluster); err != nil || requeueAfter > 0 {
		if err != nil {
			r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
				"stage": "restartStatefulSetIfNeeded",
				"error": err.Error(),
			})
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}

	if err := r.reconcileStatus(ctx, rabbitmqCluster); err != nil {
		r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
			"stage": "reconcileStatus",
			"error": err.Error(),
		})
		return ctrl.Result{}, err
	}

	// By this point the StatefulSet may have finished deploying. Run any
	// post-deploy steps if so, or requeue until the deployment is finished.
	if requeueAfter, err := r.runRabbitmqCLICommandsIfAnnotated(ctx, rabbitmqCluster); err != nil || requeueAfter > 0 {
		if err != nil {
			r.setReconcileSuccess(ctx, rabbitmqCluster, corev1.ConditionFalse, "FailedCLICommand", err.Error())
			r.logTrace(ctx, "ReconcileError", "", rabbitmqCluster, map[string]interface{}{
				"stage": "cliCommands",
				"error": err.Error(),
			})
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}

	// Set ReconcileSuccess to true and update observedGeneration after all reconciliation steps have finished with no error
	rabbitmqCluster.Status.ObservedGeneration = rabbitmqCluster.GetGeneration()
	r.setReconcileSuccess(ctx, rabbitmqCluster, corev1.ConditionTrue, "Success", "Finish reconciling")
	r.logTrace(ctx, "ReconcileSuccess", "", rabbitmqCluster, nil)

	logger.Info("Finished reconciling")

	return ctrl.Result{}, nil
}

func (r *RabbitmqClusterReconciler) getRabbitmqCluster(ctx context.Context, namespacedName types.NamespacedName) (*rabbitmqv1beta1.RabbitmqCluster, error) {
	rabbitmqClusterInstance := &rabbitmqv1beta1.RabbitmqCluster{}
	err := r.Get(ctx, namespacedName, rabbitmqClusterInstance)
	return rabbitmqClusterInstance, err
}

// logAndRecordOperationResult - helper function to log and record events with message and error
// it logs and records 'updated' and 'created' OperationResult, and ignores OperationResult 'unchanged'
func (r *RabbitmqClusterReconciler) logAndRecordOperationResult(ctx context.Context, logger logr.Logger, rmq runtime.Object, resource runtime.Object, operationResult controllerutil.OperationResult, err error) {
	if operationResult == controllerutil.OperationResultNone && err == nil {
		return
	}

	var operation string
	switch operationResult {
	case controllerutil.OperationResultCreated:
		operation = "create"
	case controllerutil.OperationResultUpdated:
		operation = "update"
	default:
		operation = string(operationResult)
	}

	caser := cases.Title(language.English)
	if err == nil {
		msg := fmt.Sprintf("%sd resource %s of Type %T", operation, resource.(metav1.Object).GetName(), resource.(metav1.Object))
		logger.Info(msg)
		r.Recorder.Event(rmq, corev1.EventTypeNormal, fmt.Sprintf("Successful%s", caser.String(operation)), msg)

		if rmqCluster, ok := rmq.(*rabbitmqv1beta1.RabbitmqCluster); ok {
			details := map[string]interface{}{
				"operation": operation,
			}
			if obj, ok := resource.(metav1.Object); ok {
				details["resourceName"] = obj.GetName()
				details["resourceKind"] = resource.GetObjectKind().GroupVersionKind().Kind
			}

			eventType := "ResourceUpdated"
		switch res := resource.(type) {
		case *appsv1.StatefulSet:
			eventType = "StatefulSetUpdated"
			if res.Spec.Replicas != nil {
				details["replicas"] = *res.Spec.Replicas
			}
		case *corev1.Service:
			eventType = "ServiceUpdated"
			details["serviceType"] = string(res.Spec.Type)
			if res.Spec.ClusterIP != "" {
				details["clusterIP"] = res.Spec.ClusterIP
			}
			if len(res.Spec.ClusterIPs) > 0 {
				details["clusterIPs"] = res.Spec.ClusterIPs
			}
		case *corev1.ConfigMap:
			eventType = "ConfigMapUpdated"
		case *corev1.Secret:
			eventType = "SecretUpdated"
		}

			r.logTrace(ctx, eventType, "", rmqCluster, details)
		}
	}

	if err != nil {
		var msg string
		if operation != "unchanged" {
			msg = fmt.Sprintf("failed to %s resource %s of Type %T: ", operation, resource.(metav1.Object).GetName(), resource.(metav1.Object))
		}
		msg = fmt.Sprintf("%s%s", msg, err)
		logger.Error(err, msg)
		r.Recorder.Event(rmq, corev1.EventTypeWarning, fmt.Sprintf("Failed%s", caser.String(operation)), msg)
		if rmqCluster, ok := rmq.(*rabbitmqv1beta1.RabbitmqCluster); ok {
			details := map[string]interface{}{
				"operation": operation,
				"error":     err.Error(),
			}
			if obj, ok := resource.(metav1.Object); ok {
				details["resourceName"] = obj.GetName()
				details["resourceKind"] = resource.GetObjectKind().GroupVersionKind().Kind
			}
			r.logTrace(ctx, "ResourceUpdateFailed", "", rmqCluster, details)
		}
	}
}

func (r *RabbitmqClusterReconciler) updateStatusConditions(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster) (time.Duration, error) {
	logger := ctrl.LoggerFrom(ctx)
	childResources, err := r.getChildResources(ctx, rmq)
	if err != nil {
		return 0, err
	}

	oldConditions := make([]status.RabbitmqClusterCondition, len(rmq.Status.Conditions))
	copy(oldConditions, rmq.Status.Conditions)
	rmq.Status.SetConditions(childResources)

	if !reflect.DeepEqual(rmq.Status.Conditions, oldConditions) {
		if err = r.Status().Update(ctx, rmq); err != nil {
			// FIXME: must fetch again to avoid the conflict
			if k8serrors.IsConflict(err) {
				logger.Info("failed to update status because of conflict; requeueing...",
					"namespace", rmq.Namespace,
					"name", rmq.Name)
				return 2 * time.Second, nil
			}
			return 0, err
		}
	}
	return 0, nil
}

func (r *RabbitmqClusterReconciler) getChildResources(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster) ([]runtime.Object, error) {
	sts := &appsv1.StatefulSet{}
	endpointSliceList := &discoveryv1.EndpointSliceList{}
	endpointSlice := &discoveryv1.EndpointSlice{}

	if err := r.Get(ctx,
		types.NamespacedName{Name: rmq.ChildResourceName("server"), Namespace: rmq.Namespace},
		sts); err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	} else if k8serrors.IsNotFound(err) {
		sts = nil
	}

	selector, err := labels.Parse(fmt.Sprintf("%s=%s", discoveryv1.LabelServiceName, rmq.Name))
	if err != nil {
		return nil, err
	}

	listOptions := client.ListOptions{
		LabelSelector: selector,
		Namespace:     rmq.Namespace,
	}

	if err := r.List(ctx, endpointSliceList, &listOptions); err != nil {
		r.logTrace(ctx, "EndpointSliceListFailed", "", rmq, map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	} else if len(endpointSliceList.Items) == 0 {
		endpointSlice = nil
	} else {
		endpointSlice = &endpointSliceList.Items[0]
	}
	if endpointSlice != nil {
		readyCount := 0
		notReadyCount := 0
		unknownCount := 0
		for _, ep := range endpointSlice.Endpoints {
			if ep.Conditions.Ready == nil {
				unknownCount++
				continue
			}
			if *ep.Conditions.Ready {
				readyCount++
			} else {
				notReadyCount++
			}
		}
		r.logTrace(ctx, "EndpointSliceObserved", "", rmq, map[string]interface{}{
			"name":                  endpointSlice.Name,
			"totalEndpoints":        len(endpointSlice.Endpoints),
			"readyEndpoints":        readyCount,
			"notReadyEndpoints":     notReadyCount,
			"unknownReadyEndpoints": unknownCount,
		})
	} else {
		r.logTrace(ctx, "EndpointSliceNotFound", "", rmq, map[string]interface{}{
			"serviceName": rmq.Name,
		})
	}

	return []runtime.Object{sts, endpointSlice}, nil
}

func (r *RabbitmqClusterReconciler) setReconcileSuccess(ctx context.Context, rabbitmqCluster *rabbitmqv1beta1.RabbitmqCluster, condition corev1.ConditionStatus, reason, msg string) {
	rabbitmqCluster.Status.SetCondition(status.ReconcileSuccess, condition, reason, msg)
	if writerErr := r.Status().Update(ctx, rabbitmqCluster); writerErr != nil {
		ctrl.LoggerFrom(ctx).Error(writerErr, "Failed to update Custom Resource status",
			"namespace", rabbitmqCluster.Namespace,
			"name", rabbitmqCluster.Name)
	}
}

func (r *RabbitmqClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	for _, resource := range []client.Object{&appsv1.StatefulSet{}, &corev1.ConfigMap{}, &corev1.Service{}} {
		if err := mgr.GetFieldIndexer().IndexField(context.Background(), resource, ownerKey, addResourceToIndex); err != nil {
			return err
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&rabbitmqv1beta1.RabbitmqCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}

func addResourceToIndex(rawObj client.Object) []string {
	switch resourceObject := rawObj.(type) {
	case *appsv1.StatefulSet, *corev1.ConfigMap, *corev1.Service, *rbacv1.Role, *rbacv1.RoleBinding, *corev1.ServiceAccount, *corev1.Secret:
		owner := metav1.GetControllerOf(resourceObject)
		return validateAndGetOwner(owner)
	default:
		return nil
	}
}

func validateAndGetOwner(owner *metav1.OwnerReference) []string {
	if owner == nil {
		return nil
	}
	if owner.APIVersion != apiGVStr || owner.Kind != ownerKind {
		return nil
	}
	return []string{owner.Name}
}

func (r *RabbitmqClusterReconciler) markForQueueRebalance(ctx context.Context, rmq *rabbitmqv1beta1.RabbitmqCluster) error {
	if rmq.Annotations == nil {
		rmq.Annotations = make(map[string]string)
	}

	if len(rmq.Annotations[queueRebalanceAnnotation]) > 0 {
		return nil
	}

	rmq.Annotations[queueRebalanceAnnotation] = time.Now().Format(time.RFC3339)

	return r.Update(ctx, rmq)
}
