// Copyright 2026
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

// Package argocd implements the KSM adapter that deploys the services of a
// ServiceSet through Argo CD: one Application per service, created in the Argo
// CD namespace of the management cluster, with the ServiceSet's status fed
// back from what Argo CD reports about them.
package argocd

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/json"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	"github.com/K0rdent/kcm/internal/record"
	"github.com/K0rdent/kcm/internal/serviceset"
	kubeutil "github.com/K0rdent/kcm/internal/util/kube"
	pollerutil "github.com/K0rdent/kcm/internal/util/poller"
	ratelimitutil "github.com/K0rdent/kcm/internal/util/ratelimit"
)

// ServiceSetReconciler reconciles a ServiceSet into Argo CD Application
// objects.
type ServiceSetReconciler struct {
	client.Client

	timeFunc func() time.Time

	SystemNamespace string

	// ArgoCDNamespace is where Argo CD runs, and therefore the only namespace
	// it reads Application, cluster and repository objects from.
	ArgoCDNamespace string

	// AdapterName and AdapterNamespace identify the workload running this
	// controller, matched against the adapter reference of a
	// [github.com/K0rdent/kcm/api/v1beta1.StateManagementProvider].
	AdapterName      string
	AdapterNamespace string

	// ProviderName is the StateManagementProvider this adapter serves. It is
	// what keeps two adapters hosted by the same workload - the built-in
	// Sveltos one and this - from both acting on the same ServiceSet.
	ProviderName string

	MaxConcurrentReconciles int
	requeueInterval         time.Duration
}

func (r *ServiceSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	start := time.Now()
	l := ctrl.LoggerFrom(ctx)
	l.Info("Reconciling ServiceSet")

	serviceSet := new(kcmv1.ServiceSet)
	err = r.Get(ctx, req.NamespacedName, serviceSet)
	if apierrors.IsNotFound(err) {
		l.Info("ServiceSet not found, skipping")
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	smp := new(kcmv1.StateManagementProvider)
	if err = r.Get(ctx, client.ObjectKey{Name: serviceSet.Spec.Provider.Name}, smp); err != nil {
		return ctrl.Result{}, err
	}
	if !r.serves(smp) {
		l.V(1).Info("ServiceSet is not served by this adapter, skipping", "provider", smp.Name)
		return ctrl.Result{}, nil
	}

	if !serviceSet.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, serviceSet)
	}

	if controllerutil.AddFinalizer(serviceSet, kcmv1.ServiceSetFinalizer) {
		return ctrl.Result{}, r.Update(ctx, serviceSet)
	}

	continueReconciliation, err := labelsMatchSelector(serviceSet.Labels, smp.Spec.Selector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check ServiceSet labels: %w", err)
	}
	if !continueReconciliation {
		l.V(1).Info("ServiceSet labels do not match provider selector, skipping")
		return ctrl.Result{}, nil
	}

	clone := serviceSet.DeepCopy()
	defer func() {
		if err == nil {
			fillNotDeployedServices(serviceSet, r.timeFunc)
		}
		// The status is written even when the reconciliation failed, so the
		// failure shows up on the object rather than only in the logs. The
		// original error is still returned, so the object is requeued with
		// respect to the rate limiter.
		if !equality.Semantic.DeepEqual(clone.Status, serviceSet.Status) {
			if updateErr := r.Status().Update(ctx, serviceSet); updateErr != nil {
				err = errors.Join(err, updateErr)
			}
		}
		l.Info("Finished reconciling ServiceSet", "duration", time.Since(start), "err", err)
	}()

	serviceSet.Status.Cluster = serviceset.ClusterReference(serviceSet)
	serviceSet.Status.Provider = kcmv1.ProviderState{
		Ready:     smp.Status.Ready,
		Suspended: smp.Spec.Suspend,
	}
	if !smp.Status.Ready {
		if clone.Status.Provider.Ready {
			record.Eventf(serviceSet, smp, kcmv1.StateManagementProviderNotReadyEvent, kcmv1.ServiceSetReconcileEventAction,
				"StateManagementProvider %s not ready, skipping ServiceSet %s reconciliation", smp.Name, serviceSet.Name)
		}
		l.Info("StateManagementProvider is not ready, skipping", "provider", serviceSet.Spec.Provider)
		return ctrl.Result{}, nil
	}
	if smp.Spec.Suspend {
		if !clone.Status.Provider.Suspended {
			record.Eventf(serviceSet, smp, kcmv1.StateManagementProviderSuspendedEvent, kcmv1.ServiceSetReconcileEventAction,
				"StateManagementProvider %s suspended, skipping ServiceSet %s reconciliation", smp.Name, serviceSet.Name)
		}
		l.Info("StateManagementProvider is suspended, skipping", "provider", serviceSet.Spec.Provider)
		return ctrl.Result{}, nil
	}

	if err = r.ensureApplications(ctx, serviceSet); err != nil {
		conditionOldState := apimeta.FindStatusCondition(clone.Status.Conditions, kcmv1.ServiceSetProfileCondition)
		conditionNewState := apimeta.FindStatusCondition(serviceSet.Status.Conditions, kcmv1.ServiceSetProfileCondition)
		if conditionStatusChangedToFalse(conditionOldState, conditionNewState) {
			record.Warnf(serviceSet, nil, kcmv1.ServiceSetEnsureProfileFailedEvent, kcmv1.ServiceSetEnsureProfileEventAction,
				"Failed to ensure Argo CD Applications for ServiceSet %s: %v", serviceSet.Name, err)
		}
		return ctrl.Result{}, err
	}

	if err = r.collectServiceStatuses(ctx, serviceSet); err != nil {
		conditionOldState := apimeta.FindStatusCondition(clone.Status.Conditions, kcmv1.ServiceSetStatusesCollectedCondition)
		conditionNewState := apimeta.FindStatusCondition(serviceSet.Status.Conditions, kcmv1.ServiceSetStatusesCollectedCondition)
		if conditionStatusChangedToFalse(conditionOldState, conditionNewState) {
			record.Warnf(serviceSet, nil, kcmv1.ServiceSetCollectServiceStatusesFailedEvent, kcmv1.ServiceSetCollectServiceStatusesEventAction,
				"Failed to collect Service statuses for ServiceSet %s: %v", serviceSet.Name, err)
		}
		return ctrl.Result{}, err
	}

	// Argo CD moves on its own clock - a sync it started is not an event on
	// any object this controller watches. The poller covers the long run; this
	// keeps a freshly created ServiceSet from waiting a whole tick for its
	// first status.
	if !serviceSet.Status.Deployed {
		return ctrl.Result{RequeueAfter: r.requeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

// serves reports whether the given provider is the one this adapter is
// responsible for.
func (r *ServiceSetReconciler) serves(smp *kcmv1.StateManagementProvider) bool {
	if r.ProviderName != "" && smp.Name != r.ProviderName {
		return false
	}
	return smp.Spec.Adapter.Name == r.AdapterName && smp.Spec.Adapter.Namespace == r.AdapterNamespace
}

// ensureApplications brings the Applications of a ServiceSet in line with its
// spec: one per service, and none left over from a service that was removed.
func (r *ServiceSetReconciler) ensureApplications(ctx context.Context, serviceSet *kcmv1.ServiceSet) (err error) {
	start := time.Now()
	l := ctrl.LoggerFrom(ctx)
	l.Info("Ensuring Argo CD Applications")

	condition := findCondition(serviceSet, kcmv1.ServiceSetProfileCondition)
	initialStatus := condition.Status

	status := metav1.ConditionFalse
	reason := kcmv1.ServiceSetProfileNotReadyReason
	message := kcmv1.ServiceSetProfileNotReadyMessage

	defer func() {
		updated := updateCondition(serviceSet, condition, status, reason, message, r.timeFunc())
		if updated && initialStatus != status && status == metav1.ConditionTrue {
			record.Eventf(serviceSet, nil, kcmv1.ServiceSetEnsureProfileSuccessEvent, kcmv1.ServiceSetEnsureProfileEventAction,
				"Successfully ensured Argo CD Applications for ServiceSet %s", serviceSet.Name)
		}
		l.V(1).Info("Finished ensuring Argo CD Applications", "duration", time.Since(start))
	}()

	config := new(providerConfig)
	if raw := serviceSet.Spec.Provider.Config; raw != nil && len(raw.Raw) > 0 {
		if err := json.Unmarshal(raw.Raw, config); err != nil {
			reason = kcmv1.ServiceSetProfileBuildFailedReason
			message = fmt.Sprintf("Failed to read provider config of ServiceSet %s: %v", serviceSet.Name, err)
			return fmt.Errorf("failed to unmarshal provider config: %w", err)
		}
	}

	rgnClient, err := r.regionalClient(ctx, serviceSet)
	if err != nil {
		reason = kcmv1.ServiceSetProfileBuildFailedReason
		message = fmt.Sprintf("Failed to reach the cluster holding the kubeconfig for ServiceSet %s: %v", serviceSet.Name, err)
		return err
	}

	expected := make(map[string]struct{}, len(serviceSet.Spec.Services))
	for _, svc := range serviceSet.Spec.Services {
		name, err := r.ensureApplication(ctx, rgnClient, serviceSet, svc, *config)
		if err != nil {
			reason = kcmv1.ServiceSetHelmChartsBuildFailedReason
			message = fmt.Sprintf("Failed to ensure Argo CD Application for service %s/%s: %v", svc.Namespace, svc.Name, err)
			return err
		}
		expected[name] = struct{}{}
		seedServiceState(serviceSet, svc, r.timeFunc)
	}

	if err := r.pruneApplications(ctx, serviceSet, expected); err != nil {
		reason = kcmv1.ServiceSetProfileBuildFailedReason
		message = fmt.Sprintf("Failed to remove stale Argo CD Applications of ServiceSet %s: %v", serviceSet.Name, err)
		return err
	}

	status = metav1.ConditionTrue
	reason = kcmv1.ServiceSetProfileReadyReason
	message = kcmv1.ServiceSetProfileReadyMessage
	return nil
}

func (r *ServiceSetReconciler) ensureApplication(
	ctx context.Context,
	rgnClient client.Client,
	serviceSet *kcmv1.ServiceSet,
	svc kcmv1.ServiceWithValues,
	config providerConfig,
) (string, error) {
	tmpl := new(kcmv1.ServiceTemplate)
	tmplKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: svc.Template}
	if err := r.Get(ctx, tmplKey, tmpl); err != nil {
		return "", fmt.Errorf("failed to get ServiceTemplate %s: %w", tmplKey, err)
	}

	src, repo, err := sourceForService(ctx, r.Client, svc, tmpl)
	if err != nil {
		return "", err
	}
	if err := r.ensureRepository(ctx, serviceSet.Namespace, repo); err != nil {
		return "", err
	}

	values, err := resolveValues(ctx, r.Client, serviceSet.Namespace, svc)
	if err != nil {
		return "", err
	}
	src.Helm.Values = values

	destination, err := r.destination(ctx, rgnClient, serviceSet, effectiveNamespace(svc.Namespace))
	if err != nil {
		return "", err
	}

	spec, err := toUnstructuredSpec(&applicationSpec{
		Project:              config.project(),
		Source:               src,
		Destination:          destination,
		SyncPolicy:           config.syncPolicyFrom(),
		IgnoreDifferences:    config.IgnoreDifferences,
		RevisionHistoryLimit: config.RevisionHistoryLimit,
	})
	if err != nil {
		return "", err
	}

	name := applicationName(serviceSet, svc)
	app := newApplication()
	key := client.ObjectKey{Namespace: r.ArgoCDNamespace, Name: name}
	err = r.Get(ctx, key, app)
	if client.IgnoreNotFound(err) != nil {
		return "", fmt.Errorf("failed to get Application %s: %w", key, err)
	}

	if err != nil {
		app = newApplication()
		app.SetName(key.Name)
		app.SetNamespace(key.Namespace)
		app.SetLabels(applicationLabels(serviceSet, svc))
		app.SetFinalizers([]string{applicationFinalizer})
		app.Object["spec"] = spec
		if err := r.Create(ctx, app); client.IgnoreAlreadyExists(err) != nil {
			return "", fmt.Errorf("failed to create Application %s: %w", key, err)
		}
		return name, nil
	}

	if equality.Semantic.DeepEqual(app.Object["spec"], spec) {
		return name, nil
	}
	app.Object["spec"] = spec
	app.SetLabels(applicationLabels(serviceSet, svc))
	if err := r.Update(ctx, app); err != nil {
		return "", fmt.Errorf("failed to update Application %s: %w", key, err)
	}
	return name, nil
}

// pruneApplications deletes the Applications of a ServiceSet that no service
// in its spec asks for any more.
func (r *ServiceSetReconciler) pruneApplications(ctx context.Context, serviceSet *kcmv1.ServiceSet, expected map[string]struct{}) error {
	apps, err := r.listApplications(ctx, serviceSet)
	if err != nil {
		return err
	}
	for i := range apps {
		app := &apps[i]
		if _, ok := expected[app.GetName()]; ok {
			continue
		}
		if !app.GetDeletionTimestamp().IsZero() {
			continue
		}
		if err := r.Delete(ctx, app); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("failed to delete stale Application %s/%s: %w", app.GetNamespace(), app.GetName(), err)
		}
	}
	return nil
}

func (r *ServiceSetReconciler) listApplications(ctx context.Context, serviceSet *kcmv1.ServiceSet) ([]unstructured.Unstructured, error) {
	list := newApplicationList()
	err := r.List(ctx, list,
		client.InNamespace(r.ArgoCDNamespace),
		client.MatchingLabels(serviceSetSelector(serviceSet)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list Applications of ServiceSet %s: %w", client.ObjectKeyFromObject(serviceSet), err)
	}
	return list.Items, nil
}

func (r *ServiceSetReconciler) collectServiceStatuses(ctx context.Context, serviceSet *kcmv1.ServiceSet) (err error) {
	start := time.Now()
	l := ctrl.LoggerFrom(ctx)
	l.Info("Collecting Service statuses")

	condition := findCondition(serviceSet, kcmv1.ServiceSetStatusesCollectedCondition)
	initialStatus := condition.Status

	status := metav1.ConditionFalse
	reason := kcmv1.ServiceSetStatusesNotCollectedReason
	message := kcmv1.ServiceSetStatusesNotCollectedMessage

	defer func() {
		updated := updateCondition(serviceSet, condition, status, reason, message, r.timeFunc())
		if updated && initialStatus != status && status == metav1.ConditionTrue {
			record.Eventf(serviceSet, nil, kcmv1.ServiceSetCollectServiceStatusesSuccessEvent, kcmv1.ServiceSetCollectServiceStatusesEventAction,
				"Successfully collected service statuses for ServiceSet %s", serviceSet.Name)
		}
		l.V(1).Info("Finished services status collection", "duration", time.Since(start))
	}()

	apps, err := r.listApplications(ctx, serviceSet)
	if err != nil {
		return err
	}
	byName := make(map[string]*unstructured.Unstructured, len(apps))
	for i := range apps {
		byName[apps[i].GetName()] = &apps[i]
	}

	now := r.timeFunc()
	states := make([]kcmv1.ServiceState, 0, len(serviceSet.Spec.Services))
	for _, svc := range serviceSet.Spec.Services {
		app, ok := byName[applicationName(serviceSet, svc)]
		if !ok {
			// Created a moment ago and not visible yet, or deleted behind our
			// back - either way it is not deployed.
			states = append(states, kcmv1.ServiceState{
				Name:                    svc.Name,
				Namespace:               svc.Namespace,
				Template:                svc.Template,
				Type:                    kcmv1.ServiceTypeHelm,
				State:                   kcmv1.ServiceStateProvisioning,
				LastStateTransitionTime: ptrTime(now),
			})
			continue
		}
		states = append(states, serviceState(app, svc, findServiceState(serviceSet, svc), now))
	}

	serviceSet.Status.Services = states
	serviceSet.Status.Deployed = len(states) > 0 && !slices.ContainsFunc(states, func(s kcmv1.ServiceState) bool {
		return s.State != kcmv1.ServiceStateDeployed
	})

	r.updateServicesInReadyStateCondition(serviceSet)

	status = metav1.ConditionTrue
	reason = kcmv1.ServiceSetStatusesCollectedReason
	message = kcmv1.ServiceSetStatusesCollectedMessage
	return nil
}

// reconcileDelete removes the Applications of a ServiceSet and waits for Argo
// CD to take the services off the cluster before releasing the finalizer.
func (r *ServiceSetReconciler) reconcileDelete(ctx context.Context, serviceSet *kcmv1.ServiceSet) (ctrl.Result, error) {
	l := ctrl.LoggerFrom(ctx)
	l.Info("Reconciling ServiceSet deletion")

	if slices.ContainsFunc(serviceSet.Status.Services, func(state kcmv1.ServiceState) bool {
		return state.State != kcmv1.ServiceStateDeleting
	}) {
		states := make([]kcmv1.ServiceState, 0, len(serviceSet.Status.Services))
		for _, state := range serviceSet.Status.Services {
			newState := state.DeepCopy()
			newState.State = kcmv1.ServiceStateDeleting
			newState.LastStateTransitionTime = ptrTime(r.timeFunc())
			states = append(states, *newState)
		}
		serviceSet.Status.Services = states
		return ctrl.Result{}, r.Status().Update(ctx, serviceSet)
	}

	apps, err := r.listApplications(ctx, serviceSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(apps) > 0 {
		for i := range apps {
			app := &apps[i]
			if !app.GetDeletionTimestamp().IsZero() {
				continue
			}
			if err := r.terminateOperation(ctx, app); err != nil {
				return ctrl.Result{}, err
			}
			l.Info("Deleting Application", "application", client.ObjectKeyFromObject(app))
			if err := r.Delete(ctx, app); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, fmt.Errorf("failed to delete Application %s/%s: %w", app.GetNamespace(), app.GetName(), err)
			}
		}
		// The Applications carry Argo CD's resources finalizer, so they stay
		// until what they deployed is gone from the target cluster.
		return ctrl.Result{RequeueAfter: r.requeueInterval}, nil
	}

	if controllerutil.RemoveFinalizer(serviceSet, kcmv1.ServiceSetFinalizer) {
		if err := r.Update(ctx, serviceSet); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

// terminateOperation asks Argo CD to stop a sync that is still in flight, the
// way `argocd app terminate-op` does.
//
// Deleting an Application with a running operation is not enough: Argo CD
// finishes what it started first, and a sync that keeps failing is retried
// with a growing back-off, so a service that can never apply holds up the
// removal of the whole ServiceSet for minutes.
func (r *ServiceSetReconciler) terminateOperation(ctx context.Context, app *unstructured.Unstructured) error {
	// A phase that is missing or not a string is not an operation in flight,
	// and NestedString reports both as not-found.
	phase, found, _ := unstructured.NestedString(app.Object, "status", "operationState", "phase")
	if !found || phase != operationRunning {
		return nil
	}
	if err := unstructured.SetNestedField(app.Object, operationTerminating, "status", "operationState", "phase"); err != nil {
		return fmt.Errorf("failed to mark the operation of Application %s terminating: %w", app.GetName(), err)
	}
	ctrl.LoggerFrom(ctx).Info("Terminating in-flight Argo CD sync", "application", client.ObjectKeyFromObject(app))
	if err := r.Update(ctx, app); client.IgnoreNotFound(err) != nil {
		// Not fatal: the delete below still happens, it just waits for the
		// sync Argo CD is still retrying.
		ctrl.LoggerFrom(ctx).Error(err, "failed to terminate the sync, deleting anyway",
			"application", client.ObjectKeyFromObject(app))
	}
	return nil
}

// regionalClient returns the client for the cluster that holds the kubeconfig
// of the ServiceSet's target cluster: the management cluster, unless the
// cluster was provisioned from a region.
func (r *ServiceSetReconciler) regionalClient(ctx context.Context, serviceSet *kcmv1.ServiceSet) (client.Client, error) {
	if serviceSet.Spec.Cluster == "" || serviceSet.Spec.Provider.SelfManagement {
		return r.Client, nil
	}

	clusterKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: serviceSet.Spec.Cluster}
	cd := new(kcmv1.ClusterDeployment)
	if err := r.Get(ctx, clusterKey, cd); err != nil {
		return nil, fmt.Errorf("failed to get ClusterDeployment %s: %w", clusterKey, err)
	}

	cred := new(kcmv1.Credential)
	credKey := client.ObjectKey{Namespace: cd.Namespace, Name: cd.Spec.Credential}
	if err := r.Get(ctx, credKey, cred); err != nil {
		return nil, fmt.Errorf("failed to get Credential %s: %w", credKey, err)
	}
	if cred.Spec.Region == "" {
		return r.Client, nil
	}

	rgn, err := kubeutil.GetRegionalClientByRegionName(ctx, r.Client, r.SystemNamespace, cred.Spec.Region, regionalScheme)
	if err != nil {
		return nil, fmt.Errorf("failed to get regional client for region %s: %w", cred.Spec.Region, err)
	}
	return rgn, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ServiceSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Client = mgr.GetClient()
	if r.timeFunc == nil {
		r.timeFunc = time.Now
	}
	if r.ArgoCDNamespace == "" {
		r.ArgoCDNamespace = DefaultArgoCDNamespace
	}
	r.requeueInterval = 10 * time.Second

	// Argo CD reports progress on its own Application objects, which this
	// controller reads without watching: the CRD may not exist when the
	// manager starts, and an informer on it would never recover from that.
	poller := pollerutil.NewRunner(
		enqueueApplications(r.Client, r.ArgoCDNamespace, r.ProviderName),
		pollerutil.WithInterval(r.requeueInterval),
		pollerutil.WithBufferSize(max(r.MaxConcurrentReconciles, 1)*10),
		pollerutil.WithName("poller-argocd-applications"),
	)
	if err := mgr.Add(poller); err != nil {
		return fmt.Errorf("failed to add poller to manager: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.TypedOptions[ctrl.Request]{
			MaxConcurrentReconciles: max(r.MaxConcurrentReconciles, 1),
			RateLimiter:             ratelimitutil.DefaultFastSlow(),
		}).
		Named("ksm-argocd-adapter").
		Watches(&kcmv1.ServiceSet{}, kubeutil.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) ([]ctrl.Request, error) {
			serviceSet, ok := o.(*kcmv1.ServiceSet)
			if !ok {
				return nil, nil
			}
			provider := new(kcmv1.StateManagementProvider)
			if err := r.Get(ctx, client.ObjectKey{Name: serviceSet.Spec.Provider.Name}, provider); err != nil {
				if apierrors.IsNotFound(err) {
					return nil, nil
				}
				return nil, fmt.Errorf("failed to get StateManagementProvider %s: %w", serviceSet.Spec.Provider.Name, err)
			}
			if !r.serves(provider) {
				return nil, nil
			}
			return []ctrl.Request{{Name: serviceSet.Name, Namespace: serviceSet.Namespace}}, nil
		})).
		Watches(&kcmv1.StateManagementProvider{}, kubeutil.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) ([]ctrl.Request, error) {
			provider, ok := o.(*kcmv1.StateManagementProvider)
			if !ok || !r.serves(provider) {
				return nil, nil
			}
			selector := fields.OneTermEqualSelector(kcmv1.ServiceSetProviderIndexKey, provider.Name)
			serviceSets := new(kcmv1.ServiceSetList)
			if err := r.List(ctx, serviceSets, client.MatchingFieldsSelector{Selector: selector}); err != nil {
				return nil, fmt.Errorf("failed to list ServiceSets for StateManagementProvider %s: %w", provider.Name, err)
			}
			requests := make([]ctrl.Request, 0, len(serviceSets.Items))
			for _, serviceSet := range serviceSets.Items {
				requests = append(requests, ctrl.Request{Name: serviceSet.Name, Namespace: serviceSet.Namespace})
			}
			return requests, nil
		})).
		WatchesRawSource(source.TypedChannel(poller.GetEventChannel(), &handler.TypedEnqueueRequestForObject[*kcmv1.ServiceSet]{})).
		Complete(r)
}

// updateServicesInReadyStateCondition updates the "ServicesInReadyState" condition.
func (r *ServiceSetReconciler) updateServicesInReadyStateCondition(serviceSet *kcmv1.ServiceSet) {
	deployed := 0
	for _, svc := range serviceSet.Status.Services {
		if svc.State == kcmv1.ServiceStateDeployed {
			deployed++
		}
	}
	total := len(serviceSet.Spec.Services)
	condition := findCondition(serviceSet, kcmv1.ServicesInReadyStateCondition)
	status := metav1.ConditionTrue
	if deployed != total {
		status = metav1.ConditionFalse
	}
	if deployed <= total {
		updateCondition(serviceSet, condition, status, kcmv1.ServicesInReadyStateCondition,
			fmt.Sprintf("%d/%d", deployed, total), r.timeFunc())
	}
}

// seedServiceState makes a service visible in the status as soon as its
// Application exists, before Argo CD has said anything about it.
func seedServiceState(serviceSet *kcmv1.ServiceSet, svc kcmv1.ServiceWithValues, now func() time.Time) {
	if slices.ContainsFunc(serviceSet.Status.Services, func(s kcmv1.ServiceState) bool {
		return s.Name == svc.Name && s.Namespace == svc.Namespace
	}) {
		return
	}
	serviceSet.Status.Services = append(serviceSet.Status.Services, kcmv1.ServiceState{
		LastStateTransitionTime: ptrTime(now()),
		Type:                    kcmv1.ServiceTypeHelm,
		Name:                    svc.Name,
		Namespace:               svc.Namespace,
		Template:                svc.Template,
		State:                   kcmv1.ServiceStateProvisioning,
	})
}

func findServiceState(serviceSet *kcmv1.ServiceSet, svc kcmv1.ServiceWithValues) *kcmv1.ServiceState {
	for i := range serviceSet.Status.Services {
		s := &serviceSet.Status.Services[i]
		if s.Name == svc.Name && s.Namespace == svc.Namespace {
			return s
		}
	}
	return nil
}

// resolveValues flattens the values a service is deployed with into the single
// YAML document an Argo CD Application source carries.
//
// Argo CD has no equivalent of a values reference to an object in the
// management cluster, so the referenced ConfigMaps and Secrets are read here;
// inline values win over them, as they do for every other provider.
func resolveValues(ctx context.Context, c client.Client, namespace string, svc kcmv1.ServiceWithValues) (string, error) {
	documents := make([]string, 0, len(svc.ValuesFrom)+1)
	for _, ref := range svc.ValuesFrom {
		key := client.ObjectKey{Namespace: namespace, Name: ref.Name}
		switch ref.Kind {
		case kcmv1.ConfigMapKind:
			cm := new(corev1.ConfigMap)
			if err := c.Get(ctx, key, cm); err != nil {
				return "", fmt.Errorf("failed to get ConfigMap %s referenced by service %s: %w", key, svc.Name, err)
			}
			for _, k := range sortedKeys(cm.Data) {
				documents = append(documents, cm.Data[k])
			}
		case kcmv1.SecretKind:
			secret := new(corev1.Secret)
			if err := c.Get(ctx, key, secret); err != nil {
				return "", fmt.Errorf("failed to get Secret %s referenced by service %s: %w", key, svc.Name, err)
			}
			for _, k := range sortedKeysBytes(secret.Data) {
				documents = append(documents, string(secret.Data[k]))
			}
		default:
			return "", fmt.Errorf("unsupported valuesFrom kind %q in service %s", ref.Kind, svc.Name)
		}
	}
	documents = append(documents, svc.Values)
	return mergeValues(documents)
}
