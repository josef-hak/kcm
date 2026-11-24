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

package argocd

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

// Argo CD health and sync vocabulary, as reported on Application.status.
const (
	healthHealthy  = "Healthy"
	healthDegraded = "Degraded"

	syncSynced = "Synced"

	operationFailed = "Failed"
	operationError  = "Error"
)

// applicationStatus is the part of Application.status the adapter reads.
type applicationStatus struct {
	OperationState *operationState        `json:"operationState,omitempty"`
	Health         healthStatus           `json:"health"`
	Sync           syncStatus             `json:"sync"`
	Conditions     []applicationCondition `json:"conditions,omitempty"`
}

type healthStatus struct {
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

type syncStatus struct {
	Status   string `json:"status,omitempty"`
	Revision string `json:"revision,omitempty"`
}

type operationState struct {
	Phase   string `json:"phase,omitempty"`
	Message string `json:"message,omitempty"`
}

type applicationCondition struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
}

func statusFromApplication(app *unstructured.Unstructured) (applicationStatus, error) {
	var status applicationStatus
	raw, found, err := unstructured.NestedMap(app.Object, "status")
	if err != nil {
		return status, fmt.Errorf("failed to read status of Application %s: %w", app.GetName(), err)
	}
	if !found {
		return status, nil
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &status); err != nil {
		return status, fmt.Errorf("failed to convert status of Application %s: %w", app.GetName(), err)
	}
	return status, nil
}

// serviceState translates one Application into the state of the service it
// backs.
//
// Deployed requires both axes to agree: Argo CD reports the live resources as
// Healthy and the cluster as Synced with the revision it was told to deploy.
// Sync alone would call a service done the moment the manifests were applied,
// before anything they created came up; health alone would keep reporting the
// previous revision as fine while a new one is still rolling out.
func serviceState(app *unstructured.Unstructured, svc kcmv1.ServiceWithValues, previous *kcmv1.ServiceState, now time.Time) kcmv1.ServiceState {
	state := kcmv1.ServiceState{
		Name:      svc.Name,
		Namespace: svc.Namespace,
		Template:  svc.Template,
		Type:      kcmv1.ServiceTypeHelm,
		State:     kcmv1.ServiceStateProvisioning,
	}
	if previous != nil {
		state.Version = previous.Version
		state.LastStateTransitionTime = previous.LastStateTransitionTime
	}

	status, err := statusFromApplication(app)
	if err != nil {
		state.State = kcmv1.ServiceStateFailed
		state.FailureMessage = err.Error()
		return stamp(state, previous, now)
	}

	targetRevision, _, _ := unstructured.NestedString(app.Object, "spec", "source", "targetRevision")

	switch {
	case status.Health.Status == healthHealthy && status.Sync.Status == syncSynced:
		state.State = kcmv1.ServiceStateDeployed
		// The revision Argo CD confirmed, not the one we asked for: during an
		// upgrade the two differ until the new chart is actually on cluster,
		// and KCM gates dependent services on the confirmed one.
		state.Version = firstNonEmpty(status.Sync.Revision, targetRevision, svc.Version)
		state.FailureMessage = ""

	case status.OperationState != nil && (status.OperationState.Phase == operationFailed || status.OperationState.Phase == operationError):
		state.State = kcmv1.ServiceStateFailed
		state.FailureMessage = firstNonEmpty(status.OperationState.Message, "sync "+strings.ToLower(status.OperationState.Phase))

	case status.Health.Status == healthDegraded:
		state.State = kcmv1.ServiceStateFailed
		state.FailureMessage = firstNonEmpty(status.Health.Message, conditionMessage(status.Conditions), "application is degraded")

	default:
		state.State = kcmv1.ServiceStateProvisioning
		state.FailureMessage = ""
	}

	// An error Argo CD cannot even attempt a sync from - a chart that does not
	// resolve, a destination it was refused - is reported on the conditions
	// rather than on an operation that never ran.
	if state.State == kcmv1.ServiceStateProvisioning {
		if msg := errorConditionMessage(status.Conditions); msg != "" {
			state.State = kcmv1.ServiceStateFailed
			state.FailureMessage = msg
		}
	}

	return stamp(state, previous, now)
}

// stamp keeps LastStateTransitionTime pinned to the moment the state actually
// changed, so the field means what its name says.
func stamp(state kcmv1.ServiceState, previous *kcmv1.ServiceState, now time.Time) kcmv1.ServiceState {
	if previous == nil || previous.State != state.State || state.LastStateTransitionTime == nil {
		state.LastStateTransitionTime = ptrTime(now)
	}
	return state
}

func errorConditionMessage(conditions []applicationCondition) string {
	for _, c := range conditions {
		if strings.HasSuffix(c.Type, "Error") {
			return c.Type + ": " + c.Message
		}
	}
	return ""
}

func conditionMessage(conditions []applicationCondition) string {
	for _, c := range conditions {
		if c.Message != "" {
			return c.Type + ": " + c.Message
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
