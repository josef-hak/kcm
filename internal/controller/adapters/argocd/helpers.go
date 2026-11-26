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
	"slices"
	"time"

	"helm.sh/helm/v3/pkg/chartutil"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	schemeutil "github.com/K0rdent/kcm/internal/util/scheme"
)

// DefaultArgoCDNamespace is where the Argo CD provider chart installs Argo CD,
// and therefore where the adapter writes Applications.
const DefaultArgoCDNamespace = "argocd"

func regionalScheme() (*runtime.Scheme, error) {
	return schemeutil.GetRegionalScheme()
}

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

// mergeValues coalesces YAML documents into one, with later documents
// overriding earlier ones.
func mergeValues(documents []string) (string, error) {
	merged := make(map[string]any)
	for _, doc := range documents {
		if doc == "" {
			continue
		}
		values := make(map[string]any)
		if err := yaml.Unmarshal([]byte(doc), &values); err != nil {
			return "", fmt.Errorf("failed to parse values: %w", err)
		}
		merged = chartutil.CoalesceTables(values, merged)
	}
	if len(merged) == 0 {
		return "", nil
	}
	out, err := yaml.Marshal(merged)
	if err != nil {
		return "", fmt.Errorf("failed to render values: %w", err)
	}
	return string(out), nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func sortedKeysBytes(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// labelsMatchSelector returns true if the labels of the ServiceSet match the selector.
func labelsMatchSelector(serviceSetLabels map[string]string, selector *metav1.LabelSelector) (bool, error) {
	if selector == nil {
		return true, nil
	}
	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return false, fmt.Errorf("failed to convert LabelSelector to Selector: %w", err)
	}
	return labelSelector.Matches(labels.Set(serviceSetLabels)), nil
}

// findCondition finds the condition of the given type in the ServiceSet,
// returning a fresh False one when it is not there yet.
func findCondition(serviceSet *kcmv1.ServiceSet, conditionType string) metav1.Condition {
	condition := apimeta.FindStatusCondition(serviceSet.Status.Conditions, conditionType)
	if condition == nil {
		condition = &metav1.Condition{
			Type:               conditionType,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: serviceSet.Generation,
		}
	}
	return *condition
}

// updateCondition updates the given condition of the ServiceSet.
func updateCondition(
	serviceSet *kcmv1.ServiceSet,
	condition metav1.Condition,
	status metav1.ConditionStatus,
	reason string,
	message string,
	transitionTime time.Time,
) bool {
	if condition.Status != status || condition.Reason != reason || condition.Message != message {
		condition.LastTransitionTime = metav1.NewTime(transitionTime)
	}
	condition.ObservedGeneration = serviceSet.Generation
	condition.Status = status
	condition.Reason = reason
	condition.Message = message
	return apimeta.SetStatusCondition(&serviceSet.Status.Conditions, condition)
}

// conditionStatusChangedToFalse returns true if the condition status changed
// to False from True or from not being there at all.
func conditionStatusChangedToFalse(conditionOldState, conditionNewState *metav1.Condition) bool {
	if conditionNewState == nil {
		return false
	}
	if conditionOldState == nil {
		return conditionNewState.Status == metav1.ConditionFalse
	}
	return conditionOldState.Status != conditionNewState.Status && conditionNewState.Status == metav1.ConditionFalse
}

// fillNotDeployedServices records the services the spec asks for that have no
// state yet, so the ServiceSet never reports less than it was handed.
func fillNotDeployedServices(serviceSet *kcmv1.ServiceSet, now func() time.Time) {
	known := make(map[client.ObjectKey]struct{}, len(serviceSet.Status.Services))
	for _, state := range serviceSet.Status.Services {
		known[client.ObjectKey{Namespace: state.Namespace, Name: state.Name}] = struct{}{}
	}
	for _, svc := range serviceSet.Spec.Services {
		if _, ok := known[client.ObjectKey{Namespace: svc.Namespace, Name: svc.Name}]; ok {
			continue
		}
		serviceSet.Status.Services = append(serviceSet.Status.Services, kcmv1.ServiceState{
			Name:      svc.Name,
			Namespace: svc.Namespace,
			Template:  svc.Template,
			// Only Helm services ever reach this adapter; anything else was
			// rejected while the Application was being built.
			Type:                    kcmv1.ServiceTypeHelm,
			State:                   kcmv1.ServiceStateNotDeployed,
			LastStateTransitionTime: ptrTime(now()),
		})
	}
}
