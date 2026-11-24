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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

func application(t *testing.T, manifest string) *unstructured.Unstructured {
	t.Helper()
	obj := make(map[string]any)
	require.NoError(t, yaml.Unmarshal([]byte(manifest), &obj))
	return &unstructured.Unstructured{Object: obj}
}

func Test_serviceState(t *testing.T) {
	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	svc := kcmv1.ServiceWithValues{Name: "traefik", Namespace: "traefik", Template: "traefik-41-2-0", Version: "41.2.0"}

	for _, tc := range []struct {
		name        string
		manifest    string
		wantState   string
		wantVersion string
		wantFailure string
		wantHealth  metav1.ConditionStatus
	}{
		{
			// The version reported is the one Argo CD synced rather than the
			// one it was asked for.
			name: "healthy and synced",
			manifest: `
spec: {source: {targetRevision: "41.2.0"}}
status:
  health: {status: Healthy}
  sync: {status: Synced, revision: "41.2.0"}`,
			wantState:   kcmv1.ServiceStateDeployed,
			wantVersion: "41.2.0",
			wantHealth:  metav1.ConditionTrue,
		},
		{
			// Mid-upgrade: the resources that are up are still the old chart,
			// so the service is not at the version its spec asks for.
			name: "healthy but out of sync",
			manifest: `
spec: {source: {targetRevision: "41.3.0"}}
status:
  health: {status: Healthy}
  sync: {status: OutOfSync, revision: "41.2.0"}`,
			wantState: kcmv1.ServiceStateProvisioning,
		},
		{
			// Synced is the bar, as it is for a Helm release under Sveltos.
			// Health that has not settled - and on a cluster with no load
			// balancer never will - is reported, not waited for.
			name: "synced but still rolling out",
			manifest: `
spec: {source: {targetRevision: "41.2.0"}}
status:
  health: {status: Progressing, message: "Caused by Service traefik"}
  sync: {status: Synced, revision: "41.2.0"}`,
			wantState:   kcmv1.ServiceStateDeployed,
			wantVersion: "41.2.0",
			wantHealth:  metav1.ConditionFalse,
		},
		{
			name: "sync failed",
			manifest: `
spec: {source: {targetRevision: "41.2.0"}}
status:
  health: {status: Missing}
  sync: {status: OutOfSync}
  operationState: {phase: Failed, message: "one or more objects failed to apply"}`,
			wantState:   kcmv1.ServiceStateFailed,
			wantFailure: "one or more objects failed to apply",
		},
		{
			name: "degraded",
			manifest: `
spec: {source: {targetRevision: "41.2.0"}}
status:
  health: {status: Degraded, message: "pod crash looping"}
  sync: {status: Synced, revision: "41.2.0"}`,
			wantState:   kcmv1.ServiceStateFailed,
			wantFailure: "pod crash looping",
		},
		{
			// Nothing to sync from: Argo CD never runs an operation, so the
			// verdict is only on the conditions.
			name: "chart cannot be resolved",
			manifest: `
spec: {source: {targetRevision: "41.2.0"}}
status:
  health: {}
  sync: {status: Unknown}
  conditions:
    - {type: ComparisonError, message: "chart traefik not found"}`,
			wantState:   kcmv1.ServiceStateFailed,
			wantFailure: "ComparisonError: chart traefik not found",
		},
		{
			// Just created; Argo CD has not looked at it yet.
			name:      "no status yet",
			manifest:  `spec: {source: {targetRevision: "41.2.0"}}`,
			wantState: kcmv1.ServiceStateProvisioning,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := serviceState(application(t, tc.manifest), svc, nil, now)
			require.Equal(t, tc.wantState, state.State)
			require.Equal(t, tc.wantVersion, state.Version)
			require.Equal(t, tc.wantFailure, state.FailureMessage)
			require.Equal(t, kcmv1.ServiceTypeHelm, state.Type)
			require.Equal(t, metav1.NewTime(now), *state.LastStateTransitionTime)
			if tc.wantHealth != "" {
				cond := apimeta.FindStatusCondition(state.Conditions, serviceHealthCondition)
				require.NotNil(t, cond, "health has to be reported even when it does not gate the state")
				require.Equal(t, tc.wantHealth, cond.Status)
			}
		})
	}
}

func Test_serviceState_transitionTime(t *testing.T) {
	svc := kcmv1.ServiceWithValues{Name: "traefik", Namespace: "traefik"}
	deployed := application(t, `
spec: {source: {targetRevision: "41.2.0"}}
status:
  health: {status: Healthy}
  sync: {status: Synced, revision: "41.2.0"}`)

	first := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	state := serviceState(deployed, svc, nil, first)
	require.Equal(t, metav1.NewTime(first), *state.LastStateTransitionTime)

	// Same state an hour later: the stamp is when it became Deployed, not when
	// it was last looked at.
	later := first.Add(time.Hour)
	state = serviceState(deployed, svc, &state, later)
	require.Equal(t, metav1.NewTime(first), *state.LastStateTransitionTime)

	degraded := application(t, `
spec: {source: {targetRevision: "41.2.0"}}
status:
  health: {status: Degraded, message: boom}
  sync: {status: Synced}`)
	state = serviceState(degraded, svc, &state, later)
	require.Equal(t, kcmv1.ServiceStateFailed, state.State)
	require.Equal(t, metav1.NewTime(later), *state.LastStateTransitionTime)
}
