// Copyright 2025
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

package flux

import (
	"testing"
	"time"

	helmcontrollerv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

// These are the ServiceSet's own contract rather than Flux's: its conditions,
// its cluster reference, the services it never got to, and which provider the
// adapter answers for. Every adapter carries a copy, so every copy has to
// behave the same way.

var testTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestLabelsMatchSelector(t *testing.T) {
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{
		"ksm.k0rdent.mirantis.com/adapter": "kcm-controller-manager-flux",
	}}

	match, err := labelsMatchSelector(map[string]string{
		"ksm.k0rdent.mirantis.com/adapter": "kcm-controller-manager-flux",
		"unrelated":                        "label",
	}, selector)
	require.NoError(t, err)
	require.True(t, match)

	match, err = labelsMatchSelector(map[string]string{
		"ksm.k0rdent.mirantis.com/adapter": "kcm-controller-manager",
	}, selector)
	require.NoError(t, err)
	require.False(t, match, "another adapter's ServiceSet")

	// No selector is not "match nothing": a provider without one has not
	// narrowed anything down.
	match, err = labelsMatchSelector(nil, nil)
	require.NoError(t, err)
	require.True(t, match)

	_, err = labelsMatchSelector(nil, &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "k", Operator: "Bogus"}},
	})
	require.Error(t, err)
}

func TestConditionHelpers(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{ObjectMeta: metav1.ObjectMeta{Generation: 5}}

	// A condition that is not there yet starts out Unknown, so the first
	// update always counts as a change.
	fresh := findCondition(serviceSet, kcmv1.ServiceSetProfileCondition)
	require.Equal(t, metav1.ConditionUnknown, fresh.Status)

	require.True(t, updateCondition(serviceSet, fresh, metav1.ConditionTrue,
		kcmv1.ServiceSetProfileReadyReason, kcmv1.ServiceSetProfileReadyMessage, testTime))
	require.Len(t, serviceSet.Status.Conditions, 1)
	require.Equal(t, int64(5), serviceSet.Status.Conditions[0].ObservedGeneration)

	found := findCondition(serviceSet, kcmv1.ServiceSetProfileCondition)
	require.Equal(t, metav1.ConditionTrue, found.Status)
	require.Equal(t, kcmv1.ServiceSetProfileReadyReason, found.Reason)

	require.True(t, updateCondition(serviceSet, found, metav1.ConditionFalse,
		kcmv1.ServiceSetProfileNotReadyReason, "no", testTime))
	require.Equal(t, metav1.ConditionFalse,
		findCondition(serviceSet, kcmv1.ServiceSetProfileCondition).Status)
}

func TestConditionStatusChangedToFalse(t *testing.T) {
	tru := &metav1.Condition{Status: metav1.ConditionTrue}
	fls := &metav1.Condition{Status: metav1.ConditionFalse}

	require.True(t, conditionStatusChangedToFalse(tru, fls), "true -> false is the event")
	require.True(t, conditionStatusChangedToFalse(nil, fls), "so is arriving already false")
	require.False(t, conditionStatusChangedToFalse(fls, fls), "still false is not a new failure")
	require.False(t, conditionStatusChangedToFalse(fls, tru))
	require.False(t, conditionStatusChangedToFalse(tru, nil))
}

func TestFillNotDeployedServices(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{
		Spec: kcmv1.ServiceSetSpec{Services: []kcmv1.ServiceWithValues{
			{Name: "traefik", Namespace: "traefik", Template: "traefik-41-2-0"},
			{Name: "cert-manager", Namespace: "cert-manager", Template: "cert-manager-1-20-2"},
		}},
	}
	serviceSet.Status.Services = []kcmv1.ServiceState{{
		Name: "traefik", Namespace: "traefik", State: kcmv1.ServiceStateDeployed,
	}}

	fillNotDeployedServices(serviceSet, func() time.Time { return testTime })

	require.Len(t, serviceSet.Status.Services, 2)
	require.Equal(t, kcmv1.ServiceStateDeployed, serviceSet.Status.Services[0].State,
		"a service that has a state keeps it")
	require.Equal(t, "cert-manager", serviceSet.Status.Services[1].Name)
	require.Equal(t, kcmv1.ServiceStateNotDeployed, serviceSet.Status.Services[1].State)
	require.Equal(t, kcmv1.ServiceTypeHelm, serviceSet.Status.Services[1].Type)

	// Idempotent: a second pass adds nothing.
	fillNotDeployedServices(serviceSet, func() time.Time { return testTime })
	require.Len(t, serviceSet.Status.Services, 2)
}

func TestClusterReference(t *testing.T) {
	// Self-management has no ClusterDeployment to point at.
	self := clusterReference(&kcmv1.ServiceSet{
		Spec: kcmv1.ServiceSetSpec{Provider: kcmv1.StateManagementProviderConfig{SelfManagement: true}},
	})
	require.NotNil(t, self)
	require.NotEqual(t, kcmv1.ClusterDeploymentKind, self.Kind)

	child := clusterReference(&kcmv1.ServiceSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kcm-system"},
		Spec:       kcmv1.ServiceSetSpec{Cluster: "child"},
	})
	require.Equal(t, kcmv1.ClusterDeploymentKind, child.Kind)
	require.Equal(t, "child", child.Name)
	require.Equal(t, "kcm-system", child.Namespace)
}

func TestServes(t *testing.T) {
	r := &ServiceSetReconciler{
		AdapterName:      "kcm-controller-manager",
		AdapterNamespace: "kcm-system",
		ProviderName:     "ksm-flux",
	}
	provider := func(name, adapter, namespace string) *kcmv1.StateManagementProvider {
		return &kcmv1.StateManagementProvider{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: kcmv1.StateManagementProviderSpec{
				Adapter: kcmv1.ResourceReference{Name: adapter, Namespace: namespace},
			},
		}
	}

	require.True(t, r.serves(provider("ksm-flux", "kcm-controller-manager", "kcm-system")))
	require.False(t, r.serves(provider("ksm-argocd", "kcm-controller-manager", "kcm-system")),
		"adapters share one Deployment, so the provider name is the discriminator")
	require.False(t, r.serves(provider("ksm-flux", "someone-else", "kcm-system")))
	require.False(t, r.serves(provider("ksm-flux", "kcm-controller-manager", "elsewhere")))
}

func TestDeployedChartVersion(t *testing.T) {
	// The newest entry is what is on the cluster now.
	hr := &helmcontrollerv2.HelmRelease{Status: helmcontrollerv2.HelmReleaseStatus{
		History: helmcontrollerv2.Snapshots{
			{Version: 2, ChartVersion: "1.21.1"},
			{Version: 1, ChartVersion: "1.20.2"},
		},
	}}
	require.Equal(t, "1.21.1", deployedChartVersion(hr))

	// Nothing installed yet: no version to stamp, rather than a wrong one.
	require.Empty(t, deployedChartVersion(&helmcontrollerv2.HelmRelease{}))
}

func TestIsTerminalFailureReason(t *testing.T) {
	for _, reason := range []string{
		helmcontrollerv2.InstallFailedReason,
		helmcontrollerv2.UpgradeFailedReason,
		helmcontrollerv2.TestFailedReason,
		helmcontrollerv2.RollbackFailedReason,
		helmcontrollerv2.UninstallFailedReason,
		helmcontrollerv2.ArtifactFailedReason,
	} {
		require.True(t, isTerminalFailureReason(reason), reason)
	}
	// An attempt still in flight is not a failure; calling it one would make
	// every install flap through Failed on its way up.
	for _, reason := range []string{
		"Progressing", "Reconciling", helmcontrollerv2.DependencyNotReadyReason,
		helmcontrollerv2.InstallSucceededReason, "",
	} {
		require.False(t, isTerminalFailureReason(reason), reason)
	}
}

func TestReleaseFailureMessage(t *testing.T) {
	fallback := &metav1.Condition{Message: "Helm rollback succeeded"}

	// Released carries why the attempt failed; the remediation only says what
	// was done about it.
	hr := &helmcontrollerv2.HelmRelease{Status: helmcontrollerv2.HelmReleaseStatus{
		Conditions: []metav1.Condition{{
			Type: helmcontrollerv2.ReleasedCondition, Status: metav1.ConditionFalse,
			Message: "replicas: Invalid value: -1",
		}},
	}}
	require.Equal(t, "replicas: Invalid value: -1", releaseFailureMessage(hr, fallback))

	// Nothing to prefer: the remediation's own message is all there is.
	require.Equal(t, "Helm rollback succeeded",
		releaseFailureMessage(&helmcontrollerv2.HelmRelease{}, fallback))

	// A Released that is true describes the attempt that worked, so it is not
	// the reason for anything.
	ok := &helmcontrollerv2.HelmRelease{Status: helmcontrollerv2.HelmReleaseStatus{
		Conditions: []metav1.Condition{{
			Type: helmcontrollerv2.ReleasedCondition, Status: metav1.ConditionTrue, Message: "fine",
		}},
	}}
	require.Equal(t, "Helm rollback succeeded", releaseFailureMessage(ok, fallback))
}

func TestHelmOptionsFor(t *testing.T) {
	svcOnly := kcmv1.ServiceWithValues{HelmOptions: &kcmv1.ServiceHelmOptions{Description: "from service"}}
	require.Equal(t, "from service", helmOptionsFor(svcOnly, &kcmv1.ServiceTemplate{}).Description)

	// The template wins: it is the narrower statement about the chart.
	tmpl := &kcmv1.ServiceTemplate{Spec: kcmv1.ServiceTemplateSpec{
		HelmOptions: &kcmv1.ServiceHelmOptions{Description: "from template"},
	}}
	require.Equal(t, "from template", helmOptionsFor(svcOnly, tmpl).Description)

	require.Nil(t, helmOptionsFor(kcmv1.ServiceWithValues{}, &kcmv1.ServiceTemplate{}))
}

func TestApplyHelmOptions(t *testing.T) {
	// Defaults: the namespace is created, nothing else is turned off.
	hr := new(helmcontrollerv2.HelmRelease)
	applyHelmOptions(hr, nil)
	require.True(t, hr.Spec.Install.CreateNamespace)
	require.False(t, hr.Spec.Install.DisableWait)
	require.Nil(t, hr.Spec.Timeout)

	no, yes := false, true
	hr = new(helmcontrollerv2.HelmRelease)
	applyHelmOptions(hr, &kcmv1.ServiceHelmOptions{
		Wait:        &no,
		WaitForJobs: &no,
		SkipCRDs:    &yes,
		Atomic:      &yes,
		Timeout:     &metav1.Duration{Duration: time.Minute},
	})
	require.True(t, hr.Spec.Install.DisableWait)
	require.True(t, hr.Spec.Upgrade.DisableWait)
	require.True(t, hr.Spec.Install.DisableWaitForJobs)
	require.Equal(t, helmcontrollerv2.Skip, hr.Spec.Install.CRDs)
	require.Equal(t, time.Minute, hr.Spec.Timeout.Duration)

	// Helm's --atomic is a rollback on failure, which in Flux is a
	// remediation strategy rather than a flag.
	require.NotNil(t, hr.Spec.Upgrade.Remediation)
	require.NotNil(t, hr.Spec.Upgrade.Remediation.Strategy)
	require.Equal(t, helmcontrollerv2.RollbackRemediationStrategy, *hr.Spec.Upgrade.Remediation.Strategy)
	require.NotNil(t, hr.Spec.Install.Remediation)
}

func TestValuesHelpers(t *testing.T) {
	refs := valuesReferences([]kcmv1.ValuesFrom{
		{Kind: "ConfigMap", Name: "cm"}, {Kind: "Secret", Name: "sec"},
	})
	require.Equal(t, []helmcontrollerv2.ValuesReference{
		{Kind: "ConfigMap", Name: "cm"}, {Kind: "Secret", Name: "sec"},
	}, refs)
	require.Nil(t, valuesReferences(nil))

	values, err := valuesFromYAML("controller:\n  replicaCount: 2\n")
	require.NoError(t, err)
	require.JSONEq(t, `{"controller":{"replicaCount":2}}`, string(values.Raw))

	// No values is nil, not an empty document Flux would have to merge.
	values, err = valuesFromYAML("   \n")
	require.NoError(t, err)
	require.Nil(t, values)

	_, err = valuesFromYAML("not: [valid")
	require.Error(t, err)
}

func TestDriftDetection(t *testing.T) {
	require.Equal(t, helmcontrollerv2.DriftDetectionEnabled, driftDetection("enabled").Mode)
	require.Equal(t, helmcontrollerv2.DriftDetectionWarn, driftDetection("warn").Mode)
	// Unset and "disabled" both mean: say nothing, and let Flux default.
	require.Nil(t, driftDetection(""))
	require.Nil(t, driftDetection("disabled"))
}

func TestIsPaused(t *testing.T) {
	require.False(t, isPaused(&kcmv1.ServiceSet{}))
	require.True(t, isPaused(&kcmv1.ServiceSet{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{kcmv1.ServiceSetPausedAnnotation: ""},
	}}), "the annotation's presence is what pauses, whatever its value")
}

func TestSetChartSourceUnsupported(t *testing.T) {
	// Neither a chart KCM reconciles nor a Flux source on the template: there
	// is nothing to point the HelmRelease at, and saying so beats producing a
	// HelmRelease that can never resolve.
	err := setChartSource(new(helmcontrollerv2.HelmRelease), &kcmv1.ServiceTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "kcm-system"},
		Spec:       kcmv1.ServiceTemplateSpec{Helm: &kcmv1.HelmSpec{}},
	})
	require.Error(t, err)
}
