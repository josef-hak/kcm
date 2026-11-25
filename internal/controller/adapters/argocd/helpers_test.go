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

package argocd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

// These are the ServiceSet's own contract rather than Argo CD's: its
// conditions, the services it never got to, and which provider the adapter
// answers for. Every adapter carries a copy, so every copy has to behave the
// same way.

var testTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestLabelsMatchSelector(t *testing.T) {
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{
		"ksm.k0rdent.mirantis.com/adapter": "kcm-controller-manager-argocd",
	}}

	match, err := labelsMatchSelector(map[string]string{
		"ksm.k0rdent.mirantis.com/adapter": "kcm-controller-manager-argocd",
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

func TestFindCondition(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{Generation: 7}
	serviceSet.Status.Conditions = []metav1.Condition{{
		Type: kcmv1.ServiceSetProfileCondition, Status: metav1.ConditionTrue, Reason: "Ready",
	}}

	found := findCondition(serviceSet, kcmv1.ServiceSetProfileCondition)
	require.Equal(t, metav1.ConditionTrue, found.Status)
	require.Equal(t, "Ready", found.Reason)

	// A condition that is not there yet starts out false, carrying the
	// generation it was observed at.
	fresh := findCondition(serviceSet, kcmv1.ServiceSetStatusesCollectedCondition)
	require.Equal(t, kcmv1.ServiceSetStatusesCollectedCondition, fresh.Type)
	require.Equal(t, metav1.ConditionFalse, fresh.Status)
	require.Equal(t, int64(7), fresh.ObservedGeneration)
}

func TestUpdateCondition(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{Generation: 3}

	changed := updateCondition(serviceSet, findCondition(serviceSet, kcmv1.ServiceSetProfileCondition),
		metav1.ConditionTrue, "Ready", "all good", testTime)
	require.True(t, changed)
	require.Len(t, serviceSet.Status.Conditions, 1)
	require.Equal(t, int64(3), serviceSet.Status.Conditions[0].ObservedGeneration)
	require.Equal(t, testTime, serviceSet.Status.Conditions[0].LastTransitionTime.Time)

	// Restating the same thing is not a transition.
	later := testTime.Add(time.Hour)
	updateCondition(serviceSet, findCondition(serviceSet, kcmv1.ServiceSetProfileCondition),
		metav1.ConditionTrue, "Ready", "all good", later)
	require.Equal(t, testTime, serviceSet.Status.Conditions[0].LastTransitionTime.Time,
		"an unchanged condition must keep its transition time")

	// A new message is.
	updateCondition(serviceSet, findCondition(serviceSet, kcmv1.ServiceSetProfileCondition),
		metav1.ConditionFalse, "Broken", "it is not", later)
	require.Equal(t, metav1.ConditionFalse, serviceSet.Status.Conditions[0].Status)
	require.Equal(t, later, serviceSet.Status.Conditions[0].LastTransitionTime.Time)
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
	require.Equal(t, "cert-manager-1-20-2", serviceSet.Status.Services[1].Template)

	// Idempotent: a second pass adds nothing.
	fillNotDeployedServices(serviceSet, func() time.Time { return testTime })
	require.Len(t, serviceSet.Status.Services, 2)
}

func TestServes(t *testing.T) {
	r := &ServiceSetReconciler{
		AdapterName:      "kcm-controller-manager",
		AdapterNamespace: "kcm-system",
		ProviderName:     "ksm-argocd",
	}
	provider := func(name, adapter, namespace string) *kcmv1.StateManagementProvider {
		return &kcmv1.StateManagementProvider{
			Name: name,
			Spec: kcmv1.StateManagementProviderSpec{
				Adapter: kcmv1.ResourceReference{Name: adapter, Namespace: namespace},
			},
		}
	}

	require.True(t, r.serves(provider("ksm-argocd", "kcm-controller-manager", "kcm-system")))
	require.False(t, r.serves(provider("ksm-flux", "kcm-controller-manager", "kcm-system")),
		"adapters share one Deployment, so the provider name is the discriminator")
	require.False(t, r.serves(provider("ksm-argocd", "someone-else", "kcm-system")))
	require.False(t, r.serves(provider("ksm-argocd", "kcm-controller-manager", "elsewhere")))
}

func TestFindServiceState(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{}
	serviceSet.Status.Services = []kcmv1.ServiceState{
		{Name: "a", Namespace: "ns1", State: kcmv1.ServiceStateDeployed},
		{Name: "a", Namespace: "ns2", State: kcmv1.ServiceStateFailed},
	}

	// Name alone is not an identity: the same release name in two namespaces
	// is two services.
	found := findServiceState(serviceSet, kcmv1.ServiceWithValues{Name: "a", Namespace: "ns2"})
	require.NotNil(t, found)
	require.Equal(t, kcmv1.ServiceStateFailed, found.State)

	require.Nil(t, findServiceState(serviceSet, kcmv1.ServiceWithValues{Name: "a", Namespace: "ns3"}))

	// The pointer is into the status, so a caller can write through it.
	found.State = kcmv1.ServiceStateDeployed
	require.Equal(t, kcmv1.ServiceStateDeployed, serviceSet.Status.Services[1].State)
}

func TestApplicationLabelsAndSelector(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{
		Name: "set", Namespace: "kcm-system",
	}

	// A service with no namespace is deployed into "default", and the label
	// has to say so or the Application cannot be found again by the service
	// the status is being collected for.
	labels := applicationLabels(serviceSet, kcmv1.ServiceWithValues{Name: "svc"})
	require.Equal(t, "default", labels[labelServiceNamespace])
	require.Equal(t, "svc", labels[labelServiceName])
	require.Equal(t, kcmv1.KCMManagedLabelValue, labels[kcmv1.KCMManagedLabelKey])

	// The selector is the subset that identifies the ServiceSet, so it has to
	// match what the labels carry for it.
	selector := serviceSetSelector(serviceSet)
	for k, v := range selector {
		require.Equal(t, v, labels[k], "selector key %q must match the Application's own label", k)
	}
	require.NotContains(t, selector, labelServiceName, "the selector spans every service of the set")
}

func TestClusterSecretNameIsPerCluster(t *testing.T) {
	a := clusterSecretName(&kcmv1.ServiceSet{
		Namespace: "ns",
		Spec:      kcmv1.ServiceSetSpec{Cluster: "one"},
	})
	b := clusterSecretName(&kcmv1.ServiceSet{
		Namespace: "ns",
		Spec:      kcmv1.ServiceSetSpec{Cluster: "two"},
	})
	require.NotEqual(t, a, b)
	require.LessOrEqual(t, len(a), 253)
}

func TestDecodeBasicAuth(t *testing.T) {
	creds, err := decodeBasicAuth("dXNlcjpwYXNz") // user:pass
	require.NoError(t, err)
	require.Equal(t, [2]string{"user", "pass"}, creds)

	// A password with a colon in it still belongs to the password.
	creds, err = decodeBasicAuth("dXNlcjpwYTpzcw==") // user:pa:ss
	require.NoError(t, err)
	require.Equal(t, [2]string{"user", "pa:ss"}, creds)

	_, err = decodeBasicAuth("not base64 at all")
	require.Error(t, err)

	_, err = decodeBasicAuth("bm9jb2xvbg==") // nocolon
	require.Error(t, err)
}

func TestSecretDataEqual(t *testing.T) {
	a := map[string][]byte{"url": []byte("https://example.test"), "type": []byte("helm")}
	require.True(t, secretDataEqual(a, map[string][]byte{
		"url": []byte("https://example.test"), "type": []byte("helm"),
	}))
	require.False(t, secretDataEqual(a, map[string][]byte{"url": []byte("https://other.test"), "type": []byte("helm")}))
	require.False(t, secretDataEqual(a, map[string][]byte{"url": []byte("https://example.test")}),
		"a missing key is a difference")
	require.True(t, secretDataEqual(nil, nil))
}

func TestSeedServiceState(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{}
	svc := kcmv1.ServiceWithValues{Name: "svc", Namespace: "ns", Template: "tmpl"}

	seedServiceState(serviceSet, svc, func() time.Time { return testTime })
	require.Len(t, serviceSet.Status.Services, 1)
	require.Equal(t, kcmv1.ServiceStateProvisioning, serviceSet.Status.Services[0].State)
	require.Equal(t, kcmv1.ServiceTypeHelm, serviceSet.Status.Services[0].Type)

	// Seeding again must not overwrite what Argo CD has since reported.
	serviceSet.Status.Services[0].State = kcmv1.ServiceStateDeployed
	seedServiceState(serviceSet, svc, func() time.Time { return testTime })
	require.Len(t, serviceSet.Status.Services, 1)
	require.Equal(t, kcmv1.ServiceStateDeployed, serviceSet.Status.Services[0].State)
}

func TestUpdateServicesInReadyStateCondition(t *testing.T) {
	r := &ServiceSetReconciler{timeFunc: func() time.Time { return testTime }}
	serviceSet := &kcmv1.ServiceSet{
		Spec: kcmv1.ServiceSetSpec{Services: []kcmv1.ServiceWithValues{
			{Name: "a", Namespace: "ns"}, {Name: "b", Namespace: "ns"},
		}},
	}
	serviceSet.Status.Services = []kcmv1.ServiceState{
		{Name: "a", Namespace: "ns", State: kcmv1.ServiceStateDeployed},
		{Name: "b", Namespace: "ns", State: kcmv1.ServiceStateProvisioning},
	}

	r.updateServicesInReadyStateCondition(serviceSet)
	cond := findCondition(serviceSet, kcmv1.ServicesInReadyStateCondition)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "1/2", cond.Message)

	serviceSet.Status.Services[1].State = kcmv1.ServiceStateDeployed
	r.updateServicesInReadyStateCondition(serviceSet)
	cond = findCondition(serviceSet, kcmv1.ServicesInReadyStateCondition)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Equal(t, "2/2", cond.Message)
}
