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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	testscheme "github.com/K0rdent/kcm/test/scheme"
)

func Test_applicationName(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{ObjectMeta: metav1.ObjectMeta{Name: "my-cluster", Namespace: "kcm-system"}}

	require.Equal(t, "kcm-system-my-cluster-traefik-traefik",
		applicationName(serviceSet, kcmv1.ServiceWithValues{Name: "traefik", Namespace: "traefik"}))

	// Applications of every ServiceSet share one namespace, so two services
	// of the same name in different namespaces must not collapse into one.
	require.NotEqual(t,
		applicationName(serviceSet, kcmv1.ServiceWithValues{Name: "traefik", Namespace: "a"}),
		applicationName(serviceSet, kcmv1.ServiceWithValues{Name: "traefik", Namespace: "b"}))

	long := strings.Repeat("x", 300)
	name := applicationName(serviceSet, kcmv1.ServiceWithValues{Name: long, Namespace: "traefik"})
	require.Len(t, name, 253)
	require.NotEqual(t, name,
		applicationName(serviceSet, kcmv1.ServiceWithValues{Name: long + "y", Namespace: "traefik"}))
}

func Test_providerConfig_syncPolicy(t *testing.T) {
	// Nothing configured: KCM promises a service listed in a ServiceSet
	// converges without anybody pressing a button.
	policy := providerConfig{}.syncPolicyFrom()
	require.NotNil(t, policy.Automated)
	require.True(t, policy.Automated.Prune)
	require.True(t, policy.Automated.SelfHeal)
	require.Equal(t, []string{"CreateNamespace=true"}, policy.SyncOptions)

	off := false
	require.Nil(t, providerConfig{Automated: &off}.syncPolicyFrom().Automated)

	policy = providerConfig{Prune: &off, SyncOptions: []string{"ServerSideApply=true"}}.syncPolicyFrom()
	require.False(t, policy.Automated.Prune)
	require.True(t, policy.Automated.SelfHeal)
	require.Equal(t, []string{"ServerSideApply=true"}, policy.SyncOptions)

	require.Equal(t, defaultProject, providerConfig{}.project())
	require.Equal(t, "kcm", providerConfig{Project: "kcm"}.project())
}

func Test_resolveValues(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "defaults", Namespace: testNamespace},
		Data:       map[string]string{"values.yaml": "traefik:\n  replicas: 1\n  image: base\n"},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: testNamespace},
		Data:       map[string][]byte{"values.yaml": []byte("traefik:\n  image: from-secret\n")},
	}
	cl := fake.NewClientBuilder().WithScheme(testscheme.Scheme).WithObjects(cm, secret).Build()

	svc := kcmv1.ServiceWithValues{
		Name: "traefik",
		ValuesFrom: []kcmv1.ValuesFrom{
			{Kind: kcmv1.ConfigMapKind, Name: "defaults"},
			{Kind: kcmv1.SecretKind, Name: "creds"},
		},
		Values: "traefik:\n  replicas: 3\n",
	}

	values, err := resolveValues(t.Context(), cl, testNamespace, svc)
	require.NoError(t, err)
	// Inline wins over every reference, later references win over earlier
	// ones, and keys nobody overrode survive the merge.
	require.YAMLEq(t, "traefik:\n  replicas: 3\n  image: from-secret\n", values)
}

func Test_resolveValues_empty(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(testscheme.Scheme).Build()
	values, err := resolveValues(t.Context(), cl, testNamespace, kcmv1.ServiceWithValues{Name: "traefik"})
	require.NoError(t, err)
	require.Empty(t, values)
}
