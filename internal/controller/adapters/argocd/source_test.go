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

	helmcontrollerv2 "github.com/fluxcd/helm-controller/api/v2"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	testscheme "github.com/K0rdent/kcm/test/scheme"
)

const testNamespace = "kcm-system"

func helmChart(name, chart, version, repoKind, repoName string, artifactRevision string) *sourcev1.HelmChart {
	hc := &sourcev1.HelmChart{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: sourcev1.HelmChartSpec{
			Chart:     chart,
			Version:   version,
			SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: repoKind, Name: repoName},
		},
	}
	if artifactRevision != "" {
		hc.Status.Artifact = &fluxmeta.Artifact{Revision: artifactRevision}
	}
	return hc
}

func serviceTemplate(chartRefName string) *kcmv1.ServiceTemplate {
	return &kcmv1.ServiceTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "traefik-41-2-0", Namespace: testNamespace},
		Spec: kcmv1.ServiceTemplateSpec{
			Helm: &kcmv1.HelmSpec{ChartSpec: &sourcev1.HelmChartSpec{Chart: "traefik", Version: "41.2.0"}},
		},
		Status: kcmv1.ServiceTemplateStatus{
			TemplateStatusCommon: kcmv1.TemplateStatusCommon{
				Valid:    true,
				ChartRef: &helmcontrollerv2.CrossNamespaceSourceReference{Name: chartRefName, Namespace: testNamespace},
			},
		},
	}
}

func Test_sourceForService(t *testing.T) {
	for _, tc := range []struct {
		name         string
		objects      []client.Object
		wantRepoURL  string
		wantChart    string
		wantPath     string
		wantRevision string
		wantOCI      bool
	}{
		{
			// The shape every KSM scenario uses: a chart in an OCI registry.
			// Argo CD takes the registry without the scheme.
			name: "OCI repository",
			objects: []client.Object{
				helmChart("chart-traefik", "traefik", "41.2.0", sourcev1.HelmRepositoryKind, "catalog", ""),
				&sourcev1.HelmRepository{
					ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: testNamespace},
					Spec:       sourcev1.HelmRepositorySpec{Type: "oci", URL: "oci://ghcr.io/k0rdent/catalog/charts"},
				},
			},
			wantRepoURL:  "ghcr.io/k0rdent/catalog/charts",
			wantChart:    "traefik",
			wantRevision: "41.2.0",
			wantOCI:      true,
		},
		{
			name: "classic HTTP repository",
			objects: []client.Object{
				helmChart("chart-traefik", "traefik", "41.2.0", sourcev1.HelmRepositoryKind, "catalog", ""),
				&sourcev1.HelmRepository{
					ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: testNamespace},
					Spec:       sourcev1.HelmRepositorySpec{URL: "https://charts.example.com"},
				},
			},
			wantRepoURL:  "https://charts.example.com",
			wantChart:    "traefik",
			wantRevision: "41.2.0",
		},
		{
			// A range in the spec resolves to one version on the artifact, and
			// that is the one the cluster actually got.
			name: "artifact revision wins over the spec version",
			objects: []client.Object{
				helmChart("chart-traefik", "traefik", ">=41.0.0", sourcev1.HelmRepositoryKind, "catalog", "41.2.0"),
				&sourcev1.HelmRepository{
					ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: testNamespace},
					Spec:       sourcev1.HelmRepositorySpec{Type: "oci", URL: "oci://ghcr.io/k0rdent/catalog/charts"},
				},
			},
			wantRepoURL:  "ghcr.io/k0rdent/catalog/charts",
			wantChart:    "traefik",
			wantRevision: "41.2.0",
			wantOCI:      true,
		},
		{
			// A git chart is a path in a repository, not a chart name.
			name: "git repository",
			objects: []client.Object{
				helmChart("chart-traefik", "charts/traefik", "41.2.0", sourcev1.GitRepositoryKind, "catalog-git", ""),
				&sourcev1.GitRepository{
					ObjectMeta: metav1.ObjectMeta{Name: "catalog-git", Namespace: testNamespace},
					Spec: sourcev1.GitRepositorySpec{
						URL:       "https://github.com/k0rdent/catalog.git",
						Reference: &sourcev1.GitRepositoryRef{Tag: "v1.2.3"},
					},
				},
			},
			wantRepoURL:  "https://github.com/k0rdent/catalog.git",
			wantPath:     "charts/traefik",
			wantRevision: "v1.2.3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := fake.NewClientBuilder().WithScheme(testscheme.Scheme).WithObjects(tc.objects...).Build()
			svc := kcmv1.ServiceWithValues{Name: "traefik", Namespace: "traefik", Template: "traefik-41-2-0"}

			src, repo, err := sourceForService(t.Context(), cl, testNamespace, svc, serviceTemplate("chart-traefik"))
			require.NoError(t, err)
			require.Equal(t, tc.wantRepoURL, src.RepoURL)
			require.Equal(t, tc.wantChart, src.Chart)
			require.Equal(t, tc.wantPath, src.Path)
			require.Equal(t, tc.wantRevision, src.TargetRevision)
			require.Equal(t, "traefik", src.Helm.ReleaseName)
			require.Equal(t, "traefik", src.Helm.Namespace)
			require.Equal(t, tc.wantOCI, repo.oci)
		})
	}
}

func Test_sourceForService_unsupported(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(testscheme.Scheme).Build()
	svc := kcmv1.ServiceWithValues{Name: "traefik", Namespace: "traefik"}

	for _, tc := range []struct {
		tmpl *kcmv1.ServiceTemplate
		name string
	}{
		{
			name: "kustomize template",
			tmpl: &kcmv1.ServiceTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "kust", Namespace: testNamespace},
				Spec:       kcmv1.ServiceTemplateSpec{Kustomize: &kcmv1.SourceSpec{Path: "."}},
			},
		},
		{
			// The chart lives in a Flux source object in the management
			// cluster, which Argo CD's repo server cannot read.
			name: "chart from a flux source",
			tmpl: &kcmv1.ServiceTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "fluxsrc", Namespace: testNamespace},
				Spec:       kcmv1.ServiceTemplateSpec{Helm: &kcmv1.HelmSpec{ChartSource: &kcmv1.SourceSpec{Path: "charts/traefik"}}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := sourceForService(t.Context(), cl, testNamespace, svc, tc.tmpl)
			require.ErrorIs(t, err, errUnsupportedServiceTemplate)
		})
	}
}

func Test_repositorySecretName(t *testing.T) {
	a := repositorySecretName("ghcr.io/k0rdent/catalog/charts")
	require.Equal(t, a, repositorySecretName("ghcr.io/k0rdent/catalog/charts"), "same URL must give the same name")
	require.NotEqual(t, a, repositorySecretName("ghcr.io/k0rdent/other/charts"))
	require.Regexp(t, `^kcm-repo-[0-9a-f]{16}$`, a)
}
