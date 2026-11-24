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
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	helmutil "github.com/K0rdent/kcm/internal/util/helm"
)

var errUnsupportedServiceTemplate = errors.New("unsupported ServiceTemplate for the Argo CD provider")

// helmRepository is what the adapter has to teach Argo CD about a chart's
// origin on top of the Application source itself: a repository secret, needed
// whenever the repository is not anonymously readable over plain HTTPS.
type helmRepository struct {
	secretRef *fluxmeta.LocalObjectReference
	url       string
	oci       bool
}

// sourceForService translates the chart a ServiceTemplate points at into an
// Argo CD Application source.
//
// Only Helm templates are supported. Kustomize and plain-resource templates
// describe a Flux source object that lives in the management cluster, which
// Argo CD's repo server has no way to read; they are rejected by name rather
// than silently skipped.
func sourceForService(
	ctx context.Context,
	c client.Client,
	namespace string,
	svc kcmv1.ServiceWithValues,
	tmpl *kcmv1.ServiceTemplate,
) (*applicationSource, *helmRepository, error) {
	switch {
	case tmpl.Spec.Helm == nil:
		return nil, nil, fmt.Errorf("%w: ServiceTemplate %s/%s is not backed by a Helm chart",
			errUnsupportedServiceTemplate, tmpl.Namespace, tmpl.Name)
	case tmpl.Spec.Helm.ChartSource != nil:
		return nil, nil, fmt.Errorf("%w: ServiceTemplate %s/%s installs from a Flux source, which Argo CD cannot read",
			errUnsupportedServiceTemplate, tmpl.Namespace, tmpl.Name)
	}

	if !tmpl.Status.Valid {
		return nil, nil, fmt.Errorf("ServiceTemplate %s/%s is invalid", tmpl.Namespace, tmpl.Name)
	}
	if tmpl.GetCommonStatus() == nil || tmpl.GetCommonStatus().ChartRef == nil {
		return nil, nil, fmt.Errorf("status for ServiceTemplate %s/%s has not been updated yet", tmpl.Namespace, tmpl.Name)
	}

	chart := new(sourcev1.HelmChart)
	chartRef := client.ObjectKey{
		Namespace: tmpl.GetCommonStatus().ChartRef.Namespace,
		Name:      tmpl.GetCommonStatus().ChartRef.Name,
	}
	if err := c.Get(ctx, chartRef, chart); err != nil {
		return nil, nil, fmt.Errorf("failed to get HelmChart %s referenced by ServiceTemplate %s/%s: %w",
			chartRef, tmpl.Namespace, tmpl.Name, err)
	}

	version, err := chartVersion(chart)
	if err != nil {
		return nil, nil, err
	}

	source := &applicationSource{
		TargetRevision: version,
		Helm: &helmSource{
			ReleaseName: svc.Name,
			// Argo CD renders the chart against the destination namespace by
			// default, which is what the service asked for; naming it here
			// keeps `.Release.Namespace` right for charts that read it.
			Namespace: effectiveNamespace(svc.Namespace),
		},
	}
	if tmpl.Spec.HelmOptions != nil && tmpl.Spec.HelmOptions.SkipCRDs != nil {
		source.Helm.SkipCrds = *tmpl.Spec.HelmOptions.SkipCRDs
	}
	if svc.HelmOptions != nil && svc.HelmOptions.SkipCRDs != nil {
		source.Helm.SkipCrds = *svc.HelmOptions.SkipCRDs
	}

	repoRef := client.ObjectKey{Namespace: chart.Namespace, Name: chart.Spec.SourceRef.Name}
	switch chart.Spec.SourceRef.Kind {
	case sourcev1.HelmRepositoryKind:
		repo := new(sourcev1.HelmRepository)
		if err := c.Get(ctx, repoRef, repo); err != nil {
			return nil, nil, fmt.Errorf("failed to get HelmRepository %s: %w", repoRef, err)
		}
		oci := repo.Spec.Type == helmutil.RegistryTypeOCI
		// Argo CD takes an OCI chart repository without the scheme; see
		// https://argo-cd.readthedocs.io/en/stable/user-guide/helm/.
		source.RepoURL = strings.TrimSuffix(strings.TrimPrefix(repo.Spec.URL, "oci://"), "/")
		source.Chart = chart.Spec.Chart
		return source, &helmRepository{url: source.RepoURL, oci: oci, secretRef: repo.Spec.SecretRef}, nil

	case sourcev1.GitRepositoryKind:
		repo := new(sourcev1.GitRepository)
		if err := c.Get(ctx, repoRef, repo); err != nil {
			return nil, nil, fmt.Errorf("failed to get GitRepository %s: %w", repoRef, err)
		}
		source.RepoURL = repo.Spec.URL
		source.Path = chart.Spec.Chart
		source.TargetRevision = gitRevision(repo)
		return source, &helmRepository{url: repo.Spec.URL, secretRef: repo.Spec.SecretRef}, nil

	default:
		return nil, nil, fmt.Errorf("%w: unsupported HelmChart source kind %s", errUnsupportedServiceTemplate, chart.Spec.SourceRef.Kind)
	}
}

// chartVersion is the version Argo CD should pull, preferring the revision
// Flux resolved over the (possibly range-valued) one in the spec.
func chartVersion(chart *sourcev1.HelmChart) (string, error) {
	version := chart.Spec.Version
	if chart.Status.Artifact != nil && chart.Status.Artifact.Revision != "" {
		if _, err := semver.NewVersion(chart.Status.Artifact.Revision); err == nil {
			version = chart.Status.Artifact.Revision
		}
	}
	if _, err := semver.NewVersion(version); err != nil {
		return "", fmt.Errorf("failed to determine version %q of HelmChart %s/%s: %w", version, chart.Namespace, chart.Name, err)
	}
	return version, nil
}

func gitRevision(repo *sourcev1.GitRepository) string {
	if repo.Spec.Reference == nil {
		return "HEAD"
	}
	switch {
	case repo.Spec.Reference.Tag != "":
		return repo.Spec.Reference.Tag
	case repo.Spec.Reference.Commit != "":
		return repo.Spec.Reference.Commit
	case repo.Spec.Reference.Branch != "":
		return repo.Spec.Reference.Branch
	default:
		return "HEAD"
	}
}

// repositorySecretName derives a stable, collision-free secret name from the
// repository URL: Argo CD matches repositories by the url field, not by name,
// so the name only has to be unique and reproducible.
func repositorySecretName(url string) string {
	sum := sha256.Sum256([]byte(url))
	return "kcm-repo-" + hex.EncodeToString(sum[:])[:16]
}

// ensureRepository registers a chart repository with Argo CD when it needs
// more than an anonymous HTTPS fetch to be read: OCI registries and any
// repository Flux holds credentials for.
func (r *ServiceSetReconciler) ensureRepository(ctx context.Context, namespace string, repo *helmRepository) error {
	if repo == nil || (!repo.oci && repo.secretRef == nil) {
		return nil
	}

	data := map[string][]byte{
		"type": []byte("helm"),
		"url":  []byte(repo.url),
		"name": []byte(repositorySecretName(repo.url)),
	}
	if repo.oci {
		data["enableOCI"] = []byte("true")
	}

	if repo.secretRef != nil {
		username, password, err := repositoryCredentials(ctx, r.Client, namespace, repo)
		if err != nil {
			return err
		}
		if username != "" {
			data["username"] = []byte(username)
		}
		if password != "" {
			data["password"] = []byte(password)
		}
	}

	secret := &corev1.Secret{}
	secret.Name = repositorySecretName(repo.url)
	secret.Namespace = r.ArgoCDNamespace
	key := client.ObjectKeyFromObject(secret)
	err := r.Client.Get(ctx, key, secret)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to get repository Secret %s: %w", key, err)
	}

	labels := map[string]string{
		argoSecretTypeLabel:      argoSecretTypeRepo,
		kcmv1.KCMManagedLabelKey: kcmv1.KCMManagedLabelValue,
	}
	if err != nil {
		secret.Name = key.Name
		secret.Namespace = key.Namespace
		secret.Labels = labels
		secret.Data = data
		if err := r.Client.Create(ctx, secret); client.IgnoreAlreadyExists(err) != nil {
			return fmt.Errorf("failed to create repository Secret %s: %w", key, err)
		}
		return nil
	}

	if secretDataEqual(secret.Data, data) && secret.Labels[argoSecretTypeLabel] == argoSecretTypeRepo {
		return nil
	}
	secret.Labels = labels
	secret.Data = data
	if err := r.Client.Update(ctx, secret); err != nil {
		return fmt.Errorf("failed to update repository Secret %s: %w", key, err)
	}
	return nil
}

// repositoryCredentials reads the basic-auth pair out of the secret Flux uses
// for the same repository. Both shapes Flux accepts are handled: the
// username/password pair, and the docker config a registry login produces.
func repositoryCredentials(ctx context.Context, c client.Client, namespace string, repo *helmRepository) (username, password string, _ error) {
	key := client.ObjectKey{Namespace: namespace, Name: repo.secretRef.Name}
	secret := new(corev1.Secret)
	if err := c.Get(ctx, key, secret); err != nil {
		return "", "", fmt.Errorf("failed to get repository credentials Secret %s: %w", key, err)
	}

	if len(secret.Data["username"]) > 0 || len(secret.Data["password"]) > 0 {
		return string(secret.Data["username"]), string(secret.Data["password"]), nil
	}

	dockerconfig, ok := secret.Data[corev1.DockerConfigJsonKey]
	if !ok {
		return "", "", fmt.Errorf("Secret %s has neither username/password nor %s", key, corev1.DockerConfigJsonKey)
	}

	var config struct {
		Auths map[string]struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Auth     string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(dockerconfig, &config); err != nil {
		return "", "", fmt.Errorf("failed to parse %s in Secret %s: %w", corev1.DockerConfigJsonKey, key, err)
	}

	registry, _, _ := strings.Cut(repo.url, "/")
	for host, auth := range config.Auths {
		if !strings.Contains(host, registry) {
			continue
		}
		if auth.Username != "" {
			return auth.Username, auth.Password, nil
		}
		decoded, err := decodeBasicAuth(auth.Auth)
		if err != nil {
			return "", "", fmt.Errorf("failed to decode auth for %s in Secret %s: %w", host, key, err)
		}
		return decoded[0], decoded[1], nil
	}

	return "", "", fmt.Errorf("Secret %s has no credentials for registry %s", key, registry)
}

func decodeBasicAuth(auth string) ([2]string, error) {
	raw, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return [2]string{}, err
	}
	username, password, found := strings.Cut(string(raw), ":")
	if !found {
		return [2]string{}, errors.New("auth is not in username:password form")
	}
	return [2]string{username, password}, nil
}

func secretDataEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		if string(a[k]) != string(v) {
			return false
		}
	}
	return true
}
