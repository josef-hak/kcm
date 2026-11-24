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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

// Argo CD API types are consumed as unstructured objects on purpose: pulling
// github.com/argoproj/argo-cd in as a module dependency drags the whole Argo
// server tree along with it, and the adapter only ever touches the handful of
// fields declared below.
const (
	argoAPIGroup   = "argoproj.io"
	argoAPIVersion = "v1alpha1"

	applicationKind  = "Application"
	appProjectKind   = "AppProject"
	applicationsName = "applications"
	appProjectsName  = "appprojects"

	// applicationFinalizer makes Argo CD delete the resources it applied to the
	// target cluster before the Application object itself goes away. Without it
	// a removed service would keep running on the cluster.
	applicationFinalizer = "resources-finalizer.argocd.argoproj.io"

	// defaultProject is the Argo CD project every generated Application lands
	// in unless the ServiceSet provider config names another one.
	defaultProject = "default"

	// inClusterServer is the destination Argo CD resolves to the cluster it
	// runs in, used for self-management ServiceSets.
	inClusterServer = "https://kubernetes.default.svc"
)

// Labels stamped on every object the adapter owns. They are what the adapter
// lists by: an Application lives in the Argo CD namespace, so it cannot carry
// an owner reference to a ServiceSet in another namespace.
const (
	labelServiceSetNamespace = "ksm.k0rdent.mirantis.com/serviceset-namespace"
	labelServiceSetName      = "ksm.k0rdent.mirantis.com/serviceset-name"
	labelServiceNamespace    = "ksm.k0rdent.mirantis.com/service-namespace"
	labelServiceName         = "ksm.k0rdent.mirantis.com/service-name"

	// argoSecretTypeLabel is how Argo CD discovers cluster and repository
	// credentials among the secrets in its namespace.
	argoSecretTypeLabel = "argocd.argoproj.io/secret-type"
	argoSecretTypeRepo  = "repository"
	argoSecretTypeClust = "cluster"
)

func applicationGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: argoAPIGroup, Version: argoAPIVersion, Kind: applicationKind}
}

func newApplication() *unstructured.Unstructured {
	u := new(unstructured.Unstructured)
	u.SetGroupVersionKind(applicationGVK())
	return u
}

func newApplicationList() *unstructured.UnstructuredList {
	l := new(unstructured.UnstructuredList)
	l.SetGroupVersionKind(applicationGVK().GroupVersion().WithKind(applicationKind + "List"))
	return l
}

// applicationSpec is the subset of the Argo CD Application spec the adapter
// writes. Everything it does not set is left to Argo CD's own defaults.
type applicationSpec struct {
	Source               *applicationSource     `json:"source,omitempty"`
	SyncPolicy           *syncPolicy            `json:"syncPolicy,omitempty"`
	RevisionHistoryLimit *int64                 `json:"revisionHistoryLimit,omitempty"`
	Project              string                 `json:"project"`
	IgnoreDifferences    []map[string]any       `json:"ignoreDifferences,omitempty"`
	Destination          applicationDestination `json:"destination"`
}

type applicationSource struct {
	Helm           *helmSource `json:"helm,omitempty"`
	RepoURL        string      `json:"repoURL"`
	Chart          string      `json:"chart,omitempty"`
	Path           string      `json:"path,omitempty"`
	TargetRevision string      `json:"targetRevision,omitempty"`
}

type helmSource struct {
	ReleaseName string `json:"releaseName,omitempty"`
	Values      string `json:"values,omitempty"`
	Namespace   string `json:"namespace,omitempty"`
	SkipCrds    bool   `json:"skipCrds,omitempty"`
}

type applicationDestination struct {
	Server    string `json:"server,omitempty"`
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace"`
}

type syncPolicy struct {
	Automated   *automatedSync `json:"automated,omitempty"`
	SyncOptions []string       `json:"syncOptions,omitempty"`
}

type automatedSync struct {
	Prune    bool `json:"prune"`
	SelfHeal bool `json:"selfHeal"`
}

// providerConfig is the ServiceSet's .spec.provider.config as this adapter
// reads it. Fields meant for another provider are ignored rather than
// rejected - the field is a free-form JSON shared by every adapter.
type providerConfig struct {
	Prune                *bool            `json:"prune,omitempty"`
	SelfHeal             *bool            `json:"selfHeal,omitempty"`
	Automated            *bool            `json:"automated,omitempty"`
	RevisionHistoryLimit *int64           `json:"revisionHistoryLimit,omitempty"`
	Project              string           `json:"project,omitempty"`
	SyncOptions          []string         `json:"syncOptions,omitempty"`
	IgnoreDifferences    []map[string]any `json:"ignoreDifferences,omitempty"`
}

// syncPolicyFrom turns the provider config into an Argo CD sync policy.
// Automated sync with prune and self-heal is the default: it is the mode that
// matches what KCM promises for a service listed in a ServiceSet - the cluster
// converges on the spec without anybody pressing a button.
func (c providerConfig) syncPolicyFrom() *syncPolicy {
	policy := &syncPolicy{
		SyncOptions: c.SyncOptions,
		Automated:   &automatedSync{Prune: true, SelfHeal: true},
	}
	if c.Automated != nil && !*c.Automated {
		policy.Automated = nil
	}
	if policy.Automated != nil {
		if c.Prune != nil {
			policy.Automated.Prune = *c.Prune
		}
		if c.SelfHeal != nil {
			policy.Automated.SelfHeal = *c.SelfHeal
		}
	}
	if len(policy.SyncOptions) == 0 {
		// The namespace a service asks for is rarely one that already exists
		// on a freshly provisioned cluster.
		policy.SyncOptions = []string{"CreateNamespace=true"}
	}
	return policy
}

func (c providerConfig) project() string {
	if c.Project != "" {
		return c.Project
	}
	return defaultProject
}

// applicationName is the name of the Application backing one service of one
// ServiceSet. Applications of every ServiceSet share the Argo CD namespace, so
// the name carries the ServiceSet's namespace and name as well as the
// service's own coordinates.
func applicationName(serviceSet *kcmv1.ServiceSet, svc kcmv1.ServiceWithValues) string {
	return truncateName(strings.Join([]string{
		serviceSet.Namespace,
		serviceSet.Name,
		effectiveNamespace(svc.Namespace),
		svc.Name,
	}, "-"))
}

// truncateName keeps a generated name within the 253 characters a Kubernetes
// object name allows, replacing the overflow with a digest of the full name so
// two long names cannot collapse into one.
func truncateName(name string) string {
	const maxNameLength = 253
	if len(name) <= maxNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return name[:maxNameLength-9] + "-" + hex.EncodeToString(sum[:])[:8]
}

func applicationLabels(serviceSet *kcmv1.ServiceSet, svc kcmv1.ServiceWithValues) map[string]string {
	return map[string]string{
		kcmv1.KCMManagedLabelKey: kcmv1.KCMManagedLabelValue,
		labelServiceSetNamespace: serviceSet.Namespace,
		labelServiceSetName:      serviceSet.Name,
		labelServiceNamespace:    effectiveNamespace(svc.Namespace),
		labelServiceName:         svc.Name,
	}
}

// serviceSetSelector matches every Application generated for one ServiceSet.
func serviceSetSelector(serviceSet *kcmv1.ServiceSet) map[string]string {
	return map[string]string{
		labelServiceSetNamespace: serviceSet.Namespace,
		labelServiceSetName:      serviceSet.Name,
	}
}

func toUnstructuredSpec(spec *applicationSpec) (map[string]any, error) {
	out, err := runtime.DefaultUnstructuredConverter.ToUnstructured(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Application spec: %w", err)
	}
	return out, nil
}

func effectiveNamespace(namespace string) string {
	if namespace == "" {
		return "default"
	}
	return namespace
}
