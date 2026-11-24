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
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	kubeutil "github.com/K0rdent/kcm/internal/util/kube"
)

// argoClusterConfig is the connection blob Argo CD reads out of the `config`
// key of a cluster secret.
type argoClusterConfig struct {
	BearerToken     string        `json:"bearerToken,omitempty"`
	TLSClientConfig argoTLSConfig `json:"tlsClientConfig"`
}

type argoTLSConfig struct {
	ServerName string `json:"serverName,omitempty"`
	CAData     []byte `json:"caData,omitempty"`
	CertData   []byte `json:"certData,omitempty"`
	KeyData    []byte `json:"keyData,omitempty"`
	Insecure   bool   `json:"insecure"`
}

func clusterSecretName(serviceSet *kcmv1.ServiceSet) string {
	return truncateName(serviceSet.Namespace + "-" + serviceSet.Spec.Cluster)
}

// destination resolves where the services of a ServiceSet have to land.
//
// Self-management points at the cluster Argo CD itself runs in, which Argo CD
// always knows about. Anything else is a cluster KCM provisioned or adopted,
// which Argo CD only learns about from a cluster secret the adapter writes
// from the kubeconfig KCM already holds.
func (r *ServiceSetReconciler) destination(ctx context.Context, rgnClient client.Client, serviceSet *kcmv1.ServiceSet, namespace string) (applicationDestination, error) {
	dst := applicationDestination{Namespace: namespace}
	if serviceSet.Spec.Provider.SelfManagement {
		dst.Server = inClusterServer
		return dst, nil
	}

	name, err := r.ensureClusterSecret(ctx, rgnClient, serviceSet)
	if err != nil {
		return applicationDestination{}, err
	}
	dst.Name = name
	return dst, nil
}

// ensureClusterSecret mirrors the target cluster's kubeconfig into the Argo CD
// namespace in the shape Argo CD expects, and returns the cluster name a
// destination can refer to.
func (r *ServiceSetReconciler) ensureClusterSecret(ctx context.Context, rgnClient client.Client, serviceSet *kcmv1.ServiceSet) (string, error) {
	clusterKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: serviceSet.Spec.Cluster}
	kubeconfig, err := childKubeconfig(ctx, rgnClient, clusterKey)
	if err != nil {
		return "", err
	}

	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return "", fmt.Errorf("failed to parse kubeconfig of cluster %s: %w", clusterKey, err)
	}

	name := clusterSecretName(serviceSet)
	config := argoClusterConfig{
		BearerToken: restCfg.BearerToken,
		TLSClientConfig: argoTLSConfig{
			Insecure:   restCfg.Insecure,
			ServerName: restCfg.ServerName,
			CAData:     restCfg.CAData,
			CertData:   restCfg.CertData,
			KeyData:    restCfg.KeyData,
		},
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("failed to marshal Argo CD cluster config for %s: %w", clusterKey, err)
	}

	data := map[string][]byte{
		"name":   []byte(name),
		"server": []byte(restCfg.Host),
		"config": raw,
	}
	labels := map[string]string{
		argoSecretTypeLabel:      argoSecretTypeClust,
		kcmv1.KCMManagedLabelKey: kcmv1.KCMManagedLabelValue,
		labelServiceSetNamespace: serviceSet.Namespace,
	}

	secret := new(corev1.Secret)
	key := client.ObjectKey{Namespace: r.ArgoCDNamespace, Name: name}
	err = r.Client.Get(ctx, key, secret)
	if client.IgnoreNotFound(err) != nil {
		return "", fmt.Errorf("failed to get cluster Secret %s: %w", key, err)
	}
	if err != nil {
		secret.Name = key.Name
		secret.Namespace = key.Namespace
		secret.Labels = labels
		secret.Data = data
		if err := r.Client.Create(ctx, secret); client.IgnoreAlreadyExists(err) != nil {
			return "", fmt.Errorf("failed to create cluster Secret %s: %w", key, err)
		}
		return name, nil
	}

	if secretDataEqual(secret.Data, data) && secret.Labels[argoSecretTypeLabel] == argoSecretTypeClust {
		return name, nil
	}
	secret.Labels = labels
	secret.Data = data
	if err := r.Client.Update(ctx, secret); err != nil {
		return "", fmt.Errorf("failed to update cluster Secret %s: %w", key, err)
	}
	return name, nil
}

// childKubeconfig reads the kubeconfig of a target cluster. A cluster KCM
// provisioned carries the CAPI secret; an adopted one carries the secret its
// ClusterDeployment was created from, which is named after the Sveltos cluster
// object even when Sveltos is not the provider deploying to it.
func childKubeconfig(ctx context.Context, cl client.Client, clusterKey client.ObjectKey) ([]byte, error) {
	secret := new(corev1.Secret)
	capiKey := kubeutil.GetKubeconfigSecretKey(clusterKey)
	err := cl.Get(ctx, capiKey, secret)
	if err == nil {
		if value, ok := secret.Data["value"]; ok {
			return value, nil
		}
		return nil, fmt.Errorf("Secret %s has no \"value\" key", capiKey)
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get kubeconfig Secret %s: %w", capiKey, err)
	}

	adoptedKey := client.ObjectKey{Namespace: clusterKey.Namespace, Name: clusterKey.Name + "-sveltos-kubeconfig"}
	if err := cl.Get(ctx, adoptedKey, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig Secret for cluster %s (tried %s and %s): %w",
			clusterKey, capiKey.Name, adoptedKey.Name, err)
	}
	switch len(secret.Data) {
	case 0:
		return nil, fmt.Errorf("Secret %s has no data", adoptedKey)
	case 1:
		for _, v := range secret.Data {
			return v, nil
		}
	}
	return nil, fmt.Errorf("Secret %s has more than one key, cannot tell which holds the kubeconfig", adoptedKey)
}
