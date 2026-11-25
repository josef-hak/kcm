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
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	"github.com/K0rdent/kcm/internal/serviceset"
	pollerutil "github.com/K0rdent/kcm/internal/util/poller"
)

const (
	// enqueueBaseBackoff is how often a ServiceSet with work still in flight
	// is looked at, and the interval the back-off resets to whenever Argo CD
	// moves.
	enqueueBaseBackoff = 10 * time.Second

	// enqueueMaxBackoff caps the back-off, so a sync that is stuck rather than
	// slow still gets reported on at a predictable interval.
	enqueueMaxBackoff = 5 * time.Minute
)

// enqueueState is the per-ServiceSet polling memory: the Application
// revisions last seen, and when this ServiceSet may be enqueued again.
type enqueueState struct {
	nextEligibleTime time.Time
	lastSeenRevision string
	currentBackoff   time.Duration
}

var enqueueStates = struct {
	entries map[client.ObjectKey]*enqueueState
	sync.Mutex
}{entries: make(map[client.ObjectKey]*enqueueState)}

// evaluate decides whether one ServiceSet is worth a reconcile this tick,
// updating the receiver in place.
//
//nolint:revive // deployed is an axis of the state machine, not a caller-driven switch
func (s *enqueueState) evaluate(now time.Time, revision string, deployed bool) bool {
	if s.lastSeenRevision != revision {
		s.lastSeenRevision = revision
		s.currentBackoff = enqueueBaseBackoff
		s.nextEligibleTime = now.Add(s.currentBackoff)
		return true
	}
	if deployed {
		return false
	}
	if now.Before(s.nextEligibleTime) {
		return false
	}
	s.currentBackoff *= 2
	if s.currentBackoff > enqueueMaxBackoff {
		s.currentBackoff = enqueueMaxBackoff
	}
	s.nextEligibleTime = now.Add(s.currentBackoff)
	return true
}

func loadOrCreateEnqueueState(key client.ObjectKey) *enqueueState {
	enqueueStates.Lock()
	defer enqueueStates.Unlock()
	s, ok := enqueueStates.entries[key]
	if !ok {
		s = &enqueueState{}
		enqueueStates.entries[key] = s
	}
	return s
}

func pruneEnqueueStates(seen map[client.ObjectKey]struct{}) {
	enqueueStates.Lock()
	defer enqueueStates.Unlock()
	for key := range enqueueStates.entries {
		if _, ok := seen[key]; !ok {
			delete(enqueueStates.entries, key)
		}
	}
}

// enqueueApplications returns a poller function that reconciles a ServiceSet
// when the Applications behind it have moved, and otherwise backs off.
//
// Argo CD's progress is not an event on anything this controller watches: the
// Application CRD may not even exist when the manager starts. One list of the
// Applications in the Argo CD namespace per tick is what tells us a sync
// happened, and their resource versions, concatenated, are the cheap way to
// notice.
func enqueueApplications(cl client.Client, argoCDNamespace, providerName string) pollerutil.EnqueueFunc[*kcmv1.ServiceSet] {
	return func(ctx context.Context) ([]*kcmv1.ServiceSet, error) {
		logger := ctrl.LoggerFrom(ctx)

		serviceSets := new(kcmv1.ServiceSetList)
		if err := cl.List(ctx, serviceSets); err != nil {
			return nil, fmt.Errorf("failed to list ServiceSet objects: %w", err)
		}

		apps := newApplicationList()
		if err := cl.List(ctx, apps, client.InNamespace(argoCDNamespace),
			client.MatchingLabels{kcmv1.KCMManagedLabelKey: kcmv1.KCMManagedLabelValue}); err != nil {
			// Before the Argo CD provider finished installing there is no such
			// kind; nothing to poll until there is.
			logger.V(1).Error(err, "failed to list Argo CD Applications")
			return nil, nil
		}

		revisions := make(map[client.ObjectKey][]string, len(serviceSets.Items))
		for i := range apps.Items {
			app := &apps.Items[i]
			labels := app.GetLabels()
			key := client.ObjectKey{
				Namespace: labels[labelServiceSetNamespace],
				Name:      labels[labelServiceSetName],
			}
			if key.Name == "" {
				continue
			}
			revisions[key] = append(revisions[key], app.GetName()+":"+app.GetResourceVersion())
		}

		now := time.Now()
		seen := make(map[client.ObjectKey]struct{}, len(serviceSets.Items))
		out := make([]*kcmv1.ServiceSet, 0, len(serviceSets.Items))
		for i := range serviceSets.Items {
			serviceSet := &serviceSets.Items[i]
			key := client.ObjectKeyFromObject(serviceSet)

			if !serviceSet.GetDeletionTimestamp().IsZero() {
				continue
			}
			if providerName != "" && serviceSet.Spec.Provider.Name != providerName {
				continue
			}
			seen[key] = struct{}{}

			parts := revisions[key]
			slices.Sort(parts)
			state := loadOrCreateEnqueueState(key)
			if state.evaluate(now, strings.Join(parts, ","), serviceset.FullyDeployed(serviceSet)) {
				logger.V(1).Info("Scheduling reconcile", "service_set", key, "next_backoff", state.currentBackoff)
				out = append(out, serviceSet)
			}
		}

		pruneEnqueueStates(seen)
		return out, nil
	}
}
