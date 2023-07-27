/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"bytes"
	"context"
	"github.com/VictoriaMetrics/operator/controllers/factory/limiter"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/operator/internal/config"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	k8sClusterRateLimiter = limiter.NewRateLimiter("k8scluster", 5)
	k8sClusterSync        sync.Mutex
	synctlMap             = make(map[string]*SynchronizerController)
)

const clusterPrefix = "cluster-"

// K8sClusterReconciler reconciles a VMRule object
type K8sClusterReconciler struct {
	client.Client
	Log           logr.Logger
	OriginScheme  *runtime.Scheme
	BaseConf      *config.BaseOperatorConf
	OriginManager manager.Manager
}

// Scheme implements interface.
func (r *K8sClusterReconciler) Scheme() *runtime.Scheme {
	return r.OriginScheme
}

// Reconcile general reconcile method for controller
func (r *K8sClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	namespace, err := getNamespace()
	if err != nil {
		return ctrl.Result{}, err
	}
	if namespace != req.Namespace || !strings.HasPrefix(req.Name, clusterPrefix) {
		return ctrl.Result{}, nil
	}

	l := r.Log.WithValues("k8scluster", req.NamespacedName)

	k8sClusterSync.Lock()
	defer k8sClusterSync.Unlock()

	clusterName := strings.TrimPrefix(req.Name, clusterPrefix)
	// Fetch the cluster kubeconfig instance
	instance := &v1.Secret{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if sterr, ok := err.(*errors.StatusError); ok && sterr.ErrStatus.Code == 404 {
			if synctl, ok := synctlMap[clusterName]; ok {
				synctl.Cancel()
				delete(synctlMap, clusterName)
			}
			return ctrl.Result{}, nil
		}
		return handleGetError(req, "k8scluster", err)
	}
	if v1.ServiceAccountKubeconfigKey != instance.Type {
		return ctrl.Result{}, nil
	}

	RegisterObjectStat(instance, "k8scluster")
	if k8sClusterRateLimiter.MustThrottleReconcile() {
		// fast path
		return ctrl.Result{}, nil
	}

	// delete
	if !instance.DeletionTimestamp.IsZero() {
		if synctl, ok := synctlMap[clusterName]; ok {
			synctl.Cancel()
			delete(synctlMap, clusterName)
		}
		return ctrl.Result{}, nil
	}

	kubeconfig, ok := instance.Data["kubeconfig.yaml"]
	if !ok {
		l.Error(err, "cannot read kubeconfig for cluster "+clusterName)
		return ctrl.Result{}, nil
	}
	// create or update
	synctl, ok := synctlMap[clusterName]
	if !ok {
		synctl, err = NewSynchronizerController(clusterName, kubeconfig, r.Client, r.BaseConf)
		if err != nil {
			l.Error(err, "cannot build synchronizerController")
			return ctrl.Result{}, err
		}
		synctlMap[clusterName] = synctl
	} else {
		if !bytes.Equal(synctl.kubeconfig, kubeconfig) {
			synctl.Cancel()
			synctl, err = NewSynchronizerController(clusterName, kubeconfig, r.Client, r.BaseConf)
			if err != nil {
				l.Error(err, "cannot build synchronizerController")
				return ctrl.Result{}, err
			}
			synctlMap[clusterName] = synctl
		}
	}
	if err := r.OriginManager.Add(synctl); err != nil {
		l.Error(err, "cannot add synchronizerController")
		return ctrl.Result{}, err
	}
	l.Info("SynchronizerController size: %s: %v", len(synctlMap), synctlMap)

	return
}

// SetupWithManager general setup method
func (r *K8sClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.Secret{}).
		WithOptions(getDefaultOptions()).
		Complete(r)
}
