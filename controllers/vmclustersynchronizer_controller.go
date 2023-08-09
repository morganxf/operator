package controllers

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/operator/api/v1beta1"
	"github.com/VictoriaMetrics/operator/controllers/converter"
	"github.com/VictoriaMetrics/operator/internal/config"
	"github.com/go-logr/logr"
	v1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

const clusterGatewayURL = "%s/apis/cluster.core.oam.dev/v1alpha1/clustergateways/%s/proxy"

// SynchronizerController - watches for prometheus objects
// and create VictoriaMetrics objects
type SynchronizerController struct {
	promClient     versioned.Interface
	vclient        client.Client
	ruleInf        cache.SharedInformer
	baseConf       *config.BaseOperatorConf
	clusterName    string
	kubeconfig     []byte
	restKubeConfig *rest.Config
	cancel         context.CancelFunc
	Log            logr.Logger
}

// NewSynchronizerController builder for vmclustersynchronizer service
func NewSynchronizerController(clusterName string, kubeconfig []byte, restKubeConfig rest.Config,
	vclient client.Client, baseConf *config.BaseOperatorConf, l logr.Logger) (*SynchronizerController, error) {
	c := &SynchronizerController{
		vclient:        vclient,
		baseConf:       baseConf,
		clusterName:    clusterName,
		kubeconfig:     kubeconfig,
		restKubeConfig: &restKubeConfig,
		Log:            l,
	}
	l.Info("NewSynchronizerController")
	if IsCNStackMode() {
		c.restKubeConfig.Host = fmt.Sprintf(clusterGatewayURL, c.restKubeConfig.Host, clusterName)
		promCl, err := versioned.NewForConfig(c.restKubeConfig)
		if err != nil {
			return nil, fmt.Errorf("cannot build promClient for cluster %s: %w", clusterName, err)
		}
		c.promClient = promCl
	} else {
		clientCfg, err := clientcmd.NewClientConfigFromBytes(kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("cannot build kubeClient for cluster %s: %w", clusterName, err)
		}
		restCfg, err := clientCfg.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("cannot build restClient for cluster %s: %w", clusterName, err)
		}
		promCl, err := versioned.NewForConfig(restCfg)
		if err != nil {
			return nil, fmt.Errorf("cannot build promClient for cluster %s: %w", clusterName, err)
		}
		c.promClient = promCl
	}
	c.ruleInf = cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return c.promClient.MonitoringV1().PrometheusRules(config.MustGetWatchNamespace()).List(context.TODO(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return c.promClient.MonitoringV1().PrometheusRules(config.MustGetWatchNamespace()).Watch(context.TODO(), options)
			},
		},
		&v1.PrometheusRule{},
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	c.ruleInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.CreatePrometheusRule,
		UpdateFunc: c.UpdatePrometheusRule,
	})
	namespace, err := getNamespace()
	if err != nil {
		c.Log.Error(err, "failed to get namespace")
		return nil, err
	}
	promRules, err := c.promClient.MonitoringV1().PrometheusRules(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		c.Log.Error(err, "failed to list prometheusrule")
		return nil, err
	}
	c.Log.Info("prometheusrule size", "size", len(promRules.Items))
	return c, nil
}

func (c *SynchronizerController) runInformerWithDiscovery(ctx context.Context, group, kind string, runInformer func(<-chan struct{})) error {
	err := waitForAPIResource(ctx, c.promClient.Discovery(), group, kind)
	if err != nil {
		return fmt.Errorf("error wait for %s, err: %w", kind, err)
	}
	runInformer(ctx.Done())
	return nil
}

// Start implements interface.
func (c *SynchronizerController) Start(ctx context.Context) error {
	var errG errgroup.Group
	log.Info("starting cluster synchronizer")
	ctx, c.cancel = context.WithCancel(ctx)
	c.Run(ctx, &errG)
	go func() {
		log.Info("waiting for cluster synchronizer to stop")
		err := errG.Wait()
		if err != nil {
			log.Error(err, "error occured at cluster synchronizer")
		}
	}()
	return nil
}

// Run - starts vmclustersynchronizer with background discovery process for each prometheus api object
func (c *SynchronizerController) Run(ctx context.Context, group *errgroup.Group) {
	if c.baseConf.EnabledPrometheusConverter.PrometheusRule {
		group.Go(func() error {
			return c.runInformerWithDiscovery(ctx, v1.SchemeGroupVersion.String(), v1.PrometheusRuleKind, c.ruleInf.Run)
		})
	}
}

func (c *SynchronizerController) Cancel() {
	if c.cancel != nil {
		c.cancel()
	}
}

// CreatePrometheusRule converts prometheus rule to vmrule
func (c *SynchronizerController) CreatePrometheusRule(rule interface{}) {
	promRule := rule.(*v1.PrometheusRule)
	l := log.WithValues("cluster", c.clusterName, "name", promRule.Name, "ns", promRule.Namespace)
	l.Info("start to create prometheusrule")
	cr := converter.ConvertPromRule(promRule, c.baseConf)
	c.fillVMRule(cr, l)

	err := c.vclient.Create(context.Background(), cr)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			c.UpdatePrometheusRule(nil, promRule)
			return
		}
		l.Error(err, "cannot create AlertRule from Prometheusrule")
		return
	}
	l.Info("end to create prometheusrule")
}

// UpdatePrometheusRule updates vmrule
func (c *SynchronizerController) UpdatePrometheusRule(_old, new interface{}) {
	promRuleNew := new.(*v1.PrometheusRule)
	l := log.WithValues("cluster", c.clusterName, "name", promRuleNew.Name, "ns", promRuleNew.Namespace)
	l.Info("start to update prometheusrule")
	VMRule := converter.ConvertPromRule(promRuleNew, c.baseConf)
	c.fillVMRule(VMRule, l)

	ctx := context.Background()
	existingVMRule := &v1beta1.VMRule{}
	err := c.vclient.Get(ctx, types.NamespacedName{Name: VMRule.Name, Namespace: VMRule.Namespace}, existingVMRule)
	if err != nil {
		if errors.IsNotFound(err) {
			if err = c.vclient.Create(ctx, VMRule); err == nil {
				return
			}
		}
		l.Error(err, "cannot get existing VMRule")
		return
	}
	if existingVMRule.Annotations[IgnoreConversionLabel] == IgnoreConversion {
		l.Info("syncing for object was disabled by annotation", "annotation", IgnoreConversionLabel)
		return
	}
	existingVMRule.Spec = VMRule.Spec
	metaMergeStrategy := getMetaMergeStrategy(existingVMRule.Annotations)
	existingVMRule.Annotations = mergeLabelsWithStrategy(existingVMRule.Annotations, VMRule.Annotations, metaMergeStrategy)
	existingVMRule.Labels = mergeLabelsWithStrategy(existingVMRule.Labels, VMRule.Labels, metaMergeStrategy)
	existingVMRule.OwnerReferences = VMRule.OwnerReferences

	err = c.vclient.Update(ctx, existingVMRule)
	if err != nil {
		l.Error(err, "cannot update VMRule")
		return
	}
	l.Info("end to update prometheusrule")
}

func (c *SynchronizerController) fillVMRule(rule *v1beta1.VMRule, logger logr.Logger) {
	rule.Name = c.clusterName + "." + rule.Namespace + "." + rule.Name
	namespace, err := getNamespace()
	if err != nil {
		logger.Error(err, "cannot get namespace")
		return
	}
	rule.Namespace = namespace

	if rule.Labels == nil {
		rule.Labels = map[string]string{}
	}
	rule.Labels["cluster"] = c.clusterName
	rule.OwnerReferences = nil
}

var _namespace string

func getNamespace() (string, error) {
	if len(_namespace) != 0 {
		return _namespace, nil
	}
	_namespace = os.Getenv("NAMESPACE")
	if len(_namespace) != 0 {
		return _namespace, nil
	}
	const namespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	content, err := os.ReadFile(namespaceFile)
	if err != nil {
		return "", err
	}
	_namespace = string(content)
	return _namespace, nil
}
