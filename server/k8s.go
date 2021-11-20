package server

import (
	"net"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	AnnotationExternalServerName = "mc-router.itzg.me/externalServerName"
	AnnotationDefaultServer      = "mc-router.itzg.me/defaultServer"
)

type IK8sWatcher interface {
	StartWithConfig(kubeConfigFile string) error
	StartInCluster() error
	Stop()
}

var K8sWatcher IK8sWatcher = &k8sWatcherImpl{}

type k8sWatcherImpl struct {
	sync.RWMutex
	mappings map[string]*apps.StatefulSet
	stop     chan struct{}
}

func (w *k8sWatcherImpl) StartInCluster() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "Unable to load in-cluster config")
	}

	return w.startWithLoadedConfig(config)
}

func (w *k8sWatcherImpl) StartWithConfig(kubeConfigFile string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigFile)
	if err != nil {
		return errors.Wrap(err, "Could not load kube config file")
	}

	return w.startWithLoadedConfig(config)
}

func (w *k8sWatcherImpl) startWithLoadedConfig(config *rest.Config) error {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "Could not create kube clientset")
	}

	_, serviceController := cache.NewInformer(
		cache.NewListWatchFromClient(
			clientset.CoreV1().RESTClient(),
			string(core.ResourceServices),
			core.NamespaceAll,
			fields.Everything(),
		),
		&core.Service{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				routableService := w.extractRoutableService(obj)
				if routableService != nil {
					logrus.WithField("routableService", routableService).Debug("ADD")

					if routableService.externalServiceName != "" {
						Routes.CreateMapping(routableService.externalServiceName, routableService.containerEndpoint, routableService.autoScaleUp)
					} else {
						Routes.SetDefaultRoute(routableService.containerEndpoint)
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				routableService := w.extractRoutableService(obj)
				if routableService != nil {
					logrus.WithField("routableService", routableService).Debug("DELETE")

					if routableService.externalServiceName != "" {
						Routes.DeleteMapping(routableService.externalServiceName)
					} else {
						Routes.SetDefaultRoute("")
					}
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldRoutableService := w.extractRoutableService(oldObj)
				newRoutableService := w.extractRoutableService(newObj)
				if oldRoutableService != nil && newRoutableService != nil {
					logrus.WithFields(logrus.Fields{
						"old": oldRoutableService,
						"new": newRoutableService,
					}).Debug("UPDATE")

					if oldRoutableService.externalServiceName != "" && newRoutableService.externalServiceName != "" {
						Routes.DeleteMapping(oldRoutableService.externalServiceName)
						Routes.CreateMapping(newRoutableService.externalServiceName, newRoutableService.containerEndpoint, newRoutableService.autoScaleUp)
					} else {
						Routes.SetDefaultRoute(newRoutableService.containerEndpoint)
					}
				}
			},
		},
	)

	w.mappings = make(map[string]*apps.StatefulSet)
	_, statefulSetController := cache.NewInformer(
		cache.NewListWatchFromClient(
			clientset.AppsV1().RESTClient(),
			"statefulSets",
			core.NamespaceAll,
			fields.Everything(),
		),
		&apps.StatefulSet{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				statefulSet, ok := obj.(*apps.StatefulSet)
				if !ok {
					return
				}
				w.RLock()
				defer w.RUnlock()
				w.mappings[statefulSet.Spec.ServiceName] = statefulSet
			},
			DeleteFunc: func(obj interface{}) {
				statefulSet, ok := obj.(*apps.StatefulSet)
				if !ok {
					return
				}
				w.RLock()
				defer w.RUnlock()
				delete(w.mappings, statefulSet.Spec.ServiceName)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldStatefulSet, ok := oldObj.(*apps.StatefulSet)
				if !ok {
					return
				}
				newStatefulSet, ok := newObj.(*apps.StatefulSet)
				if !ok {
					return
				}
				w.RLock()
				defer w.RUnlock()
				delete(w.mappings, oldStatefulSet.Spec.ServiceName)
				w.mappings[newStatefulSet.Spec.ServiceName] = newStatefulSet
			},
		},
	)

	w.stop = make(chan struct{}, 1)
	logrus.Info("Monitoring Kubernetes for Minecraft services")
	go serviceController.Run(w.stop)
	go statefulSetController.Run(w.stop)

	return nil
}

func (w *k8sWatcherImpl) Stop() {
	if w.stop != nil {
		w.stop <- struct{}{}
	}
}

type routableService struct {
	externalServiceName string
	containerEndpoint   string
	autoScaleUp         func()
}

func (w *k8sWatcherImpl) extractRoutableService(obj interface{}) *routableService {
	service, ok := obj.(*core.Service)
	if !ok {
		return nil
	}

	if externalServiceName, exists := service.Annotations[AnnotationExternalServerName]; exists {
		return w.buildDetails(service, externalServiceName)
	} else if _, exists := service.Annotations[AnnotationDefaultServer]; exists {
		return w.buildDetails(service, "")
	}

	return nil
}

func (w *k8sWatcherImpl) buildDetails(service *core.Service, externalServiceName string) *routableService {
	clusterIp := service.Spec.ClusterIP
	port := "25565"
	for _, p := range service.Spec.Ports {
		if p.Name == "mc-router" {
			port = strconv.Itoa(int(p.Port))
		}
	}
	rs := &routableService{
		externalServiceName: externalServiceName,
		containerEndpoint:   net.JoinHostPort(clusterIp, port),
		autoScaleUp:         w.buildScaleUpFunction(service),
	}
	return rs
}

func (w *k8sWatcherImpl) buildScaleUpFunction(service *core.Service) func() {
	return func() {
		serviceName := service.Name
		if statefulSet, exists := w.mappings[serviceName]; exists {
			replicas := *statefulSet.Spec.Replicas
			logrus.WithFields(logrus.Fields{
				"service":     serviceName,
				"statefulSet": statefulSet.Name,
				"replicas":    replicas,
			}).Info("StatefulSet of Service Replicas")
			if replicas == 0 {
				// TODO scale up the StatefulSet by setting replicas = 1...
			}
		}
	}
}
