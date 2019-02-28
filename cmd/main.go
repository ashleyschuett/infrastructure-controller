package main

import (
	"flag"
	"os"
	"runtime"
	"time"

	"github.com/pkg/errors"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"

	"github.com/containership/cluster-manager/pkg/log"
	"github.com/containership/csctl/cloud"
	"github.com/containership/infrastructure-controller/pkg/buildinfo"
	"github.com/containership/infrastructure-controller/pkg/controller"
	"github.com/containership/infrastructure-controller/pkg/env"
)

func main() {
	log.Info("Starting Containership Infrastructure Controller...")
	log.Infof("Version: %s", buildinfo.String())
	log.Infof("Go Version: %s", runtime.Version())

	if err := env.Init(); err != nil {
		log.Fatalf("Error initializing environment: %v", err)
	}
	env.Dump(os.Stdout)

	// We don't have any of our own flags to parse, but k8s packages want to
	// use klog and we have to pass flags to that to configure it to behave
	// in a sane way.
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()

	config, err := determineConfig()
	if err != nil {
		log.Fatal(err)
	}

	kubeclientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes clientset: %+v", err)
	}

	cloudclientset, err := cloud.New(cloud.Config{
		Token:            env.ClusterToken(),
		ProvisionBaseURL: env.ProvisionBaseURL(),
	})

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeclientset, 30*time.Second)

	etcdRemovalController := controller.NewEtcdRemovalController(
		kubeclientset, cloudclientset, kubeInformerFactory)

	nodeRemovalController := controller.NewNodeRemovalController(
		kubeclientset, cloudclientset, kubeInformerFactory)

	stopCh := make(chan struct{})
	kubeInformerFactory.Start(stopCh)

	go etcdRemovalController.Run(1, stopCh)
	go nodeRemovalController.Run(1, stopCh)

	runtime.Goexit()
}

// determineConfig determines if we are running in a cluster or outside
// and gets the appropriate configuration to talk with Kubernetes.
func determineConfig() (*rest.Config, error) {
	kubeconfigPath := os.Getenv("KUBECONFIG")
	var config *rest.Config
	var err error

	// determine whether to use in cluster config or out of cluster config
	// if kubeconfigPath is not specified, default to in cluster config
	// otherwise, use out of cluster config
	if kubeconfigPath == "" {
		log.Info("Using in cluster k8s config")
		config, err = rest.InClusterConfig()
	} else {
		log.Info("Using out of cluster k8s config: ", kubeconfigPath)

		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}

	if err != nil {
		return nil, errors.Wrap(err, "determine Kubernetes config failed")
	}

	return config, nil
}
