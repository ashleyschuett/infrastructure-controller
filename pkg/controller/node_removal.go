package controller

import (
	"fmt"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	corev1 "k8s.io/api/core/v1"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelistersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/pkg/errors"

	"github.com/containership/cluster-manager/pkg/log"
	"github.com/containership/csctl/cloud"
	"github.com/containership/infrastructure-controller/pkg/cloudnode"
)

const (
	nodeRemovalControllerName = "NodeRemovalController"

	// the sync only happens on a node update, so we should retry in cases of
	// transient errors when processing a node for removal
	maxNodeRemovalRequeues = 5
)

// NodeRemovalController is a controller for removing nodes from a Kubernetes cluster
// if they have gone into "NotReady" state and no longer exist in Containership cloud
type NodeRemovalController struct {
	kubeclientset  kubernetes.Interface
	cloudclientset cloud.Interface

	nodeLister  corelistersv1.NodeLister
	nodesSynced cache.InformerSynced

	workqueue workqueue.RateLimitingInterface
}

// NewNodeRemovalController returns a new node removal controller
func NewNodeRemovalController(kubeclientset kubernetes.Interface,
	cloudclientset cloud.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory) *NodeRemovalController {
	rateLimiter := workqueue.NewItemExponentialFailureRateLimiter(delayBetweenRequeues, maxNodeRemovalRequeues)

	c := &NodeRemovalController{
		kubeclientset:  kubeclientset,
		cloudclientset: cloudclientset,
		workqueue:      workqueue.NewNamedRateLimitingQueue(rateLimiter, "NodeRemoval"),
	}

	nodeInformer := kubeInformerFactory.Core().V1().Nodes()

	log.Infof("Setting up event %s handlers", nodeRemovalControllerName)

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			newNode := new.(*corev1.Node)
			oldNode := old.(*corev1.Node)

			// only sync if the resource has changed
			if newNode.ResourceVersion == oldNode.ResourceVersion {
				return
			}

			c.enqueueNode(new)
		},
	})

	c.nodeLister = nodeInformer.Lister()
	c.nodesSynced = nodeInformer.Informer().HasSynced

	return c
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *NodeRemovalController) Run(numWorkers int, stopCh chan struct{}) {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	log.Infof("Starting %s", nodeRemovalControllerName)

	if ok := cache.WaitForCacheSync(stopCh, c.nodesSynced); !ok {
		// If this channel is unable to wait for caches to sync we stop both
		// all controllers
		close(stopCh)
		log.Error("failed to wait for caches to sync")
	}

	log.Infof("Starting %s workers", nodeRemovalControllerName)
	// Launch numWorkers amount of workers to process resources
	for i := 0; i < numWorkers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	log.Infof("Started %s workers", nodeRemovalControllerName)
	<-stopCh
	log.Infof("Shutting down %s workers", nodeRemovalControllerName)
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *NodeRemovalController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem continually pops items off of the workqueue and handles
// them
func (c *NodeRemovalController) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			log.Errorf("expected string in workqueue but got %#v", obj)
			return nil
		}

		err := c.syncHandler(key)
		return c.handleErr(err, key)
	}(obj)

	if err != nil {
		log.Error(err)
		return true
	}

	return true
}

// handleErr drops the key from the workqueue if the error is nil or requeues
// it up to a maximum number of times
func (c *NodeRemovalController) handleErr(err error, key interface{}) error {
	if err == nil {
		c.workqueue.Forget(key)
		return nil
	}

	if c.workqueue.NumRequeues(key) < maxNodeRemovalRequeues {
		c.workqueue.AddRateLimited(key)
		return errors.Wrapf(err, "error syncing node %q (has been requeued %d times)", key, c.workqueue.NumRequeues(key))
	}

	c.workqueue.Forget(key)
	log.Infof("Dropping node %q out of the queue: %v", key, err)
	return err
}

// enqueueNode enqueues a node.
func (c *NodeRemovalController) enqueueNode(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		log.Error("Error enqueueing Node: %s", err)
		return
	}

	log.Debugf("%s: added %q to workqueue ", nodeRemovalControllerName, key)
	c.workqueue.AddRateLimited(key)
}

// syncHandler surveys the state the Kubernetes cluster's nodes. If there is a
// node in "NotReady" state it will check to see if the node exists in
// Containership Cloud. If the node does not exist it will be deleted from
// the Kubernetes cluster
func (c *NodeRemovalController) syncHandler(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	node, err := c.nodeLister.Get(name)
	if err != nil {
		if kubeerrors.IsNotFound(err) {
			return nil
		}

		return errors.Wrap(err, "getting node from Kubernetes cache")
	}

	if nodeIsReady(node) {
		return nil
	}

	// We need to get the Containership Node ID from the Kubernetes node
	nodeID, err := cloudnode.ContainershipNodeIDKeyFunc(node)
	if err != nil {
		return errors.Wrap(err, "getting Containership ID")
	}

	exists, err := cloudnode.Exists(c.cloudclientset, nodeID)
	if err != nil {
		return errors.Wrap(err, "getting node from cloud")
	}

	if exists {
		return nil
	}

	err = c.kubeclientset.Core().Nodes().Delete(name, &metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "deleting node")
	}

	log.Infof("Successfully deleted node %q", name)
	return nil
}

func nodeIsReady(node *corev1.Node) bool {
	var nodeReadyCondition corev1.NodeCondition

	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			nodeReadyCondition = condition
		}
	}

	return nodeReadyCondition.Status == corev1.ConditionTrue
}
