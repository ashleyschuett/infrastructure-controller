package controller

import (
	"net/url"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelistersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"

	"github.com/pkg/errors"

	"github.com/containership/cluster-manager/pkg/log"
	"github.com/containership/csctl/cloud"
	"github.com/containership/csctl/cloud/provision/types"

	"github.com/containership/infrastructure-controller/pkg/cloudnode"
	"github.com/containership/infrastructure-controller/pkg/env"
	"github.com/containership/infrastructure-controller/pkg/etcd"
)

const (
	delayBetweenRequeues = 30 * time.Second

	// Don't requeue in order to avoid excessive requests to cloud for things
	// that we're going to naturally retry on the sync interval anyway
	maxRequeues = 0
)

// EtcdRemovalController is a controller for removing etcd members upon a node
// being deleted from the cluster.
type EtcdRemovalController struct {
	kubeclientset  kubernetes.Interface
	cloudclientset cloud.Interface

	nodeLister  corelistersv1.NodeLister
	nodesSynced cache.InformerSynced

	workqueue workqueue.RateLimitingInterface
}

// NewEtcdRemovalController returns a new etcd removal controller
func NewEtcdRemovalController(kubeclientset kubernetes.Interface,
	cloudclientset cloud.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory) *EtcdRemovalController {
	rateLimiter := workqueue.NewItemExponentialFailureRateLimiter(delayBetweenRequeues, maxRequeues)

	c := &EtcdRemovalController{
		kubeclientset:  kubeclientset,
		cloudclientset: cloudclientset,
		workqueue:      workqueue.NewNamedRateLimitingQueue(rateLimiter, "EtcdRemoval"),
	}

	nodeInformer := kubeInformerFactory.Core().V1().Nodes()

	log.Info("Setting up event handlers")

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, obj interface{}) {
			// We don't care about compare ResourceVersions because we're
			// mainly using this handler for a periodic resync to check the
			// entire system state
			c.enqueueNode(obj)
		},
		DeleteFunc: c.enqueueNode,
	})

	c.nodeLister = nodeInformer.Lister()
	c.nodesSynced = nodeInformer.Informer().HasSynced

	return c
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *EtcdRemovalController) Run(numWorkers int, stopCh chan struct{}) {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	log.Info("Starting controller")

	if ok := cache.WaitForCacheSync(stopCh, c.nodesSynced); !ok {
		// If this channel is unable to wait for caches to sync we stop both
		// all controllers
		close(stopCh)
		log.Error("failed to wait for caches to sync")
	}

	log.Info("Starting workers")
	// Launch numWorkers amount of workers to process resources
	for i := 0; i < numWorkers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	log.Info("Started workers")
	<-stopCh
	log.Info("Shutting down workers")
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *EtcdRemovalController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem continually pops items off of the workqueue and handles
// them
func (c *EtcdRemovalController) processNextWorkItem() bool {
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
func (c *EtcdRemovalController) handleErr(err error, key interface{}) error {
	if err == nil {
		c.workqueue.Forget(key)
		return nil
	}

	if c.workqueue.NumRequeues(key) < maxRequeues {
		c.workqueue.AddRateLimited(key)
		return errors.Wrapf(err, "error syncing node %q (has been requeued %d times)", key, c.workqueue.NumRequeues(key))
	}

	c.workqueue.Forget(key)
	log.Infof("Dropping node %q out of the queue: %v", key, err)
	return err
}

// enqueueNode enqueues a node.
func (c *EtcdRemovalController) enqueueNode(obj interface{}) {
	var key string
	var err error
	if key, err = cloudnode.ContainershipNodeIDKeyFunc(obj); err != nil {
		log.Error(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}

// syncHandler surveys the state of the system and determines if etcd has any
// members that do not correspond to an existing node. If this is the case, it
// requests that etcd removes that member.
// The key is ignored for now because the entire state of the system is considered,
// not only the single node that was synced.
func (c *EtcdRemovalController) syncHandler(_ string) error {
	client, err := etcd.NewClient([]string{env.EtcdEndpoint()})
	if err != nil {
		return err
	}
	defer client.Close()

	members, err := client.ListMembers()
	if err != nil {
		return errors.Wrap(err, "listing etcd members")
	}

	etcdNodes, err := getNodesRunningEtcd(c.cloudclientset)
	if err != nil {
		return errors.Wrap(err, "listing nodes running etcd")
	}

	var candidate *etcdserverpb.Member

	for _, member := range members {
		if !etcdMemberHasCloudNodeCounterpart(member, etcdNodes) {
			// Cloud doesn't know about this node
			candidate = member
			break
		}
	}

	if candidate == nil {
		// Nothing to do
		return nil
	}

	if etcd.MemberIsHealthy(candidate) {
		// If we didn't find a candidate member to remove or that member is
		// still healthy, don't do anything. We're assuming that the member
		// will eventually become unhealthy if it doesn't live on a node that
		// exists in cloud, because that node will be deleted along with the
		// etcd static pod running on it.
		log.Debugf("etcd member %q is still healthy - deferring removal", candidate.Name)
		return nil
	}

	log.Infof("Requesting etcd member remove for member with name %q", candidate.Name)
	return client.RemoveMemberByName(candidate.Name)
}

// determine if the given etcd member corresponds to one of the given nodes by
// comparing the internal IPs of the nodes to the member's peer URLs
func etcdMemberHasCloudNodeCounterpart(member *etcdserverpb.Member, nodes []types.Node) bool {
	if member == nil {
		return false
	}

	for _, addr := range member.PeerURLs {
		// Ignore errors - an etcd member wouldn't be able to come up with
		// a malformed peer URL
		u, _ := url.Parse(addr)

		for _, node := range nodes {
			if node.Addresses != nil && node.Addresses.InternalIP == u.Hostname() {
				return true
			}
		}
	}

	return false
}

// get all nodes that are running etcd from Containership
func getNodesRunningEtcd(clientset cloud.Interface) ([]types.Node, error) {
	orgID := env.OrganizationID()
	clusterID := env.ClusterID()
	nodePools, err := clientset.Provision().NodePools(orgID, clusterID).List()
	if err != nil {
		return nil, errors.Wrap(err, "listing node pools")
	}

	nodes := make([]types.Node, 0)
	for _, np := range nodePools {
		if np.Etcd != nil && *np.Etcd {
			nodesInThisPool, err := clientset.Provision().Nodes(orgID, clusterID, string(np.ID)).List()
			if err != nil {
				return nil, errors.Wrapf(err, "listing nodes in node pool %q", np.ID)
			}

			nodes = append(nodes, nodesInThisPool...)
		}
	}

	return nodes, nil
}
