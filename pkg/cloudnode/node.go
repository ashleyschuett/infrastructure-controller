package cloudnode

import (
	"github.com/pkg/errors"

	"github.com/containership/csctl/cloud"
	"github.com/containership/csctl/cloud/rest"

	"github.com/containership/infrastructure-controller/pkg/env"
)

// Exists checks if a node with passed in ID exists in any node pools
// associated with the cluster
func Exists(clientset cloud.Interface, id string) (bool, error) {
	pools, err := clientset.Provision().NodePools(env.OrganizationID(), env.ClusterID()).List()
	if err != nil {
		return false, err
	}

	for _, np := range pools {
		exists, err := nodeExistsInPool(clientset, string(np.ID), id)
		if err != nil {
			return false, err
		}

		if exists {
			return true, nil
		}
	}

	return false, nil
}

// nodeExistsInPool returns true if the node with the given ID exists in the
// given node pool, else false.
func nodeExistsInPool(clientset cloud.Interface, nodePoolID, nodeID string) (bool, error) {
	_, err := clientset.Provision().Nodes(env.OrganizationID(), env.ClusterID(), nodePoolID).Get(nodeID)
	if err == nil {
		// Found it
		return true, nil
	}

	switch err := err.(type) {
	case rest.HTTPError:
		if err.IsNotFound() {
			return false, nil
		}
	}

	// Some other error occurred
	return false, errors.Wrapf(err, "attempting to get node %q from pool %q", nodeID, nodePoolID)
}
