package cloudnode

import (
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

const (
	containershipNodeIDLabelKey = "containership.io/node-id"
)

// ContainershipNodeIDKeyFunc is a key function used to enqueue a node's ID instead of its name.  Since only one property is used (no e.g. namespace as would be typical), no
// corresponding split function is needed.
func ContainershipNodeIDKeyFunc(obj interface{}) (string, error) {
	// This is a private function intended to only be used with Node objects, so let's
	// treat it as a Node directly and avoid the meta stuff
	node, ok := obj.(*corev1.Node)
	if !ok {
		return "", errors.Errorf("cannot use node ID key function on non-Node type %T", obj)
	}

	nodeID, ok := node.Labels[containershipNodeIDLabelKey]
	if !ok {
		return "", errors.Errorf("node %q does not have a label with key %q", node.Name, containershipNodeIDLabelKey)
	}

	return nodeID, nil
}

// ContainershipNodeIDLabelSelector returns a label selector for selecting the
// node with the given Containership node ID
func ContainershipNodeIDLabelSelector(id string) labels.Selector {
	selector := labels.NewSelector()
	req, _ := labels.NewRequirement(containershipNodeIDLabelKey, selection.Equals, []string{id})
	return selector.Add(*req)
}
