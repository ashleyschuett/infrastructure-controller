package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/stretchr/testify/assert"
)

var (
	emptyStatusNode = corev1.Node{
		Status: corev1.NodeStatus{},
	}

	nodeStatus1 = corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:    corev1.NodeReady,
					Status:  corev1.ConditionTrue,
					Reason:  "KubeletReady",
					Message: "kubelet is posting ready status",
				},
			},
		},
	}

	nodeStatus2 = corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:    corev1.NodeReady,
					Status:  corev1.ConditionFalse,
					Reason:  "KubeletNotReady",
					Message: "node not ready error message",
				},
			},
		},
	}
)

func TestNodeIsReady(t *testing.T) {
	ready := nodeIsReady(&emptyStatusNode)
	assert.False(t, ready, "empty nodes always returns false")

	ready = nodeIsReady(&nodeStatus1)
	assert.True(t, ready, "ready node return true")

	ready = nodeIsReady(&nodeStatus2)
	assert.False(t, ready, "not ready node returns false")
}
