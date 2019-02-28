package cloudnode

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testNodeID = "3cc9c20a-8d6c-47ed-bc73-38ee8786c6b3"
)

func TestContainershipNodeIDKeyFunc(t *testing.T) {
	var justAnInt int
	_, err := ContainershipNodeIDKeyFunc(&justAnInt)
	assert.Error(t, err, "key func fails on non-Node")

	node := corev1.Node{}
	_, err = ContainershipNodeIDKeyFunc(&node)
	assert.Error(t, err, "key func fails when node has no labels")

	node = corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
	}

	_, err = ContainershipNodeIDKeyFunc(&node)
	assert.Error(t, err, "key func fails when node is missing node-id label")

	node.ObjectMeta.Labels[containershipNodeIDLabelKey] = testNodeID

	key, err := ContainershipNodeIDKeyFunc(&node)
	assert.NoError(t, err)
	assert.Equal(t, testNodeID, key)
}

func TestContainershipNodeIDLabelSelector(t *testing.T) {
	s := ContainershipNodeIDLabelSelector(testNodeID)
	assert.NotNil(t, s)
	expected := fmt.Sprintf("%s=%s", containershipNodeIDLabelKey, testNodeID)
	assert.Equal(t, expected, s.String())
}
