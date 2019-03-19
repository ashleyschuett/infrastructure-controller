package cloudnode

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/containership/csctl/cloud/provision/types"

	"github.com/containership/infrastructure-controller/pkg/mocks"
	"github.com/containership/infrastructure-controller/pkg/mocks/provision"
)

func strptr(s string) *string {
	return &s
}

var (
	etcdVersion   = "3.2.24"
	dockerVersion = "18.6.1"

	nps = []types.NodePool{
		{
			Name:              strptr("test1"),
			ID:                types.UUID("5678"),
			KubernetesMode:    strptr("master"),
			KubernetesVersion: strptr("1.12.1"),
			EtcdVersion:       &etcdVersion,
			DockerVersion:     &dockerVersion,
		},
		{
			Name:              strptr("test2"),
			ID:                types.UUID("4321"),
			KubernetesMode:    strptr("worker"),
			KubernetesVersion: strptr("1.11.1"),
			EtcdVersion:       nil,
			DockerVersion:     &dockerVersion,
		},
	}

	node = types.Node{
		ID: types.UUID("1234"),
		Status: &types.NodeStatus{
			Type: strptr("RUNNING"),
		},
		CreatedAt: &nodeTime,
		UpdatedAt: &nodeTime,
	}

	nodeTime = "1517001176920"
)

type HTTPError struct {
	code    int
	message string
}

// Error returns a string representing this error
func (e HTTPError) Error() string {
	s := fmt.Sprintf("server responded with status %d", e.code)
	if e.message != "" {
		s = fmt.Sprintf("%s: %s", s, e.message)
	}

	return s
}

func TestExists(t *testing.T) {
	cloudclientset := &mocks.Interface{}
	provisionInterface := &provision.Interface{}
	nodes := &provision.NodeInterface{}
	nodepools := &provision.NodePoolInterface{}

	// set up cloud provision mock
	cloudclientset.On("Provision").Return(provisionInterface)

	// set up nodes and node pools calls for provision to be mocked
	provisionInterface.On("NodePools", mock.Anything, mock.Anything).Return(nodepools)
	provisionInterface.On("Nodes", mock.Anything, mock.Anything, mock.Anything).Return(nodes)

	// test we handle List returning an error
	nodepools.On("List", mock.Anything, mock.Anything).Return([]types.NodePool{}, errors.New("test error case")).Once()
	nodes.On("Get", mock.AnythingOfType("string")).Return(func(id string) *types.Node {
		if id == string(node.ID) {
			return &node
		}

		return nil
	}, func(id string) error {
		if id == "error" {
			return errors.New("test error case")
		}

		return nil
	})

	_, err := Exists(cloudclientset, "id")
	assert.Error(t, err)

	// test if no node pool matches node ID we return false
	nodepools.On("List", mock.Anything, mock.Anything).Return([]types.NodePool{}, nil).Once()

	exists, err := Exists(cloudclientset, "id")
	assert.NoError(t, err)
	assert.False(t, exists)

	// test node returns false if getting node errors
	nodepools.On("List", mock.Anything, mock.Anything).Return(nps, nil)

	exists, err = Exists(cloudclientset, "error")
	assert.Error(t, err)
	assert.False(t, exists)

	exists, err = Exists(cloudclientset, "1234")
	assert.NoError(t, err)
	assert.True(t, exists)
}
