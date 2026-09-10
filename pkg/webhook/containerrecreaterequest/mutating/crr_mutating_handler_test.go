/*
Copyright 2026 The Kruise Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mutating

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	appsv1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	"github.com/openkruise/kruise/pkg/util"
)

func TestInjectPodIntoContainerRecreateRequest_VirtualKubeletLabel(t *testing.T) {
	basePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       types.UID("pod-uid-123"),
		},
		Spec: v1.PodSpec{
			NodeName: "test-node",
			Containers: []v1.Container{
				{Name: "main"},
			},
		},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name:         "main",
					ContainerID:  "docker://abc123",
					RestartCount: 0,
				},
			},
		},
	}

	baseCRR := func() *appsv1alpha1.ContainerRecreateRequest {
		return &appsv1alpha1.ContainerRecreateRequest{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-crr",
				Namespace: "default",
				Labels:    map[string]string{},
			},
			Spec: appsv1alpha1.ContainerRecreateRequestSpec{
				PodName: "test-pod",
				Containers: []appsv1alpha1.ContainerRecreateRequestContainer{
					{Name: "main"},
				},
				Strategy: &appsv1alpha1.ContainerRecreateRequestStrategy{},
			},
		}
	}

	tests := []struct {
		name        string
		node        *v1.Node
		expectLabel bool
		expectErr   bool
	}{
		{
			name: "node has virtual-kubelet label, CRR should get the label",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-node",
					Labels: map[string]string{
						util.VirtualKubeletLabelKey: util.VirtualKubeletLabelValue,
					},
				},
			},
			expectLabel: true,
			expectErr:   false,
		},
		{
			name: "node does not have virtual-kubelet label, CRR should not get the label",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-node",
					Labels: map[string]string{"foo": "bar"},
				},
			},
			expectLabel: false,
			expectErr:   false,
		},
		{
			name:        "node not found, should not error and CRR should not get the label",
			node:        nil,
			expectLabel: false,
			expectErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crr := baseCRR()
			err := injectPodIntoContainerRecreateRequest(crr, basePod, tt.node)

			if tt.expectErr && err == nil {
				t.Fatalf("expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			labelVal, hasLabel := crr.Labels[util.VirtualKubeletLabelKey]
			if tt.expectLabel {
				if !hasLabel || labelVal != util.VirtualKubeletLabelValue {
					t.Errorf("expected CRR to have label %s=%s, got labels: %v",
						util.VirtualKubeletLabelKey, util.VirtualKubeletLabelValue, crr.Labels)
				}
			} else {
				if hasLabel {
					t.Errorf("expected CRR not to have label %s, got labels: %v",
						util.VirtualKubeletLabelKey, crr.Labels)
				}
			}
		})
	}
}

// TestInjectPodIntoContainerRecreateRequest_RestartableInitContainer makes sure a CRR can target
// a restartable init container (native sidecar container). Such a container reports its status in
// status.initContainerStatuses, so looking up status.containerStatuses only would reject the
// request and leave no way to recreate a native sidecar.
func TestInjectPodIntoContainerRecreateRequest_RestartableInitContainer(t *testing.T) {
	restartAlways := v1.ContainerRestartPolicyAlways
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       types.UID("pod-uid-123"),
		},
		Spec: v1.PodSpec{
			NodeName: "test-node",
			InitContainers: []v1.Container{
				{Name: "setup"},
				{Name: "sidecar", RestartPolicy: &restartAlways},
			},
			Containers: []v1.Container{{Name: "main"}},
		},
		Status: v1.PodStatus{
			InitContainerStatuses: []v1.ContainerStatus{
				{Name: "setup", ContainerID: "containerd://setup1", RestartCount: 0},
				{Name: "sidecar", ContainerID: "containerd://sidecar1", RestartCount: 2},
			},
			ContainerStatuses: []v1.ContainerStatus{
				{Name: "main", ContainerID: "containerd://main1", RestartCount: 0},
			},
		},
	}

	crrOf := func(containerName string) *appsv1alpha1.ContainerRecreateRequest {
		return &appsv1alpha1.ContainerRecreateRequest{
			ObjectMeta: metav1.ObjectMeta{Name: "test-crr", Namespace: "default", Labels: map[string]string{}},
			Spec: appsv1alpha1.ContainerRecreateRequestSpec{
				PodName:    "test-pod",
				Containers: []appsv1alpha1.ContainerRecreateRequestContainer{{Name: containerName}},
				Strategy:   &appsv1alpha1.ContainerRecreateRequestStrategy{},
			},
		}
	}

	cases := []struct {
		name               string
		containerName      string
		expectErr          bool
		expectContainerID  string
		expectRestartCount int32
	}{
		{
			name:               "restartable init container is accepted",
			containerName:      "sidecar",
			expectErr:          false,
			expectContainerID:  "containerd://sidecar1",
			expectRestartCount: 2,
		},
		{
			name:               "regular init container is accepted as well",
			containerName:      "setup",
			expectErr:          false,
			expectContainerID:  "containerd://setup1",
			expectRestartCount: 0,
		},
		{
			name:               "regular container keeps working",
			containerName:      "main",
			expectErr:          false,
			expectContainerID:  "containerd://main1",
			expectRestartCount: 0,
		},
		{
			name:          "unknown container is still rejected",
			containerName: "nonexistent",
			expectErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			crr := crrOf(tc.containerName)
			err := injectPodIntoContainerRecreateRequest(crr, pod, nil)

			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected an error for container %s, got nil", tc.containerName)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for container %s: %v", tc.containerName, err)
			}

			ctx := crr.Spec.Containers[0].StatusContext
			if ctx == nil {
				t.Fatalf("expected statusContext to be injected for container %s", tc.containerName)
			}
			if ctx.ContainerID != tc.expectContainerID {
				t.Errorf("expected containerID %s, got %s", tc.expectContainerID, ctx.ContainerID)
			}
			if ctx.RestartCount != tc.expectRestartCount {
				t.Errorf("expected restartCount %d, got %d", tc.expectRestartCount, ctx.RestartCount)
			}
		})
	}
}
