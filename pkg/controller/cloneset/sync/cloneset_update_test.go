/*
Copyright 2021 The Kruise Authors.

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

package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/openkruise/kruise/apis"
	appspub "github.com/openkruise/kruise/apis/apps/pub"
	appsv1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	clonesetcore "github.com/openkruise/kruise/pkg/controller/cloneset/core"
	clonesetutils "github.com/openkruise/kruise/pkg/controller/cloneset/utils"
	"github.com/openkruise/kruise/pkg/features"
	"github.com/openkruise/kruise/pkg/util"
	"github.com/openkruise/kruise/pkg/util/controllerfinder"
	utilfeature "github.com/openkruise/kruise/pkg/util/feature"
	"github.com/openkruise/kruise/pkg/util/inplaceupdate"
	"github.com/openkruise/kruise/pkg/util/lifecycle"
)

type manageCase struct {
	name           string
	cs             *appsv1alpha1.CloneSet
	updateRevision *apps.ControllerRevision
	revisions      []*apps.ControllerRevision
	pods           []*v1.Pod
	pvcs           []*v1.PersistentVolumeClaim
	expectedPods   []*v1.Pod
	expectedPVCs   []*v1.PersistentVolumeClaim
}

func (mc *manageCase) initial() []client.Object {
	var initialObjs []client.Object
	mc.cs.Name = "clone-test"
	initialObjs = append(initialObjs, mc.cs)

	for i := range mc.pods {
		initialObjs = append(initialObjs, mc.pods[i])
	}

	for i := range mc.pvcs {
		initialObjs = append(initialObjs, mc.pvcs[i])
	}

	return initialObjs
}

func getInt32Pointer(i int32) *int32 {
	return &i
}

func TestUpdate(t *testing.T) {
	utilruntime.Must(apis.AddToScheme(scheme.Scheme))
	now := metav1.NewTime(time.Unix(time.Now().Add(-time.Hour).Unix(), 0))
	cases := []manageCase{
		{
			name:           "do nothing",
			cs:             &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{Replicas: getInt32Pointer(1)}},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{apps.ControllerRevisionHashLabelKey: "rev_new"}},
					Spec:       v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{apps.ControllerRevisionHashLabelKey: "rev_new", apps.DefaultDeploymentUniqueLabelKey: "rev_new"}},
					Spec:       v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
		},
		{
			name:           "normal update condition",
			cs:             &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{Replicas: getInt32Pointer(1)}},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{apps.ControllerRevisionHashLabelKey: "rev_new"}},
					Spec:       v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{apps.ControllerRevisionHashLabelKey: "rev_new", apps.DefaultDeploymentUniqueLabelKey: "rev_new"}, ResourceVersion: "1"},
					Spec:       v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
						{Type: appspub.InPlaceUpdateReady, LastTransitionTime: now, Status: v1.ConditionTrue},
					}},
				},
			},
		},
		{
			name: "recreate update 1",
			cs: &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{
				Replicas:       getInt32Pointer(1),
				UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.RecreateCloneSetUpdateStrategyType},
			}},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appsv1alpha1.CloneSetInstanceID:      "id-0",
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			pvcs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", ResourceVersion: "1", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appsv1alpha1.CloneSetInstanceID:      "id-0",
						appsv1alpha1.SpecifiedDeleteKey:      "true",
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPVCs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
		},
		{
			name: "recreate update 2",
			cs: &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{
				Replicas:       getInt32Pointer(1),
				UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType},
			}},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2","env":["name":"k", "value":"v"]}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appsv1alpha1.CloneSetInstanceID:      "id-0",
					}},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			pvcs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", ResourceVersion: "1", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appsv1alpha1.CloneSetInstanceID:      "id-0",
						appsv1alpha1.SpecifiedDeleteKey:      "true",
					}},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPVCs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
		},
		{
			name: "inplace update",
			cs: &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{
				Replicas:       getInt32Pointer(1),
				UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType},
			}},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appsv1alpha1.CloneSetInstanceID:      "id-0",
					}},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			pvcs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.ControllerRevisionHashLabelKey:  "rev_new",
							apps.DefaultDeploymentUniqueLabelKey: "rev_new",
							appsv1alpha1.CloneSetInstanceID:      "id-0",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStateUpdating),
						},
						Annotations: map[string]string{appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
							Revision:               "rev_new",
							UpdateTimestamp:        now,
							UpdateImages:           true,
							LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "image-id-xyz"}},
							ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: now, Containers: []string{"c1"}}},
						})},
						ResourceVersion: "2",
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo2"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse, Reason: "StartInPlaceUpdate", LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPVCs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
		},
		{
			name: "inplace update with grace period",
			cs: &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{
				Replicas:       getInt32Pointer(1),
				UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType, InPlaceUpdateStrategy: &appspub.InPlaceUpdateStrategy{GracePeriodSeconds: 3630}},
			}},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appsv1alpha1.CloneSetInstanceID:      "id-0",
					}},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			pvcs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.ControllerRevisionHashLabelKey:  "rev_new",
							apps.DefaultDeploymentUniqueLabelKey: "rev_new",
							appsv1alpha1.CloneSetInstanceID:      "id-0",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStateUpdating),
						},
						Annotations: map[string]string{
							appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
								Revision:        "rev_new",
								UpdateTimestamp: now,
								UpdateImages:    true,
							}),
							appspub.InPlaceUpdateGraceKey: `{"revision":"rev_new","containerImages":{"c1":"foo2"},"graceSeconds":3630}`,
						},
						ResourceVersion: "2",
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse, Reason: "StartInPlaceUpdate", LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPVCs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
		},
		{
			name: "inplace update during grace period",
			cs: &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{
				Replicas:       getInt32Pointer(1),
				UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType, InPlaceUpdateStrategy: &appspub.InPlaceUpdateStrategy{GracePeriodSeconds: 3630}},
			}},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{apps.ControllerRevisionHashLabelKey: "rev_new", appsv1alpha1.CloneSetInstanceID: "id-0"},
						Annotations: map[string]string{
							appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
								Revision:        "rev_new",
								UpdateTimestamp: metav1.NewTime(now.Add(-time.Second * 10)),
							}),
							appspub.InPlaceUpdateGraceKey: `{"revision":"rev_new","containerImages":{"c1":"foo2"},"graceSeconds":3630}`,
						},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse, Reason: "StartInPlaceUpdate", LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			pvcs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.ControllerRevisionHashLabelKey:  "rev_new",
							apps.DefaultDeploymentUniqueLabelKey: "rev_new",
							appsv1alpha1.CloneSetInstanceID:      "id-0",
						},
						Annotations: map[string]string{
							appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
								Revision:        "rev_new",
								UpdateTimestamp: metav1.NewTime(now.Add(-time.Second * 10)),
							}),
							appspub.InPlaceUpdateGraceKey: `{"revision":"rev_new","containerImages":{"c1":"foo2"},"graceSeconds":3630}`,
						},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse, Reason: "StartInPlaceUpdate", LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPVCs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
		},
		{
			name: "inplace update continuously after grace period",
			cs: &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{
				Replicas:       getInt32Pointer(1),
				UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType, InPlaceUpdateStrategy: &appspub.InPlaceUpdateStrategy{GracePeriodSeconds: 3630}},
			}},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{apps.ControllerRevisionHashLabelKey: "rev_new", appsv1alpha1.CloneSetInstanceID: "id-0"},
						Annotations: map[string]string{
							appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
								Revision:        "rev_new",
								UpdateTimestamp: metav1.NewTime(now.Add(-time.Minute)),
							}),
							appspub.InPlaceUpdateGraceKey: `{"revision":"rev_new","containerImages":{"c1":"foo2"},"graceSeconds":3630}`,
						},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse, Reason: "StartInPlaceUpdate", LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			pvcs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.ControllerRevisionHashLabelKey:  "rev_new",
							apps.DefaultDeploymentUniqueLabelKey: "rev_new",
							appsv1alpha1.CloneSetInstanceID:      "id-0",
						},
						Annotations: map[string]string{
							appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
								Revision:               "rev_new",
								UpdateTimestamp:        metav1.NewTime(now.Add(-time.Minute)),
								LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "image-id-xyz"}},
								ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: now, Containers: []string{"c1"}}},
							}),
						},
						ResourceVersion: "1",
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo2"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse, Reason: "StartInPlaceUpdate", LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPVCs: []*v1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-0", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-1"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2", Labels: map[string]string{appsv1alpha1.CloneSetInstanceID: "id-0"}}},
			},
		},
		{
			name:           "create: preparingNormal->Normal without hook",
			cs:             &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{Replicas: getInt32Pointer(1)}},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}, NodeName: "127.0.0.1"},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStateNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}, NodeName: "127.0.0.1"},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
		},
		{
			name: "create: preparingNormal->preparingNormal, preNormal does not hook",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:  getInt32Pointer(1),
					Lifecycle: &appspub.Lifecycle{PreNormal: &appspub.LifecycleHook{LabelsHandler: map[string]string{"preNormalHooked": "true"}}},
				},
			},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
		},
		{
			name: "create: preparingNormal->Normal, preNormal does hook",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:  getInt32Pointer(1),
					Lifecycle: &appspub.Lifecycle{PreNormal: &appspub.LifecycleHook{LabelsHandler: map[string]string{"preNormalHooked": "true"}}},
				},
			},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						"preNormalHooked":                    "true",
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						"preNormalHooked":                    "true",
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStateNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
		},
		{
			name: "recreate update: preparingNormal, preNormal does not hook, specific delete",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:  getInt32Pointer(1),
					Lifecycle: &appspub.Lifecycle{PreNormal: &appspub.LifecycleHook{FinalizersHandler: []string{"preNormalHooked"}}},
				},
			},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_old",
						apps.DefaultDeploymentUniqueLabelKey: "rev_old",
						appsv1alpha1.SpecifiedDeleteKey:      "true",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionTrue},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
		},
		{
			name: "in-place update: preparingNormal->Updating, preNormal & InPlaceUpdate does not hook",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas:       getInt32Pointer(1),
					Lifecycle:      &appspub.Lifecycle{PreNormal: &appspub.LifecycleHook{FinalizersHandler: []string{"slb.com/online"}}},
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType},
				},
			},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.DefaultDeploymentUniqueLabelKey: "rev_old",
							apps.ControllerRevisionHashLabelKey:  "rev_old",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
						},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue, LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.ControllerRevisionHashLabelKey:  "rev_new",
							apps.DefaultDeploymentUniqueLabelKey: "rev_new",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStateUpdating),
						},
						Annotations: map[string]string{
							appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
								Revision:               "rev_new",
								UpdateImages:           true,
								UpdateTimestamp:        metav1.NewTime(now.Time),
								LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "image-id-xyz"}},
								ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: now, Containers: []string{"c1"}}},
							}),
						},
						ResourceVersion: "1",
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo2"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse, Reason: "StartInPlaceUpdate", LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
		},
		{
			name: "in-place update: preparingNormal->Normal, preNormal & InPlaceUpdate does hook",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas: getInt32Pointer(1),
					Lifecycle: &appspub.Lifecycle{
						PreNormal:     &appspub.LifecycleHook{FinalizersHandler: []string{"slb/online"}},
						InPlaceUpdate: &appspub.LifecycleHook{FinalizersHandler: []string{"slb/online"}},
					},
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType},
				},
			},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.DefaultDeploymentUniqueLabelKey: "rev_old",
							apps.ControllerRevisionHashLabelKey:  "rev_old",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
						},
						Finalizers: []string{"slb/online"},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue, LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.DefaultDeploymentUniqueLabelKey: "rev_old",
							apps.ControllerRevisionHashLabelKey:  "rev_old",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStateNormal),
						},
						Finalizers: []string{"slb/online"},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue, LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
		},
		{
			name: "in-place update: Normal->preparingUpdate, preNormal & InPlaceUpdate does hook",
			cs: &appsv1alpha1.CloneSet{
				Spec: appsv1alpha1.CloneSetSpec{
					Replicas: getInt32Pointer(1),
					Lifecycle: &appspub.Lifecycle{
						PreNormal:     &appspub.LifecycleHook{FinalizersHandler: []string{"slb/online"}},
						InPlaceUpdate: &appspub.LifecycleHook{FinalizersHandler: []string{"slb/online"}},
					},
					UpdateStrategy: appsv1alpha1.CloneSetUpdateStrategy{Type: appsv1alpha1.InPlaceIfPossibleCloneSetUpdateStrategyType},
				},
			},
			updateRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "rev_new"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			revisions: []*apps.ControllerRevision{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rev_old"},
					Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.DefaultDeploymentUniqueLabelKey: "rev_old",
							apps.ControllerRevisionHashLabelKey:  "rev_old",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStateNormal),
						},
						Finalizers: []string{"slb/online"},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue, LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0",
						Labels: map[string]string{
							apps.DefaultDeploymentUniqueLabelKey: "rev_old",
							apps.ControllerRevisionHashLabelKey:  "rev_old",
							appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingUpdate),
						},
						Finalizers: []string{"slb/online"},
					},
					Spec: v1.PodSpec{
						ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						Containers:     []v1.Container{{Name: "c1", Image: "foo1"}},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{
							{Type: v1.PodReady, Status: v1.ConditionTrue},
							{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue, LastTransitionTime: now},
						},
						ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "image-id-xyz"}},
					},
				},
			},
		},
		{
			name:           "create: preparingNormal->Normal without hook, pod is not scheduled, should stay at preparingNormal",
			cs:             &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{Replicas: getInt32Pointer(1)}},
			updateRevision: &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev_new"}},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: v1.ContainersReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
			expectedPods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Labels: map[string]string{
						apps.ControllerRevisionHashLabelKey:  "rev_new",
						apps.DefaultDeploymentUniqueLabelKey: "rev_new",
						appspub.LifecycleStateKey:            string(appspub.LifecycleStatePreparingNormal),
					}},
					Spec: v1.PodSpec{ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}}},
					Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{
						{Type: v1.PodReady, Status: v1.ConditionFalse},
						{Type: v1.ContainersReady, Status: v1.ConditionFalse},
						{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue},
					}},
				},
			},
		},
	}

	inplaceupdate.Clock = testingclock.NewFakeClock(now.Time)
	for _, mc := range cases {
		t.Run(mc.name, func(t *testing.T) {
			initialObjs := mc.initial()
			for _, expectedPod := range mc.expectedPods {
				for _, sourcePod := range mc.pods {
					if sourcePod.Name != expectedPod.Name || (lifecycle.GetPodLifecycleState(expectedPod) != appspub.LifecycleStatePreparingUpdate &&
						(!clonesetutils.EqualToRevisionHash("", expectedPod, mc.updateRevision.Name) || clonesetutils.EqualToRevisionHash("", sourcePod, mc.updateRevision.Name))) {
						continue
					}
					var oldRevision *apps.ControllerRevision
					for _, revision := range mc.revisions {
						if clonesetutils.EqualToRevisionHash("", sourcePod, revision.Name) {
							oldRevision = revision
							break
						}
					}
					prepared, err := inplaceupdate.PrepareUpdate(sourcePod, oldRevision, mc.updateRevision, clonesetcore.New(mc.cs).GetUpdateOptions())
					if err != nil {
						t.Fatal(err)
					}
					if prepared != nil {
						plan, err := json.Marshal(prepared.Plan)
						if err != nil {
							t.Fatal(err)
						}
						if expectedPod.Annotations == nil {
							expectedPod.Annotations = map[string]string{}
						}
						expectedPod.Annotations[appspub.InPlaceUpdatePlanKey] = string(plan)
						if raw, exists := appspub.GetInPlaceUpdateState(expectedPod); exists {
							state := &appspub.InPlaceUpdateState{}
							if err := json.Unmarshal([]byte(raw), state); err != nil {
								t.Fatal(err)
							}
							state.UpdatePlan = prepared.Plan
							expectedPod.Annotations[appspub.InPlaceUpdateStateKey] = util.DumpJSON(state)
						}
					}
				}
			}
			fakeClient := fake.NewClientBuilder().WithObjects(initialObjs...).Build()
			ctrl := &realControl{
				fakeClient,
				lifecycle.New(fakeClient),
				inplaceupdate.New(fakeClient, clonesetutils.RevisionAdapterImpl),
				record.NewFakeRecorder(10),
				&controllerfinder.ControllerFinder{Client: fakeClient},
			}
			currentRevision := mc.updateRevision
			if len(mc.revisions) > 0 {
				currentRevision = mc.revisions[0]
			}
			if err := ctrl.Update(mc.cs, currentRevision, mc.updateRevision, mc.revisions, mc.pods, mc.pvcs); err != nil {
				t.Fatalf("Failed to test %s, manage error: %v", mc.name, err)
			}
			podList := v1.PodList{}
			if err := ctrl.Client.List(context.TODO(), &podList, &client.ListOptions{}); err != nil {
				t.Fatalf("Failed to test %s, get pods error: %v", mc.name, err)
			}
			if len(podList.Items) != len(mc.expectedPods) {
				t.Fatalf("Failed to test %s, unexpected pods length, expected %v, got %v", mc.name, util.DumpJSON(mc.expectedPods), util.DumpJSON(podList.Items))
			}
			for _, p := range mc.expectedPods {
				p.APIVersion = "v1"
				p.Kind = "Pod"

				gotPod := &v1.Pod{}
				if err := ctrl.Client.Get(context.TODO(), types.NamespacedName{Namespace: p.Namespace, Name: p.Name}, gotPod); err != nil {
					t.Fatalf("Failed to test %s, get pod %s error: %v", mc.name, p.Name, err)
				}
				gotPod.APIVersion = "v1"
				gotPod.Kind = "Pod"

				if v, ok := gotPod.Annotations[appspub.LifecycleTimestampKey]; ok {
					if p.Annotations == nil {
						p.Annotations = map[string]string{}
					}
					p.Annotations[appspub.LifecycleTimestampKey] = v
				}
				p.ResourceVersion = gotPod.ResourceVersion

				if !reflect.DeepEqual(gotPod, p) {
					t.Fatalf("Failed to test %s, unexpected pod %s, expected \n%v\n got \n%v", mc.name, p.Name, util.DumpJSON(p), util.DumpJSON(gotPod))
				}
			}
		})
	}
}

func newCloneSetUpdatePlanTest(t *testing.T, image, cpu string) (*realControl, *appsv1beta1.CloneSet, *v1.Pod, *apps.ControllerRevision, *apps.ControllerRevision) {
	t.Helper()
	cs := &appsv1beta1.CloneSet{
		ObjectMeta: metav1.ObjectMeta{Name: "plan-test"},
		Spec: appsv1beta1.CloneSetSpec{
			Replicas: getInt32Pointer(1),
			Lifecycle: &appspub.Lifecycle{InPlaceUpdate: &appspub.LifecycleHook{
				FinalizersHandler: []string{"approval"},
			}},
			UpdateStrategy: appsv1beta1.CloneSetUpdateStrategy{RollingUpdate: &appsv1beta1.RollingUpdateCloneSetStrategy{
				PodUpdatePolicy: appsv1beta1.InPlaceIfPossibleCloneSetPodUpdateStrategyType,
			}},
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-plan", UID: "pod-plan-uid", Finalizers: []string{"approval"},
			Labels: map[string]string{
				apps.ControllerRevisionHashLabelKey: "old", apps.DefaultDeploymentUniqueLabelKey: "old",
				appspub.LifecycleStateKey: string(appspub.LifecycleStateNormal),
			},
		},
		Spec: v1.PodSpec{
			ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
			Containers: []v1.Container{{Name: "c1", Image: "foo1", Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m")},
			}}},
		},
		Status: v1.PodStatus{
			Phase:             v1.PodRunning,
			Conditions:        []v1.PodCondition{{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue}},
			ContainerStatuses: []v1.ContainerStatus{{Name: "c1", ImageID: "old-image-id"}},
		},
	}
	revision := func(name string, spec v1.PodSpec) *apps.ControllerRevision {
		raw, err := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"template": v1.PodTemplateSpec{Spec: spec}}})
		if err != nil {
			t.Fatal(err)
		}
		return &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: runtime.RawExtension{Raw: raw}}
	}
	oldRevision := revision("old", pod.Spec)
	target := pod.Spec.DeepCopy()
	target.Containers[0].Image = image
	target.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse(cpu)
	newRevision := revision("new", *target)
	fakeClient := fake.NewClientBuilder().WithObjects(pod).Build()
	control := &realControl{
		Client: fakeClient, lifecycleControl: lifecycle.New(fakeClient),
		inplaceControl: inplaceupdate.New(fakeClient, clonesetutils.RevisionAdapterImpl),
		recorder:       record.NewFakeRecorder(10),
	}
	return control, cs, pod, oldRevision, newRevision
}

func TestCloneSetUpdatePlanBeforeHook(t *testing.T) {
	defer utilfeature.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlaceWorkloadVerticalScaling, true)()
	for _, tc := range []struct {
		name, image, cpu string
		changes          []string
		restart          bool
	}{
		{name: "image", image: "foo2", cpu: "100m", changes: []string{appspub.InPlaceUpdateChangeImage}, restart: true},
		{name: "resources", image: "foo1", cpu: "200m", changes: []string{appspub.InPlaceUpdateChangeResources}},
		{name: "image and resources", image: "foo2", cpu: "200m", changes: []string{appspub.InPlaceUpdateChangeImage, appspub.InPlaceUpdateChangeResources}, restart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control, cs, pod, oldRevision, newRevision := newCloneSetUpdatePlanTest(t, tc.image, tc.cpu)
			if _, err := control.updatePod(cs, clonesetcore.New(cs), newRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
				t.Fatal(err)
			}
			gotPod := &v1.Pod{}
			if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), gotPod); err != nil {
				t.Fatal(err)
			}
			if lifecycle.GetPodLifecycleState(gotPod) != appspub.LifecycleStatePreparingUpdate || !reflect.DeepEqual(gotPod.Spec, pod.Spec) {
				t.Fatalf("expected plan publication before mutation, got %s", util.DumpJSON(gotPod))
			}
			plan := &appspub.InPlaceUpdatePlan{}
			if err := json.Unmarshal([]byte(gotPod.Annotations[appspub.InPlaceUpdatePlanKey]), plan); err != nil {
				t.Fatal(err)
			}
			if plan.PlanID == "" || len(plan.Batches) != 1 || len(plan.Batches[0].Containers) != 1 {
				t.Fatalf("unexpected update plan: %+v", plan)
			}
			entry := plan.Batches[0].Containers[0]
			if entry.Name != "c1" || entry.RestartRequired != tc.restart || !reflect.DeepEqual(entry.Changes, tc.changes) {
				t.Fatalf("unexpected container plan: %+v", entry)
			}
		})
	}
}

func TestCloneSetUpdatePlanRearmsChangedTarget(t *testing.T) {
	control, cs, pod, oldRevision, newRevision := newCloneSetUpdatePlanTest(t, "foo2", "100m")
	if _, err := control.updatePod(cs, clonesetcore.New(cs), newRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
		t.Fatal(err)
	}
	if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	oldPlan := pod.Annotations[appspub.InPlaceUpdatePlanKey]
	pod.Finalizers = nil
	if err := control.Client.Update(context.TODO(), pod); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, changedRevision := newCloneSetUpdatePlanTest(t, "foo3", "100m")
	changedRevision.Name = "changed"
	if _, err := control.updatePod(cs, clonesetcore.New(cs), changedRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
		t.Fatal(err)
	}
	if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	if pod.Annotations[appspub.InPlaceUpdatePlanKey] == oldPlan || !lifecycle.IsPodHooked(cs.Spec.Lifecycle.InPlaceUpdate, pod) || pod.Spec.Containers[0].Image != "foo1" {
		t.Fatalf("changed target must replace the plan and rearm approval before updating: %s", util.DumpJSON(pod))
	}
	approvedPlan := pod.Annotations[appspub.InPlaceUpdatePlanKey]
	pod.Finalizers = nil
	if err := control.Client.Update(context.TODO(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := control.updatePod(cs, clonesetcore.New(cs), changedRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
		t.Fatal(err)
	}
	if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	if pod.Spec.Containers[0].Image != "foo3" || lifecycle.GetPodLifecycleState(pod) != appspub.LifecycleStateUpdating || pod.Annotations[appspub.InPlaceUpdatePlanKey] != approvedPlan {
		t.Fatalf("released current approval must execute once and retain its full plan: %s", util.DumpJSON(pod))
	}
}

func TestCloneSetCancelledUpdatePlanResumesOriginalCycle(t *testing.T) {
	for _, grace := range []bool{false, true} {
		t.Run(fmt.Sprintf("grace=%v", grace), func(t *testing.T) {
			control, cs, pod, oldRevision, newRevision := newCloneSetUpdatePlanTest(t, "foo2", "100m")
			if grace {
				cs.Spec.UpdateStrategy.RollingUpdate.InPlaceUpdateStrategy = &appspub.InPlaceUpdateStrategy{GracePeriodSeconds: 3600}
			}
			if _, err := control.updatePod(cs, clonesetcore.New(cs), newRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
				t.Fatal(err)
			}
			if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			originalPlan := pod.Annotations[appspub.InPlaceUpdatePlanKey]
			pod.Finalizers = nil
			if err := control.Client.Update(context.TODO(), pod); err != nil {
				t.Fatal(err)
			}
			if _, err := control.updatePod(cs, clonesetcore.New(cs), newRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
				t.Fatal(err)
			}
			if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			originalState := pod.Annotations[appspub.InPlaceUpdateStateKey]
			originalGrace := pod.Annotations[appspub.InPlaceUpdateGraceKey]
			if lifecycle.GetPodLifecycleState(pod) != appspub.LifecycleStateUpdating {
				t.Fatal("expected original update to be executing")
			}
			_, _, _, _, changedRevision := newCloneSetUpdatePlanTest(t, "foo3", "100m")
			changedRevision.Name = "changed"
			if _, err := control.updatePod(cs, clonesetcore.New(cs), changedRevision, []*apps.ControllerRevision{oldRevision, newRevision}, pod, nil); err != nil {
				t.Fatal(err)
			}
			if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			if lifecycle.GetPodLifecycleState(pod) != appspub.LifecycleStatePreparingUpdate || pod.Annotations[appspub.InPlaceUpdatePlanKey] == originalPlan {
				t.Fatal("expected new target to suspend the original update while awaiting approval")
			}
			if updated, _, err := control.refreshPodState(cs, clonesetcore.New(cs), pod, newRevision.Name); err != nil || !updated {
				t.Fatalf("expected cancellation to resume original update, updated=%v err=%v", updated, err)
			}
			if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			if lifecycle.GetPodLifecycleState(pod) != appspub.LifecycleStateUpdating || pod.Annotations[appspub.InPlaceUpdatePlanKey] != originalPlan ||
				pod.Annotations[appspub.InPlaceUpdateStateKey] != originalState || pod.Annotations[appspub.InPlaceUpdateGraceKey] != originalGrace {
				t.Fatalf("cancellation must restore original plan and execution without losing pending work: %s", util.DumpJSON(pod))
			}
		})
	}
}

func TestCloneSetUpdatePlanLifecycleCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source appspub.LifecycleStateType
		target appspub.LifecycleStateType
		hooked bool
		retain bool
	}{
		{name: "completed unlabeled pod", source: "", target: ""},
		{name: "completed normal pod", source: appspub.LifecycleStateNormal, target: appspub.LifecycleStateNormal},
		{name: "cancel to normal", source: appspub.LifecycleStatePreparingUpdate, target: appspub.LifecycleStateNormal, hooked: true},
		{name: "cancel to updated", source: appspub.LifecycleStatePreparingUpdate, target: appspub.LifecycleStateUpdated},
		{name: "post hook completed", source: appspub.LifecycleStateUpdated, target: appspub.LifecycleStateNormal, hooked: true},
		{name: "keep plan for post hook", source: appspub.LifecycleStateUpdating, target: appspub.LifecycleStateUpdated, retain: true},
		{name: "update completed", source: appspub.LifecycleStateUpdating, target: appspub.LifecycleStateNormal, hooked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control, cs, pod, _, _ := newCloneSetUpdatePlanTest(t, "foo2", "100m")
			pod.Labels[appspub.LifecycleStateKey] = string(tc.source)
			pod.Annotations = map[string]string{appspub.InPlaceUpdatePlanKey: `{"planID":"obsolete","batches":[]}`}
			if !tc.hooked {
				pod.Finalizers = nil
			}
			if err := control.Client.Update(context.TODO(), pod); err != nil {
				t.Fatal(err)
			}
			if updated, _, err := control.refreshPodState(cs, clonesetcore.New(cs), pod, "old"); err != nil || !updated {
				t.Fatalf("expected lifecycle refresh, updated=%v err=%v", updated, err)
			}
			if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			_, hasPlan := pod.Annotations[appspub.InPlaceUpdatePlanKey]
			if lifecycle.GetPodLifecycleState(pod) != tc.target || hasPlan != tc.retain {
				t.Fatalf("unexpected lifecycle or plan after refresh: %s", util.DumpJSON(pod))
			}
		})
	}
}

func TestCloneSetUpdatePlanPreservedWhileIncomplete(t *testing.T) {
	for _, state := range []appspub.LifecycleStateType{"", appspub.LifecycleStateNormal, appspub.LifecycleStatePreparingUpdate} {
		for _, grace := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/grace=%v", state, grace), func(t *testing.T) {
				control, cs, pod, _, _ := newCloneSetUpdatePlanTest(t, "foo2", "100m")
				pod.Labels[appspub.LifecycleStateKey] = string(state)
				pod.Annotations = map[string]string{appspub.InPlaceUpdatePlanKey: `{"planID":"active","batches":[]}`}
				updateState := appspub.InPlaceUpdateState{
					Revision: "old", UpdateTimestamp: metav1.Now(), UpdateImages: true,
					UpdatePlan:            &appspub.InPlaceUpdatePlan{PlanID: "active", Batches: []appspub.InPlaceUpdatePlanBatch{}},
					LastContainerStatuses: map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "old-image-id"}},
					NextContainerImages:   map[string]string{"c1": "foo2"},
				}
				if grace {
					updateState.NextContainerImages = nil
					pod.Annotations[appspub.InPlaceUpdateGraceKey] = util.DumpJSON(inplaceupdate.UpdateSpec{Revision: "old", GraceSeconds: 3600})
				}
				pod.Annotations[appspub.InPlaceUpdateStateKey] = util.DumpJSON(updateState)
				if err := control.Client.Update(context.TODO(), pod); err != nil {
					t.Fatal(err)
				}
				expectedState := state
				resume := state == appspub.LifecycleStatePreparingUpdate
				if resume {
					expectedState = appspub.LifecycleStateUpdating
				}
				if updated, _, err := control.refreshPodState(cs, clonesetcore.New(cs), pod, "old"); err != nil || updated != resume {
					t.Fatalf("incomplete update must preserve or resume original plan, updated=%v err=%v", updated, err)
				}
				if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
					t.Fatal(err)
				}
				if pod.Annotations[appspub.InPlaceUpdatePlanKey] == "" || lifecycle.GetPodLifecycleState(pod) != expectedState {
					t.Fatalf("expected active plan retained or restored: %s", util.DumpJSON(pod))
				}
			})
		}
	}
}

func TestCloneSetUpdatePlanClearedBeforeRecreate(t *testing.T) {
	control, cs, pod, oldRevision, newRevision := newCloneSetUpdatePlanTest(t, "foo2", "100m")
	cs.Spec.UpdateStrategy.RollingUpdate.PodUpdatePolicy = appsv1beta1.RecreateCloneSetPodUpdateStrategyType
	pod.Annotations = map[string]string{appspub.InPlaceUpdatePlanKey: `{"planID":"obsolete","batches":[]}`}
	if err := control.Client.Update(context.TODO(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := control.updatePod(cs, clonesetcore.New(cs), newRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
		t.Fatal(err)
	}
	if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	if _, exists := pod.Annotations[appspub.InPlaceUpdatePlanKey]; exists || pod.Labels[appsv1beta1.SpecifiedDeleteKey] != "" {
		t.Fatalf("expected only plan cleanup before recreation: %s", util.DumpJSON(pod))
	}
	if _, err := control.updatePod(cs, clonesetcore.New(cs), newRevision, []*apps.ControllerRevision{oldRevision}, pod, nil); err != nil {
		t.Fatal(err)
	}
	if err := control.Get(context.TODO(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	if pod.Labels[appsv1beta1.SpecifiedDeleteKey] != "true" {
		t.Fatalf("expected recreation after observing plan removal: %s", util.DumpJSON(pod))
	}
}

func TestSortUpdateIndexes(t *testing.T) {
	cases := []struct {
		strategy          appsv1alpha1.CloneSetUpdateStrategy
		pods              []*v1.Pod
		waitUpdateIndexes []int
		expectedIndexes   []int
	}{
		{
			strategy: appsv1alpha1.CloneSetUpdateStrategy{},
			pods: []*v1.Pod{
				{Status: v1.PodStatus{Phase: v1.PodPending, Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}}},
				{Status: v1.PodStatus{Phase: v1.PodPending}},
				{Status: v1.PodStatus{Phase: v1.PodPending}},
				{Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}}},
				{Status: v1.PodStatus{Phase: v1.PodRunning}},
			},
			waitUpdateIndexes: []int{0, 1, 3, 4},
			expectedIndexes:   []int{1, 0, 4, 3},
		},
	}

	coreControl := clonesetcore.New(&appsv1alpha1.CloneSet{})
	for i, tc := range cases {
		got := SortUpdateIndexes(coreControl, tc.strategy, tc.pods, tc.waitUpdateIndexes)
		if !reflect.DeepEqual(got, tc.expectedIndexes) {
			t.Fatalf("case #%d failed, expected %v, got %v", i, tc.expectedIndexes, got)
		}
	}
}

func TestCalculateUpdateCount(t *testing.T) {
	// Enable the CloneSetPartitionRollback feature-gate
	_ = utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%s=true", features.CloneSetPartitionRollback))

	readyPod := func() *v1.Pod {
		return &v1.Pod{Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}}}
	}
	cases := []struct {
		strategy           appsv1alpha1.CloneSetUpdateStrategy
		totalReplicas      int
		oldRevisionIndexes []int
		pods               []*v1.Pod
		expectedResult     int
	}{
		{
			strategy:           appsv1alpha1.CloneSetUpdateStrategy{},
			totalReplicas:      3,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{readyPod(), readyPod(), readyPod()},
			expectedResult:     1,
		},
		{
			strategy:           appsv1alpha1.CloneSetUpdateStrategy{},
			totalReplicas:      3,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{readyPod(), {}, readyPod()},
			expectedResult:     0,
		},
		{
			strategy:           appsv1alpha1.CloneSetUpdateStrategy{},
			totalReplicas:      3,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{{}, readyPod(), readyPod()},
			expectedResult:     1,
		},
		{
			strategy:           appsv1alpha1.CloneSetUpdateStrategy{},
			totalReplicas:      10,
			oldRevisionIndexes: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
			pods:               []*v1.Pod{{}, readyPod(), readyPod(), readyPod(), readyPod(), readyPod(), readyPod(), readyPod(), {}, readyPod()},
			expectedResult:     1,
		},
		{
			strategy:           appsv1alpha1.CloneSetUpdateStrategy{Partition: util.GetIntOrStrPointer(intstrutil.FromInt(2)), MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(3))},
			totalReplicas:      3,
			oldRevisionIndexes: []int{0, 1},
			pods:               []*v1.Pod{{}, readyPod(), readyPod()},
			expectedResult:     0,
		},
		{
			strategy:           appsv1alpha1.CloneSetUpdateStrategy{Partition: util.GetIntOrStrPointer(intstrutil.FromInt(2)), MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromString("50%"))},
			totalReplicas:      8,
			oldRevisionIndexes: []int{0, 1, 2, 3, 4, 5, 6},
			pods:               []*v1.Pod{{}, readyPod(), {}, readyPod(), readyPod(), readyPod(), readyPod(), {}},
			expectedResult:     3,
		},
		{
			// old revision all unavailable, partition = 0, maxUnavailable = 2, should only update 2 pods
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(0)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
			},
			totalReplicas:      5,
			oldRevisionIndexes: []int{0, 1, 2, 3, 4},
			pods:               []*v1.Pod{{}, {}, {}, {}, {}},
			expectedResult:     2,
		},
		{
			// old revision all unavailable, partition = 0, maxUnavailable = 2, 2 updating, should not update pods
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(0)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
			},
			totalReplicas:      5,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{{}, {}, {}, {}, {}},
			expectedResult:     0,
		},
		{
			// old revision all unavailable, partition = 0, maxUnavailable = 2, 1 updated and 1 updating, should only update 1 pods
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(0)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
			},
			totalReplicas:      5,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{{}, {}, {}, readyPod(), {}},
			expectedResult:     1,
		},
		{
			// old revision all unavailable, partition = 0, maxUnavailable = 2， maxSurge = 1, 1 creating, should only update 2 pods
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(0)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
				MaxSurge:       intstrutil.ValueOrDefault(nil, intstrutil.FromInt(1)),
			},
			totalReplicas:      5,
			oldRevisionIndexes: []int{0, 1, 2, 3, 4},
			pods:               []*v1.Pod{{}, {}, {}, {}, {}, {}},
			expectedResult:     2,
		},
		{
			// old revision all unavailable, partition = 0, maxUnavailable = 2， maxSurge = 1, 1 updated and 1 updating, should only update 2 pods
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(0)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
				MaxSurge:       intstrutil.ValueOrDefault(nil, intstrutil.FromInt(1)),
			},
			totalReplicas:      5,
			oldRevisionIndexes: []int{0, 1, 2, 3},
			pods:               []*v1.Pod{{}, {}, {}, {}, readyPod(), {}},
			expectedResult:     2,
		},
		{
			// old revision all unavailable, partition = 0, maxUnavailable = 2， maxSurge = 1, 1 updated and 2 updating, should only update 1 pods
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(0)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
				MaxSurge:       intstrutil.ValueOrDefault(nil, intstrutil.FromInt(1)),
			},
			totalReplicas:      5,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{{}, {}, {}, {}, readyPod(), {}},
			expectedResult:     1,
		},
		{
			// old revision all unavailable, partition = 0, maxUnavailable = 2， maxSurge = 1, 3 updating, should not update pods
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(0)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
				MaxSurge:       intstrutil.ValueOrDefault(nil, intstrutil.FromInt(1)),
			},
			totalReplicas:      5,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{{}, {}, {}, {}, {}, {}},
			expectedResult:     0,
		},
		{
			// rollback with maxUnavailable and pods in new revision are unavailable
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				Partition:      util.GetIntOrStrPointer(intstrutil.FromInt(7)),
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
			},
			totalReplicas:      8,
			oldRevisionIndexes: []int{0, 1, 2},
			pods:               []*v1.Pod{readyPod(), readyPod(), readyPod(), {}, {}, {}, {}, {}},
			expectedResult:     2,
		},
		{
			// maxUnavailable = 0 and maxSurge = 2, usedSurge = 1
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(0)),
				MaxSurge:       intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
			},
			totalReplicas:      4,
			oldRevisionIndexes: []int{0, 1},
			pods:               []*v1.Pod{readyPod(), readyPod(), readyPod(), readyPod(), readyPod()},
			expectedResult:     1,
		},
		{
			// maxUnavailable = 0 and maxSurge = 2, usedSurge = 2
			strategy: appsv1alpha1.CloneSetUpdateStrategy{
				MaxUnavailable: intstrutil.ValueOrDefault(nil, intstrutil.FromInt(0)),
				MaxSurge:       intstrutil.ValueOrDefault(nil, intstrutil.FromInt(2)),
			},
			totalReplicas:      4,
			oldRevisionIndexes: []int{0, 1, 2, 3},
			pods:               []*v1.Pod{readyPod(), readyPod(), readyPod(), readyPod(), readyPod(), readyPod()},
			expectedResult:     2,
		},
	}

	coreControl := clonesetcore.New(&appsv1alpha1.CloneSet{})
	for i, tc := range cases {
		currentRevision := "current"
		updateRevision := "updated"
		indexes := sets.NewInt(tc.oldRevisionIndexes...)
		var newRevisionIndexes []int
		for i, pod := range tc.pods {
			if !indexes.Has(i) {
				newRevisionIndexes = append(newRevisionIndexes, i)
				pod.Labels = map[string]string{apps.ControllerRevisionHashLabelKey: updateRevision}
			} else {
				pod.Labels = map[string]string{apps.ControllerRevisionHashLabelKey: currentRevision}
			}
		}

		replicas := int32(tc.totalReplicas)
		cs := &appsv1alpha1.CloneSet{Spec: appsv1alpha1.CloneSetSpec{Replicas: &replicas, UpdateStrategy: tc.strategy}}
		diffRes := calculateDiffsWithExpectation(cs, tc.pods, currentRevision, updateRevision, nil)

		var waitUpdateIndexes []int
		var targetRevision string
		if diffRes.updateNum > 0 {
			waitUpdateIndexes = tc.oldRevisionIndexes
			targetRevision = updateRevision
		} else if diffRes.updateNum < 0 {
			waitUpdateIndexes = newRevisionIndexes
			targetRevision = currentRevision
		}

		res := limitUpdateIndexes(coreControl, 0, diffRes, waitUpdateIndexes, tc.pods, targetRevision)
		if len(res) != tc.expectedResult {
			t.Fatalf("case #%d failed, expected %d, got %d", i, tc.expectedResult, res)
		}
	}
}
