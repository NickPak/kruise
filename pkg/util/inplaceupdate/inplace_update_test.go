/*
Copyright 2019 The Kruise Authors.

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

package inplaceupdate

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
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	appspub "github.com/openkruise/kruise/apis/apps/pub"
	utilclient "github.com/openkruise/kruise/pkg/client"
	"github.com/openkruise/kruise/pkg/features"
	"github.com/openkruise/kruise/pkg/util"
	utilcontainerlaunchpriority "github.com/openkruise/kruise/pkg/util/containerlaunchpriority"
	utilfeature "github.com/openkruise/kruise/pkg/util/feature"
	"github.com/openkruise/kruise/pkg/util/podadapter"
	"github.com/openkruise/kruise/pkg/util/revisionadapter"
)

func TestCalculateInPlaceUpdateSpec(t *testing.T) {
	cases := []struct {
		oldRevision  *apps.ControllerRevision
		newRevision  *apps.ControllerRevision
		expectedSpec *UpdateSpec
	}{
		{
			oldRevision: nil,
			newRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "new-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			expectedSpec: nil,
		},
		{
			oldRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "old-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
			},
			newRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "new-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			expectedSpec: &UpdateSpec{
				Revision:             "new-revision",
				ContainerImages:      map[string]string{"c1": "foo2"},
				ContainerResources:   map[string]v1.ResourceRequirements{},
				ContainerRefMetadata: make(map[string]metav1.ObjectMeta),
			},
		},
		{
			oldRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "old-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"metadata":{"labels":{"k":"v"}},"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
			},
			newRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "new-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"metadata":{"labels":{"k":"v"}},"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			expectedSpec: &UpdateSpec{
				Revision:             "new-revision",
				ContainerImages:      map[string]string{"c1": "foo2"},
				ContainerResources:   map[string]v1.ResourceRequirements{},
				ContainerRefMetadata: make(map[string]metav1.ObjectMeta),
			},
		},
		{
			oldRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "old-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","metadata":{"labels":{"k":"v"}},"spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
			},
			newRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "new-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","metadata":{"labels":{"k":"v","k1":"v1"}},"spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			expectedSpec: &UpdateSpec{
				Revision:             "new-revision",
				ContainerImages:      map[string]string{"c1": "foo2"},
				ContainerResources:   map[string]v1.ResourceRequirements{},
				ContainerRefMetadata: make(map[string]metav1.ObjectMeta),
				MetaDataPatch:        []byte(`{"metadata":{"labels":{"k1":"v1"}}}`),
			},
		},
		{
			oldRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "old-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","metadata":{"labels":{"k":"v"}},"spec":{"containers":[{"name":"c1","image":"foo1","env":[{"name":"TEST_ENV","valueFrom":{"fieldRef":{"apiVersion":"v1","fieldPath":"metadata.labels['k']"}}}]}]}}}}`)},
			},
			newRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "new-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","metadata":{"labels":{"k":"v2","k1":"v1"}},"spec":{"containers":[{"name":"c1","image":"foo2","env":[{"name":"TEST_ENV","valueFrom":{"fieldRef":{"apiVersion":"v1","fieldPath":"metadata.labels['k']"}}}]}]}}}}`)},
			},
			expectedSpec: &UpdateSpec{
				Revision:              "new-revision",
				ContainerImages:       map[string]string{"c1": "foo2"},
				ContainerResources:    map[string]v1.ResourceRequirements{},
				ContainerRefMetadata:  map[string]metav1.ObjectMeta{"c1": {Labels: map[string]string{"k": "v2"}}},
				MetaDataPatch:         []byte(`{"metadata":{"labels":{"k1":"v1"}}}`),
				UpdateEnvFromMetadata: true,
			},
		},
		{
			oldRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "old-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","metadata":{"labels":{"k":"v","k2":"v2"},"finalizers":["fz1","fz2"]},"spec":{"containers":[{"name":"c1","image":"foo1"}]}}}}`)},
			},
			newRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "new-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","metadata":{"labels":{"k":"v","k1":"v1"},"finalizers":["fz2"]},"spec":{"containers":[{"name":"c1","image":"foo2"}]}}}}`)},
			},
			expectedSpec: &UpdateSpec{
				Revision:             "new-revision",
				ContainerImages:      map[string]string{"c1": "foo2"},
				ContainerResources:   map[string]v1.ResourceRequirements{},
				ContainerRefMetadata: make(map[string]metav1.ObjectMeta),
				MetaDataPatch:        []byte(`{"metadata":{"$deleteFromPrimitiveList/finalizers":["fz1"],"$setElementOrder/finalizers":["fz2"],"labels":{"k1":"v1","k2":null}}}`),
			},
		},
		{
			oldRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "old-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"name":"c1","containers":[{"image":"foo1"}]}}}}`)},
			},
			newRevision: &apps.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "new-revision"},
				Data:       runtime.RawExtension{Raw: []byte(`{"spec":{"template":{"$patch":"replace","spec":{"containers":[{"name":"c1","image":"foo2","env":["name":"k", "value":"v"]}]}}}}`)},
			},
			expectedSpec: nil,
		},
	}

	// Enable the CloneSetPartitionRollback feature-gate
	_ = utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%s=true", features.InPlaceUpdateEnvFromMetadata))
	for i, tc := range cases {
		res := defaultCalculateInPlaceUpdateSpec(tc.oldRevision, tc.newRevision, nil)
		if !reflect.DeepEqual(res, tc.expectedSpec) {
			t.Fatalf("case #%d failed, expected %+v, got %+v", i, tc.expectedSpec, res)
		}
	}
}

func TestCheckInPlaceUpdateCompleted(t *testing.T) {
	succeedPods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "s1",
				Labels: map[string]string{
					apps.StatefulSetRevisionLabel: "new-revision",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "s2",
				Labels: map[string]string{
					apps.StatefulSetRevisionLabel: "new-revision",
				},
				Annotations: map[string]string{
					appspub.InPlaceUpdateStateKey: `{"revision":"new-revision","lastContainerStatuses":{"c1":{"imageID":"img01"}}}`,
				},
			},
			Status: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:    "c1",
						ImageID: "img02",
					},
				},
			},
		},
	}
	failPods := []*v1.Pod{
		//{
		//	ObjectMeta: metav1.ObjectMeta{
		//		Name: "f1",
		//		Labels: map[string]string{
		//			apps.StatefulSetRevisionLabel: "new-revision",
		//		},
		//		Annotations: map[string]string{
		//			appspub.InPlaceUpdateStateKey: `{"revision":"old-revision","lastContainerStatuses":{"c1":{"imageID":"img01"}}}`,
		//		},
		//	},
		//	Status: v1.PodStatus{
		//		ContainerStatuses: []v1.ContainerStatus{
		//			{
		//				Name:    "c1",
		//				ImageID: "img02",
		//			},
		//		},
		//	},
		//},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "f2",
				Labels: map[string]string{
					apps.StatefulSetRevisionLabel: "new-revision",
				},
				Annotations: map[string]string{
					appspub.InPlaceUpdateStateKey: `{"revision":"new-revision","lastContainerStatuses":{"c1":{"imageID":"img01"}}}`,
				},
			},
			Status: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:    "c1",
						ImageID: "img01",
						Image:   "image01",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "f3",
				Labels: map[string]string{
					apps.StatefulSetRevisionLabel: "new-revision",
				},
				Annotations: map[string]string{
					appspub.InPlaceUpdateStateKey: `{"revision":"new-revision","lastContainerStatuses":{"c1":{"imageID":"img01"}}}`,
				},
			},
			Status: v1.PodStatus{},
		},
	}

	for _, p := range succeedPods {
		if err := DefaultCheckInPlaceUpdateCompleted(p); err != nil {
			t.Errorf("pod %s expected check success, got %v", p.Name, err)
		}
	}
	for _, p := range failPods {
		if err := DefaultCheckInPlaceUpdateCompleted(p); err == nil {
			t.Errorf("pod %s expected check failure, got no error", p.Name)
		}
	}
}

func TestRefresh(t *testing.T) {
	aHourAgo := metav1.NewTime(time.Unix(time.Now().Add(-time.Hour).Unix(), 0))
	tenSecondsAgo := metav1.NewTime(time.Now().Add(-time.Second * 10))

	cases := []struct {
		name        string
		pod         *v1.Pod
		expectedPod *v1.Pod
	}{
		{
			name:        "no readiness-gate",
			pod:         &v1.Pod{},
			expectedPod: &v1.Pod{},
		},
		{
			name: "not in-place updated yet",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: `{"revision":"new-revision","lastContainerStatuses":{"c1":{"imageID":"img01"}}}`,
					},
				},
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
			},
			expectedPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: `{"revision":"new-revision","lastContainerStatuses":{"c1":{"imageID":"img01"}}}`,
					},
				},
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
			},
		},
		{
			name: "no existing condition",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
			},
			expectedPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionTrue, LastTransitionTime: aHourAgo}},
				},
			},
		},
		{
			name: "existing condition status is False",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionTrue,
						},
						{
							Type:   appspub.InPlaceUpdateReady,
							Status: v1.ConditionFalse,
						},
					},
				},
			},
			expectedPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionTrue,
						},
						{
							Type:               appspub.InPlaceUpdateReady,
							Status:             v1.ConditionTrue,
							LastTransitionTime: aHourAgo,
						},
					},
				},
			},
		},
		{
			name: "existing condition status is True",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionFalse,
						},
						{
							Type:   appspub.InPlaceUpdateReady,
							Status: v1.ConditionTrue,
						},
					},
				},
			},
			expectedPod: &v1.Pod{
				Spec: v1.PodSpec{
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionFalse,
						},
						{
							Type:   appspub.InPlaceUpdateReady,
							Status: v1.ConditionTrue,
						},
					},
				},
			},
		},
		{
			name: "in-place update still in grace",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{Revision: "new-revision", UpdateTimestamp: tenSecondsAgo, LastContainerStatuses: map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "img01"}}}),
						appspub.InPlaceUpdateGraceKey: `{"revision":"new-revision","containerImages":{"main":"img-name02"},"graceSeconds":30}`,
					},
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "main", Image: "img-name01"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
			},
			expectedPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{Revision: "new-revision", UpdateTimestamp: tenSecondsAgo, LastContainerStatuses: map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "img01"}}}),
						appspub.InPlaceUpdateGraceKey: `{"revision":"new-revision","containerImages":{"main":"img-name02"},"graceSeconds":30}`,
					},
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "main", Image: "img-name01"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
			},
		},
		{
			name: "in-place update reach grace period",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
							Revision:              "new-revision",
							UpdateTimestamp:       tenSecondsAgo,
							LastContainerStatuses: map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "img01"}},
						}),
						appspub.InPlaceUpdateGraceKey: `{"revision":"new-revision","containerImages":{"main":"img-name02"},"graceSeconds":5}`,
					},
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "main", Image: "img-name01"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
			},
			expectedPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
							Revision:               "new-revision",
							UpdateTimestamp:        tenSecondsAgo,
							LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "img01"}},
							ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: aHourAgo, Containers: []string{"main"}}},
						}),
					},
					ResourceVersion: "1",
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "main", Image: "img-name02"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
			},
		},
		{
			name: "do not in-place update the next batch if containers not consistent",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
							Revision:               "new-revision",
							UpdateTimestamp:        aHourAgo,
							LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "c1-img1-ID"}},
							ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: aHourAgo, Containers: []string{"c1"}}},
							NextContainerImages:    map[string]string{"c2": "c2-img2"},
						}),
					},
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "c1", Image: "c1-img2"}, {Name: "c2", Image: "c2-img1"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "c1", ImageID: "c1-img1-ID"},
						{Name: "c2", ImageID: "c2-img1-ID"},
					},
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionTrue,
						},
						{
							Type:               appspub.InPlaceUpdateReady,
							Status:             v1.ConditionFalse,
							LastTransitionTime: aHourAgo,
						},
					},
				},
			},
			expectedPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
							Revision:               "new-revision",
							UpdateTimestamp:        aHourAgo,
							LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "c1-img1-ID"}},
							ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: aHourAgo, Containers: []string{"c1"}}},
							NextContainerImages:    map[string]string{"c2": "c2-img2"},
						}),
					},
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "c1", Image: "c1-img2"}, {Name: "c2", Image: "c2-img1"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "c1", ImageID: "c1-img1-ID"},
						{Name: "c2", ImageID: "c2-img1-ID"},
					},
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionTrue,
						},
						{
							Type:               appspub.InPlaceUpdateReady,
							Status:             v1.ConditionFalse,
							LastTransitionTime: aHourAgo,
						},
					},
				},
			},
		},
		{
			name: "in-place update the next batch if containers have been consistent",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
							Revision:               "new-revision",
							UpdateTimestamp:        aHourAgo,
							LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "c1-img1-ID"}},
							ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: aHourAgo, Containers: []string{"c1"}}},
							NextContainerImages:    map[string]string{"c2": "c2-img2"},
						}),
					},
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "c1", Image: "c1-img2"}, {Name: "c2", Image: "c2-img1"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "c1", ImageID: "c1-img2-ID"},
						{Name: "c2", ImageID: "c2-img1-ID"},
					},
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionTrue,
						},
						{
							Type:               appspub.InPlaceUpdateReady,
							Status:             v1.ConditionFalse,
							LastTransitionTime: aHourAgo,
						},
					},
				},
			},
			expectedPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						apps.StatefulSetRevisionLabel: "new-revision",
					},
					Annotations: map[string]string{
						appspub.InPlaceUpdateStateKey: util.DumpJSON(appspub.InPlaceUpdateState{
							Revision:               "new-revision",
							UpdateTimestamp:        aHourAgo,
							LastContainerStatuses:  map[string]appspub.InPlaceUpdateContainerStatus{"c1": {ImageID: "c1-img1-ID"}, "c2": {ImageID: "c2-img1-ID"}},
							ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Timestamp: aHourAgo, Containers: []string{"c1"}}, {Timestamp: aHourAgo, Containers: []string{"c2"}}},
						}),
					},
				},
				Spec: v1.PodSpec{
					Containers:     []v1.Container{{Name: "c1", Image: "c1-img2"}, {Name: "c2", Image: "c2-img2"}},
					ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{Name: "c1", ImageID: "c1-img2-ID"},
						{Name: "c2", ImageID: "c2-img1-ID"},
					},
					Conditions: []v1.PodCondition{
						{
							Type:   v1.ContainersReady,
							Status: v1.ConditionTrue,
						},
						{
							Type:               appspub.InPlaceUpdateReady,
							Status:             v1.ConditionFalse,
							LastTransitionTime: aHourAgo,
						},
					},
				},
			},
		},
	}

	Clock = testingclock.NewFakeClock(aHourAgo.Time)
	for i, testCase := range cases {
		testCase.pod.Name = fmt.Sprintf("pod-%d", i)
		testCase.expectedPod.Name = fmt.Sprintf("pod-%d", i)
		testCase.expectedPod.APIVersion = "v1"
		testCase.expectedPod.Kind = "Pod"

		cli := fake.NewClientBuilder().WithObjects(testCase.pod).Build()
		ctrl := New(cli, revisionadapter.NewDefaultImpl())
		if res := ctrl.Refresh(testCase.pod, nil); res.RefreshErr != nil {
			t.Fatalf("failed to update condition: %v", res.RefreshErr)
		}

		got := &v1.Pod{}
		if err := cli.Get(context.TODO(), types.NamespacedName{Name: testCase.pod.Name}, got); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}

		got.APIVersion = "v1"
		got.Kind = "Pod"

		testCase.expectedPod.ResourceVersion = got.ResourceVersion
		if !reflect.DeepEqual(testCase.expectedPod, got) {
			t.Fatalf("case %s failed, expected \n%v\n got \n%v", testCase.name, util.DumpJSON(testCase.expectedPod), util.DumpJSON(got))
		}
	}
}

// resourceBatchTestAdapter emulates the resize subresource using a normal fake-client patch.
// Kubelet resource-status updates are simulated separately by the test.
type resourceBatchTestAdapter struct {
	*podadapter.AdapterRuntimeClient
	resizePatches int
}

func (a *resourceBatchTestAdapter) PatchPodResource(pod *v1.Pod, patch client.Patch) (*v1.Pod, error) {
	a.resizePatches++
	return a.PatchPod(pod, patch)
}

func TestRefreshResourceOnlyNextBatch(t *testing.T) {
	defer utilfeature.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlaceWorkloadVerticalScaling, true)()

	for _, gitVersion := range []string{"v1.31.0", "v1.35.2"} {
		t.Run(gitVersion, func(t *testing.T) {
			serverVersion := utilclient.GetCurrentServerVersion()
			originalVersion := *serverVersion
			serverVersion.GitVersion = gitVersion
			t.Cleanup(func() { *serverVersion = originalVersion })

			for _, tc := range []struct {
				name         string
				imageUpdated bool
				ready        bool
			}{
				{name: "previous image not updated", ready: true},
				{name: "previous container not ready", imageUpdated: true},
				{name: "advance resource-only batches", imageUpdated: true, ready: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					oldResources := v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m"), v1.ResourceMemory: resource.MustParse("128Mi")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("400m"), v1.ResourceMemory: resource.MustParse("512Mi")},
					}
					newResources := v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m"), v1.ResourceMemory: resource.MustParse("256Mi")},
					}
					pod := &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "resource-batch", Namespace: "default", UID: types.UID("original-pod")},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{Name: "helix", Image: "helix:v2", Resources: *oldResources.DeepCopy()},
								{
									Name: "meshproxy", Image: "meshproxy:v1", Resources: *oldResources.DeepCopy(),
									Env: []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(80, "resource-batch")},
								},
								{
									Name: "reloader", Image: "reloader:v1", Resources: *oldResources.DeepCopy(),
									Env: []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(10, "resource-batch")},
								},
							},
							ReadinessGates: []v1.PodReadinessGate{{ConditionType: appspub.InPlaceUpdateReady}},
						},
						Status: v1.PodStatus{
							ContainerStatuses: []v1.ContainerStatus{
								{Name: "helix", Image: "helix:v1", ImageID: "helix-old", Ready: tc.ready, Resources: oldResources.DeepCopy()},
								{Name: "meshproxy", Image: "meshproxy:v1", ContainerID: "meshproxy-original", Ready: true, Resources: oldResources.DeepCopy()},
								{Name: "reloader", Image: "reloader:v1", ContainerID: "reloader-original", Ready: true, Resources: oldResources.DeepCopy()},
							},
							Conditions: []v1.PodCondition{{Type: appspub.InPlaceUpdateReady, Status: v1.ConditionFalse}},
						},
					}
					if tc.imageUpdated {
						pod.Status.ContainerStatuses[0].Image = "helix:v2"
						pod.Status.ContainerStatuses[0].ImageID = "helix-new"
					}
					state := appspub.InPlaceUpdateState{
						Revision: "new-revision", UpdateImages: true, UpdateResources: true,
						LastContainerStatuses: map[string]appspub.InPlaceUpdateContainerStatus{"helix": {ImageID: "helix-old"}},
						NextContainerResources: map[string]v1.ResourceRequirements{
							"meshproxy": newResources, "reloader": newResources,
						},
						PreCheckBeforeNext:     &appspub.InPlaceUpdatePreCheckBeforeNext{ContainersRequiredReady: []string{"helix"}},
						ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Containers: []string{"helix"}}},
					}
					pod.Annotations = map[string]string{appspub.InPlaceUpdateStateKey: util.DumpJSON(state)}
					cli := fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()
					adapter := &resourceBatchTestAdapter{AdapterRuntimeClient: &podadapter.AdapterRuntimeClient{Client: cli}}
					ctrl := New(cli, revisionadapter.NewDefaultImpl()).(*realControl)
					ctrl.podAdapter = adapter

					refresh := func() *v1.Pod {
						t.Helper()
						current := &v1.Pod{}
						key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
						if err := cli.Get(context.TODO(), key, current); err != nil {
							t.Fatal(err)
						}
						if res := ctrl.Refresh(current, nil); res.RefreshErr != nil {
							t.Fatalf("failed to refresh: %v", res.RefreshErr)
						}
						if err := cli.Get(context.TODO(), key, current); err != nil {
							t.Fatal(err)
						}
						return current
					}
					readState := func(current *v1.Pod) appspub.InPlaceUpdateState {
						t.Helper()
						var got appspub.InPlaceUpdateState
						if err := json.Unmarshal([]byte(current.Annotations[appspub.InPlaceUpdateStateKey]), &got); err != nil {
							t.Fatal(err)
						}
						return got
					}

					got := refresh()
					if !tc.imageUpdated || !tc.ready {
						if !reflect.DeepEqual(got.Spec.Containers, pod.Spec.Containers) || adapter.resizePatches != 0 ||
							len(readState(got).ContainerBatchesRecord) != 1 {
							t.Fatal("resource batches advanced before the previous container completed and became ready")
						}
						return
					}
					if !got.Spec.Containers[1].Resources.Requests.Cpu().Equal(resource.MustParse("200m")) ||
						!got.Spec.Containers[1].Resources.Requests.Memory().Equal(resource.MustParse("256Mi")) {
						t.Fatalf("resource-only next batch did not update meshproxy: %+v", got.Spec.Containers[1].Resources)
					}
					if !reflect.DeepEqual(got.Spec.Containers[2].Resources, oldResources) {
						t.Fatal("lower-priority reloader resized before meshproxy completed")
					}
					state = readState(got)
					if len(state.NextContainerResources) != 1 || len(state.ContainerBatchesRecord) != 2 ||
						!reflect.DeepEqual(state.ContainerBatchesRecord[1].Containers, []string{"meshproxy"}) {
						t.Fatalf("unexpected intermediate batch state: %+v", state)
					}

					waiting := refresh()
					if len(readState(waiting).ContainerBatchesRecord) != 2 {
						t.Fatal("next resource batch advanced before kubelet reported resize completion")
					}
					waiting.Status.ContainerStatuses[1].Resources = waiting.Spec.Containers[1].Resources.DeepCopy()
					if err := cli.Status().Update(context.TODO(), waiting); err != nil {
						t.Fatal(err)
					}
					got = refresh()
					if !got.Spec.Containers[2].Resources.Requests.Cpu().Equal(resource.MustParse("200m")) {
						t.Fatalf("second resource-only batch did not update reloader: %+v", got.Spec.Containers[2].Resources)
					}
					state = readState(got)
					if len(state.NextContainerResources) != 0 || state.PreCheckBeforeNext != nil || len(state.ContainerBatchesRecord) != 3 {
						t.Fatalf("pending resource batches were not cleared: %+v", state)
					}
					if err := DefaultCheckInPlaceUpdateCompleted(got); err == nil {
						t.Fatal("update completed before kubelet reported the last resize")
					}
					got.Status.ContainerStatuses[2].Resources = got.Spec.Containers[2].Resources.DeepCopy()
					if err := cli.Status().Update(context.TODO(), got); err != nil {
						t.Fatal(err)
					}
					got = refresh()
					if err := DefaultCheckInPlaceUpdateCompleted(got); err != nil {
						t.Fatalf("update remained incomplete after all resource batches: %v", err)
					}
					if got.Status.Conditions[0].Status != v1.ConditionTrue {
						t.Fatal("InPlaceUpdateReady was not restored")
					}
					for i := 1; i < len(got.Spec.Containers); i++ {
						if got.Spec.Containers[i].Image != pod.Spec.Containers[i].Image ||
							!reflect.DeepEqual(got.Spec.Containers[i].Resources.Limits, oldResources.Limits) ||
							got.Status.ContainerStatuses[i].ContainerID != pod.Status.ContainerStatuses[i].ContainerID ||
							got.Status.ContainerStatuses[i].RestartCount != 0 {
							t.Fatal("resource-only batch changed an image, limit or container identity")
						}
					}
					if got.UID != pod.UID {
						t.Fatal("resource-only update changed Pod identity")
					}
					if gitVersion == "v1.35.2" && adapter.resizePatches != 2 {
						t.Fatalf("expected two resize-subresource patches, got %d", adapter.resizePatches)
					}
					if gitVersion == "v1.31.0" && adapter.resizePatches != 0 {
						t.Fatalf("legacy update unexpectedly used resize subresource: %d", adapter.resizePatches)
					}
				})
			}
		})
	}
}
