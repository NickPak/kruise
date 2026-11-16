/*
Copyright 2025 The Kruise Authors.

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

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	appspub "github.com/openkruise/kruise/apis/apps/pub"
	"github.com/openkruise/kruise/pkg/util/podreadiness"
)

type fakeAdapter struct {
	getPod             *corev1.Pod
	updatedPod         *corev1.Pod
	getCalled          bool
	updateCalled       bool
	updateStatusCalled bool
	updateCalls        int
	err                error
}

func (f *fakeAdapter) UpdatePod(pod *corev1.Pod) (*corev1.Pod, error) {
	f.updateCalled = true
	f.updateCalls++
	f.updatedPod = pod
	return pod, f.err
}

func (f *fakeAdapter) GetPod(namespace, name string) (*corev1.Pod, error) {
	if namespace == "" || name == "" {
		return nil, errors.New(fmt.Sprintf("GetPod namespace or name is empty, namespace: %s, name: %s", namespace, name))
	}

	pod := &corev1.Pod{}
	f.getCalled = true
	f.getPod = pod
	return pod, f.err
}

func (f *fakeAdapter) UpdatePodStatus(_ *corev1.Pod) error {
	f.updateStatusCalled = true
	return f.err
}

type fakePatchAdapter struct {
	fakeAdapter
	patchedPod          *corev1.Pod
	patchCalled         bool
	patchResourceCalled bool
	patchCalls          int
	patchData           []byte
	patchType           types.PatchType
}

func (f *fakePatchAdapter) PatchPod(pod *corev1.Pod, patch client.Patch) (*corev1.Pod, error) {
	f.patchCalled = true
	f.patchCalls++
	f.patchedPod = pod
	f.patchType = patch.Type()
	var err error
	f.patchData, err = patch.Data(pod)
	if err != nil {
		return nil, err
	}
	return pod, f.err
}
func (f *fakePatchAdapter) PatchPodResource(pod *corev1.Pod, _ client.Patch) (*corev1.Pod, error) {
	f.patchResourceCalled = true
	return pod, f.err
}

type fakePodReadinessControl struct {
	addCalled             bool
	removeCalled          bool
	containsReadinessGate bool
	err                   error
}

func (f *fakePodReadinessControl) AddNotReadyKey(_ *corev1.Pod, _ podreadiness.Message) error {
	f.addCalled = true
	return f.err
}
func (f *fakePodReadinessControl) RemoveNotReadyKey(_ *corev1.Pod, _ podreadiness.Message) error {
	f.removeCalled = true
	return f.err
}
func (f *fakePodReadinessControl) ContainsReadinessGate(_ *corev1.Pod) bool {
	return f.containsReadinessGate
}

func TestGetPodLifecycleState(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name string
		args args
		want appspub.LifecycleStateType
	}{
		{
			name: "label is PreparingNormal",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{appspub.LifecycleStateKey: "PreparingNormal"},
					},
				},
			},
			want: appspub.LifecycleStatePreparingNormal,
		},
		{
			name: "pod has unrelated labels only",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"unrelated": "value"},
					},
				},
			},
			want: "",
		},
		{
			name: "labels is nil",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: nil,
					},
				},
			},
			want: "",
		},
		{
			name: "pod is nil",
			args: args{
				pod: nil,
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPodLifecycleState(tt.args.pod); got != tt.want {
				t.Errorf("GetPodLifecycleState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsHookMarkPodNotReady(t *testing.T) {
	type args struct {
		lifecycleHook *appspub.LifecycleHook
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "hook is nil",
			args: args{
				lifecycleHook: nil,
			},
			want: false,
		},
		{
			name: "hook mark pod not ready",
			args: args{
				lifecycleHook: &appspub.LifecycleHook{
					MarkPodNotReady: true,
				},
			},
			want: true,
		},
		{
			name: "hook does not mark pod not ready",
			args: args{
				lifecycleHook: &appspub.LifecycleHook{
					MarkPodNotReady: false,
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsHookMarkPodNotReady(tt.args.lifecycleHook); got != tt.want {
				t.Errorf("IsHookMarkPodNotReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsLifecycleMarkPodNotReady(t *testing.T) {
	type args struct {
		lifecycle *appspub.Lifecycle
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "lifecycle is nil",
			args: args{
				lifecycle: nil,
			},
			want: false,
		},
		{
			name: "both hooks are nil",
			args: args{
				lifecycle: &appspub.Lifecycle{
					PreDelete:     nil,
					InPlaceUpdate: nil,
				},
			},
			want: false,
		},
		{
			name: "PreDelete marks pod not ready",
			args: args{lifecycle: &appspub.Lifecycle{
				PreDelete: &appspub.LifecycleHook{MarkPodNotReady: true},
			}},
			want: true,
		},
		{
			name: "InPlaceUpdate marks pod not ready",
			args: args{lifecycle: &appspub.Lifecycle{
				InPlaceUpdate: &appspub.LifecycleHook{MarkPodNotReady: true},
			}},
			want: true,
		},
		{
			name: "both hooks are false",
			args: args{lifecycle: &appspub.Lifecycle{
				PreDelete:     &appspub.LifecycleHook{MarkPodNotReady: false},
				InPlaceUpdate: &appspub.LifecycleHook{MarkPodNotReady: false},
			}},
			want: false,
		},
		{
			name: "both hooks are true",
			args: args{lifecycle: &appspub.Lifecycle{
				PreDelete:     &appspub.LifecycleHook{MarkPodNotReady: true},
				InPlaceUpdate: &appspub.LifecycleHook{MarkPodNotReady: true},
			}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsLifecycleMarkPodNotReady(tt.args.lifecycle); got != tt.want {
				t.Errorf("IsLifecycleMarkPodNotReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetPodLifecycle(t *testing.T) {
	type args struct {
		state appspub.LifecycleStateType
		pod   *corev1.Pod
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "pod is nil",
			args: args{
				state: appspub.LifecycleStateNormal,
				pod:   nil,
			},
		},
		{
			name: "empty pod, set state to Normal",
			args: args{
				state: appspub.LifecycleStateNormal,
				pod:   &corev1.Pod{},
			},
		},
		{
			name: "overwrite existing lifecycle state",
			args: args{
				state: appspub.LifecycleStateNormal,
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{appspub.LifecycleStateKey: string(appspub.LifecycleStatePreparingNormal)},
					},
				},
			},
		},
		{
			name: "annotation is nil, should initialize",
			args: args{
				state: appspub.LifecycleStateNormal,
				pod:   &corev1.Pod{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetPodLifecycle(tt.args.state)(tt.args.pod)
			if tt.args.pod == nil {
				return
			}
			if tt.args.pod.Labels[appspub.LifecycleStateKey] != string(tt.args.state) {
				t.Errorf("SetPodLifecycle() = %v, want %v", tt.args.pod.Labels[appspub.LifecycleStateKey], string(tt.args.state))
			}
			if tt.args.pod.Annotations == nil {
				t.Errorf("SetPodLifecycle() doesn't initialize annotations")
			}
			if ts := tt.args.pod.Annotations[appspub.LifecycleTimestampKey]; ts == "" {
				t.Errorf("SetPodLifecycle() missing timestamp annotation")
			}
		})
	}
}

func TestIsPodHooked(t *testing.T) {
	type args struct {
		hook *appspub.LifecycleHook
		pod  *corev1.Pod
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "hook is nil",
			args: args{
				hook: nil,
				pod:  &corev1.Pod{},
			},
			want: false,
		},
		{
			name: "pod is nil",
			args: args{
				hook: &appspub.LifecycleHook{},
				pod:  nil,
			},
			want: false,
		},
		{
			name: "finalizer matches",
			args: args{
				hook: &appspub.LifecycleHook{
					FinalizersHandler: []string{"kruise.io/unready-blocker"},
				},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{"kruise.io/unready-blocker"},
					},
				},
			},
			want: true,
		},
		{
			name: "label matches",
			args: args{
				hook: &appspub.LifecycleHook{
					LabelsHandler: map[string]string{"kruise.io/unready-blocker": "true"},
				},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"kruise.io/unready-blocker": "true"},
					},
				},
			},
			want: true,
		},
		{
			name: "finalizer and label both match",
			args: args{
				hook: &appspub.LifecycleHook{
					FinalizersHandler: []string{"kruise.io/unready-blocker"},
					LabelsHandler:     map[string]string{"kruise.io/unready-blocker": "true"},
				},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{"kruise.io/unready-blocker"},
						Labels:     map[string]string{"kruise.io/unready-blocker": "true"},
					},
				},
			},
			want: true,
		},
		{
			name: "no match",
			args: args{
				hook: &appspub.LifecycleHook{
					FinalizersHandler: []string{"kruise.io/unready-blocker"},
					LabelsHandler:     map[string]string{"kruise.io/unready-blocker": "true"},
				},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{"kruise.io/unready-blocker-other"},
						Labels:     map[string]string{"kruise.io/unready-blocker-other": "true"},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPodHooked(tt.args.hook, tt.args.pod); got != tt.want {
				t.Errorf("IsPodHooked() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPodAllHooked(t *testing.T) {
	type args struct {
		hook *appspub.LifecycleHook
		pod  *corev1.Pod
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "hook is nil",
			args: args{
				hook: nil,
				pod:  &corev1.Pod{},
			},
			want: false,
		},
		{
			name: "pod is nil",
			args: args{
				hook: &appspub.LifecycleHook{},
				pod:  nil,
			},
			want: false,
		},
		{
			name: "missing one finalizer",
			args: args{
				hook: &appspub.LifecycleHook{
					FinalizersHandler: []string{"kruise.io/unready-blocker-a", "kruise.io/unready-blocker-b"},
				},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{"kruise.io/unready-blocker-a"},
					},
				},
			},
			want: false,
		},
		{
			name: "label value mismatch",
			args: args{
				hook: &appspub.LifecycleHook{
					LabelsHandler: map[string]string{"kruise.io/unready-blocker": "true"},
				},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"kruise.io/unready-blocker": "false"},
					},
				},
			},
			want: false,
		},
		{
			name: "finalizer and label both match completely",
			args: args{
				hook: &appspub.LifecycleHook{
					FinalizersHandler: []string{"kruise.io/unready-blocker-a", "kruise.io/unready-blocker-b"},
					LabelsHandler:     map[string]string{"kruise.io/unready-blocker": "true"},
				},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{"kruise.io/unready-blocker-a", "kruise.io/unready-blocker-b"},
						Labels:     map[string]string{"kruise.io/unready-blocker": "true"},
					},
				},
			},
			want: true,
		},
		{
			name: "hook is empty, pod is non-empty",
			args: args{
				hook: &appspub.LifecycleHook{},
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Finalizers: []string{"kruise.io/unready-blocker"},
						Labels:     map[string]string{"kruise.io/unready-blocker": "true"},
					},
				},
			},
			want: true,
		},
		{
			name: "hook and pod are both empty",
			args: args{
				hook: &appspub.LifecycleHook{},
				pod:  &corev1.Pod{},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPodAllHooked(tt.args.hook, tt.args.pod); got != tt.want {
				t.Errorf("IsPodAllHooked() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_realControl_executePodNotReadyPolicy(t *testing.T) {
	type args struct {
		state appspub.LifecycleStateType
	}
	tests := []struct {
		name       string
		args       args
		wantAdd    bool
		wantRemove bool
		wantErr    bool
	}{
		{
			name: "preparingDelete triggers AddNotReadyKey",
			args: args{
				state: appspub.LifecycleStatePreparingDelete,
			},
			wantAdd: true,
		},
		{
			name:    "preparingUpdate triggers AddNotReadyKey",
			args:    args{state: appspub.LifecycleStatePreparingUpdate},
			wantAdd: true,
		},
		{
			name:       "Updated triggers RemoveNotReadyKey",
			args:       args{appspub.LifecycleStateUpdated},
			wantRemove: true,
		},
		{
			name:       "Normal triggers nothing",
			args:       args{appspub.LifecycleStateNormal},
			wantAdd:    false,
			wantRemove: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakePodReadinessControl{}
			rc := &realControl{
				podReadinessControl: fake,
			}
			pod := &corev1.Pod{}
			err := rc.executePodNotReadyPolicy(pod, tt.args.state)
			if tt.wantErr && err == nil {
				t.Errorf("rc.executePodNotReadyPolicy() expected error")
			}
			if tt.wantAdd && !fake.addCalled {
				t.Errorf("AddNotReadyKey not called")
			}
			if tt.wantRemove && !fake.removeCalled {
				t.Errorf("RemoveNotReadyKey not called")
			}
			if !tt.wantAdd && fake.addCalled {
				t.Errorf("AddNotReadyKey called")
			}
			if !tt.wantRemove && fake.removeCalled {
				t.Errorf("RemoveNotReadyKey called")
			}
		})
	}
}

func Test_realControl_UpdatePodLifecycle(t *testing.T) {
	t.Run("error from executePodNotReadyPolicy", func(t *testing.T) {
		silenceKlogForTest(t)
		rc := &realControl{
			adp: &fakeAdapter{},
			podReadinessControl: &fakePodReadinessControl{
				err: errors.New("readiness error"),
			},
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{},
		}}
		updated, gotPod, err := rc.UpdatePodLifecycle(pod, appspub.LifecycleStatePreparingDelete, true)
		if err == nil || updated {
			t.Errorf("expected error, got nil; updated=%v", updated)
		}
		if gotPod != nil {
			t.Errorf("gotPod should be nil on failure")
		}
	})

	t.Run("no-op if already in target state", func(t *testing.T) {
		rc := &realControl{
			adp:                 &fakeAdapter{},
			podReadinessControl: &fakePodReadinessControl{},
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{appspub.LifecycleStateKey: string(appspub.LifecycleStatePreparingDelete)},
		}}
		updated, gotPod, err := rc.UpdatePodLifecycle(pod, appspub.LifecycleStatePreparingDelete, false)
		if err != nil || updated == true {
			t.Errorf("expected no update, got updated=%v err=%v", updated, err)
		}
		if gotPod != pod {
			t.Errorf("expected returned pod == input pod")
		}
	})

	t.Run("patch path triggered", func(t *testing.T) {
		adp := &fakePatchAdapter{}
		rc := &realControl{
			adp:                 adp,
			podReadinessControl: &fakePodReadinessControl{},
		}
		pod := &corev1.Pod{}
		updated, gotPod, err := rc.UpdatePodLifecycle(pod, appspub.LifecycleStatePreparingDelete, false)
		if err != nil || !updated || !adp.patchCalled {
			t.Errorf("patch path failed: updated=%v err=%v patchCalled=%v", updated, err, adp.patchCalled)
		}
		if gotPod == nil {
			t.Errorf("expected gotPod to be non-nil")
		}
	})

	t.Run("fallback to UpdatePod if no PatchPod", func(t *testing.T) {
		adp := &fakeAdapter{}
		rc := &realControl{
			adp:                 adp,
			podReadinessControl: &fakePodReadinessControl{},
		}
		pod := &corev1.Pod{}
		updated, gotPod, err := rc.UpdatePodLifecycle(pod, appspub.LifecycleStatePreparingDelete, false)
		if err != nil || !updated || !adp.updateCalled {
			t.Errorf("fallback update path failed: updated=%v err=%v updateCalled=%v", updated, err, adp.updateCalled)
		}
		if gotPod == nil {
			t.Errorf("expected gotPod to be non-nil")
		}
	})
}

func Test_realControl_UpdatePodLifecycleWithHandler(t *testing.T) {
	t.Run("empty handler or empty pod", func(t *testing.T) {
		rc := &realControl{
			adp:                 &fakeAdapter{},
			podReadinessControl: &fakePodReadinessControl{},
		}
		handler := &appspub.LifecycleHook{}
		pod := &corev1.Pod{}
		updated, gotPod, err := rc.UpdatePodLifecycleWithHandler(pod, appspub.LifecycleStatePreparingDelete, nil)
		if err != nil || updated || gotPod == nil {
			t.Errorf("expected error, got nil; updated=%v, pod=%v", updated, gotPod)
		}
		updated, gotPod, err = rc.UpdatePodLifecycleWithHandler(nil, "", handler)
		if err != nil || updated || gotPod != nil {
			t.Errorf("expected error, got nil; updated=%v, pod=%v", updated, gotPod)
		}
	})
	t.Run("error from executePodNotReadyPolicy", func(t *testing.T) {
		silenceKlogForTest(t)
		rc := &realControl{
			adp: &fakeAdapter{},
			podReadinessControl: &fakePodReadinessControl{
				err: errors.New("readiness error"),
			},
		}
		handler := &appspub.LifecycleHook{
			MarkPodNotReady: true,
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{},
		}}
		updated, gotPod, err := rc.UpdatePodLifecycleWithHandler(pod, appspub.LifecycleStatePreparingDelete, handler)
		if err == nil || updated {
			t.Errorf("expected error, got nil; updated=%v", updated)
		}
		if gotPod != nil {
			t.Errorf("gotPod should be nil on failure")
		}
	})

	t.Run("no-op if already in target state", func(t *testing.T) {
		rc := &realControl{
			adp:                 &fakeAdapter{},
			podReadinessControl: &fakePodReadinessControl{},
		}
		handler := &appspub.LifecycleHook{
			MarkPodNotReady: false,
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{appspub.LifecycleStateKey: string(appspub.LifecycleStatePreparingDelete)},
		}}
		updated, gotPod, err := rc.UpdatePodLifecycleWithHandler(pod, appspub.LifecycleStatePreparingDelete, handler)
		if err != nil || updated == true {
			t.Errorf("expected no update, got updated=%v err=%v", updated, err)
		}
		if gotPod != pod {
			t.Errorf("expected returned pod == input pod")
		}
	})

	t.Run("patch path triggered", func(t *testing.T) {
		adp := &fakePatchAdapter{}
		rc := &realControl{
			adp:                 adp,
			podReadinessControl: &fakePodReadinessControl{},
		}
		handler := &appspub.LifecycleHook{
			MarkPodNotReady:   false,
			LabelsHandler:     map[string]string{"kruise.io/unready-blocker": "true"},
			FinalizersHandler: []string{"kruise.io/unready-blocker"},
		}
		pod := &corev1.Pod{}
		updated, gotPod, err := rc.UpdatePodLifecycleWithHandler(pod, appspub.LifecycleStatePreparingDelete, handler)
		if err != nil || !updated || !adp.patchCalled {
			t.Errorf("patch path failed: updated=%v err=%v patchCalled=%v", updated, err, adp.patchCalled)
		}
		if gotPod == nil {
			t.Errorf("expected gotPod to be non-nil")
		}
	})

	t.Run("fallback to UpdatePod if no PatchPod", func(t *testing.T) {
		adp := &fakeAdapter{}
		rc := &realControl{
			adp:                 adp,
			podReadinessControl: &fakePodReadinessControl{},
		}
		handler := &appspub.LifecycleHook{
			MarkPodNotReady:   false,
			LabelsHandler:     map[string]string{"kruise.io/unready-blocker": "true"},
			FinalizersHandler: []string{"kruise.io/unready-blocker"},
		}
		pod := &corev1.Pod{}
		updated, gotPod, err := rc.UpdatePodLifecycleWithHandler(pod, appspub.LifecycleStatePreparingDelete, handler)
		if err != nil || !updated || !adp.updateCalled {
			t.Errorf("fallback update path failed: updated=%v err=%v updateCalled=%v", updated, err, adp.updateCalled)
		}
		if gotPod == nil {
			t.Errorf("expected gotPod to be non-nil")
		}
	})
}

func Test_realControl_UpdatePodLifecycleWithMetadata(t *testing.T) {
	for _, usePatch := range []bool{false, true} {
		for _, state := range []appspub.LifecycleStateType{appspub.LifecycleStatePreparingUpdate, appspub.LifecycleStateUpdating} {
			t.Run(fmt.Sprintf("patch=%v/state=%s", usePatch, state), func(t *testing.T) {
				adp := &fakeAdapter{}
				patchAdp := &fakePatchAdapter{}
				rc := &realControl{adp: adp}
				if usePatch {
					rc.adp = patchAdp
				}
				pod := lifecycleMetadataTestPod()
				before := pod.DeepCopy()
				handler := &appspub.LifecycleHook{
					LabelsHandler:     map[string]string{"example.com/hook": "restored"},
					FinalizersHandler: []string{"example.com/existing", "example.com/new", "example.com/new"},
				}
				plan := `{"target":"rev\"new","phase":"Applying"}`
				mutateCalls := 0
				updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(pod, state, false, handler, func(clone *corev1.Pod) {
					mutateCalls++
					clone.Annotations["example.com/plan"] = plan
					delete(clone.Annotations, "example.com/obsolete")
					clone.Name = "must-not-change"
					clone.ResourceVersion = "must-not-change"
					clone.UID = "must-not-change"
					clone.OwnerReferences = nil
					clone.Spec.Containers[0].Image = "must-not-change"
					clone.Status.Phase = corev1.PodFailed
				})
				if err != nil || !updated || gotPod == nil || mutateCalls != 1 {
					t.Fatalf("updated=%v pod=%v err=%v mutateCalls=%d", updated, gotPod, err, mutateCalls)
				}
				if gotPod == pod || !reflect.DeepEqual(pod, before) {
					t.Fatal("metadata update mutated the input pod")
				}
				expected := before.DeepCopy()
				expected.Labels[appspub.LifecycleStateKey] = string(state)
				expected.Labels["example.com/hook"] = "restored"
				expected.Annotations["example.com/plan"] = plan
				delete(expected.Annotations, "example.com/obsolete")
				expected.Finalizers = append(expected.Finalizers, "example.com/new")
				if state != appspub.LifecycleStatePreparingUpdate {
					timestamp := gotPod.Annotations[appspub.LifecycleTimestampKey]
					if _, err := time.Parse(time.RFC3339, timestamp); err != nil || timestamp == before.Annotations[appspub.LifecycleTimestampKey] {
						t.Fatalf("invalid lifecycle timestamp %q: %v", timestamp, err)
					}
					expected.Annotations[appspub.LifecycleTimestampKey] = timestamp
				}
				if !reflect.DeepEqual(gotPod, expected) {
					t.Fatalf("unexpected metadata update:\ngot: %#v\nwant: %#v", gotPod, expected)
				}
				if !usePatch {
					if adp.updateCalls != 1 || adp.getCalled || adp.updateStatusCalled {
						t.Fatalf("expected one metadata update, adapter=%#v", adp)
					}
					return
				}
				if patchAdp.patchCalls != 1 || patchAdp.updateCalled || patchAdp.getCalled || patchAdp.patchResourceCalled {
					t.Fatalf("expected one metadata patch, adapter=%#v", patchAdp)
				}
				if patchAdp.patchType != types.MergePatchType {
					t.Fatalf("unexpected patch type %q", patchAdp.patchType)
				}
				var payload map[string]json.RawMessage
				if err := json.Unmarshal(patchAdp.patchData, &payload); err != nil {
					t.Fatal(err)
				}
				if len(payload) != 1 || payload["metadata"] == nil {
					t.Fatalf("patch must contain metadata only: %s", patchAdp.patchData)
				}
				var metadata struct {
					ResourceVersion string             `json:"resourceVersion"`
					Labels          map[string]string  `json:"labels"`
					Annotations     map[string]*string `json:"annotations"`
					Finalizers      []string           `json:"finalizers"`
				}
				if err := json.Unmarshal(payload["metadata"], &metadata); err != nil {
					t.Fatal(err)
				}
				if metadata.ResourceVersion != before.ResourceVersion || metadata.Labels["example.com/hook"] != "restored" ||
					metadata.Annotations["example.com/plan"] == nil || *metadata.Annotations["example.com/plan"] != plan ||
					!reflect.DeepEqual(metadata.Finalizers, expected.Finalizers) {
					t.Fatalf("patch is missing atomic metadata or old resourceVersion: %s", patchAdp.patchData)
				}
				if value, exists := metadata.Annotations["example.com/obsolete"]; !exists || value != nil {
					t.Fatalf("patch must delete obsolete annotation: %s", patchAdp.patchData)
				}
				if state == appspub.LifecycleStatePreparingUpdate {
					if _, exists := metadata.Annotations[appspub.LifecycleTimestampKey]; exists {
						t.Fatalf("same-state repair must preserve lifecycle timestamp: %s", patchAdp.patchData)
					}
				} else if metadata.Labels[appspub.LifecycleStateKey] != string(state) || metadata.Annotations[appspub.LifecycleTimestampKey] == nil {
					t.Fatalf("patch must carry lifecycle and timestamp together: %s", patchAdp.patchData)
				}
			})
		}
	}
}

func Test_realControl_UpdatePodLifecycleWithMetadataNoop(t *testing.T) {
	for _, usePatch := range []bool{false, true} {
		for _, emptyPod := range []bool{false, true} {
			t.Run(fmt.Sprintf("patch=%v/empty=%v", usePatch, emptyPod), func(t *testing.T) {
				adp := &fakeAdapter{}
				patchAdp := &fakePatchAdapter{}
				rc := &realControl{adp: adp}
				if usePatch {
					rc.adp = patchAdp
				}
				pod := lifecycleMetadataTestPod()
				handler := &appspub.LifecycleHook{FinalizersHandler: []string{"example.com/existing", "example.com/existing"}}
				if emptyPod {
					pod = &corev1.Pod{}
					handler = nil
				}
				before := pod.DeepCopy()
				updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(pod, GetPodLifecycleState(pod), false, handler, func(clone *corev1.Pod) {
					clone.Name = "ignored"
					clone.ResourceVersion = "ignored"
					clone.Spec.NodeName = "ignored"
					clone.Status.Phase = corev1.PodFailed
				})
				if err != nil || updated || gotPod != pod {
					t.Fatalf("expected original pod and no update: updated=%v pod=%v err=%v", updated, gotPod, err)
				}
				if !reflect.DeepEqual(pod, before) || adp.updateCalled || patchAdp.patchCalled {
					t.Fatal("no-op changed the input or wrote metadata")
				}
			})
		}
	}
}

func Test_realControl_UpdatePodLifecycleWithMetadataConflict(t *testing.T) {
	for _, usePatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("patch=%v", usePatch), func(t *testing.T) {
			conflict := apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, "pod", errors.New("old resourceVersion"))
			adp := &fakeAdapter{err: conflict}
			patchAdp := &fakePatchAdapter{fakeAdapter: fakeAdapter{err: conflict}}
			rc := &realControl{adp: adp}
			if usePatch {
				rc.adp = patchAdp
			}
			pod := lifecycleMetadataTestPod()
			before := pod.DeepCopy()
			mutateCalls := 0
			_, _, err := rc.UpdatePodLifecycleWithMetadata(pod, appspub.LifecycleStateUpdating, false, nil, func(clone *corev1.Pod) {
				mutateCalls++
				clone.Annotations["example.com/plan"] = "stale-plan"
			})
			if !apierrors.IsConflict(err) || mutateCalls != 1 || !reflect.DeepEqual(pod, before) {
				t.Fatalf("expected surfaced conflict and isolated single mutation: err=%v calls=%d", err, mutateCalls)
			}
			if usePatch {
				if patchAdp.patchCalls != 1 || patchAdp.getCalled || patchAdp.updateCalled || patchAdp.patchedPod.ResourceVersion != before.ResourceVersion {
					t.Fatal("patch retried or changed resourceVersion")
				}
			} else if adp.updateCalls != 1 || adp.getCalled || adp.updatedPod.ResourceVersion != before.ResourceVersion {
				t.Fatal("update retried or changed resourceVersion")
			}
		})
	}

	t.Run("runtime client rejects stale resourceVersion", func(t *testing.T) {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		pod := lifecycleMetadataTestPod()
		before := pod.DeepCopy()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()
		live := &corev1.Pod{}
		key := client.ObjectKeyFromObject(pod)
		if err := c.Get(context.TODO(), key, live); err != nil {
			t.Fatal(err)
		}
		live.Labels["example.com/concurrent"] = "preserve"
		if err := c.Update(context.TODO(), live); err != nil {
			t.Fatal(err)
		}
		rc := New(c)
		mutateCalls := 0
		mutate := func(clone *corev1.Pod) {
			mutateCalls++
			clone.Annotations["example.com/plan"] = "new-plan"
		}
		_, _, err := rc.UpdatePodLifecycleWithMetadata(pod, appspub.LifecycleStateUpdating, false, nil, mutate)
		if !apierrors.IsConflict(err) || mutateCalls != 1 || !reflect.DeepEqual(pod, before) {
			t.Fatalf("expected stale resourceVersion conflict, err=%v calls=%d", err, mutateCalls)
		}
		if err := c.Get(context.TODO(), key, live); err != nil {
			t.Fatal(err)
		}
		if live.Annotations["example.com/plan"] != "" || GetPodLifecycleState(live) != appspub.LifecycleStatePreparingUpdate {
			t.Fatal("conflicting patch partially applied metadata")
		}
		updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(live, appspub.LifecycleStateUpdating, false, nil, mutate)
		if err != nil || !updated || gotPod.Labels["example.com/concurrent"] != "preserve" || gotPod.Annotations["example.com/plan"] != "new-plan" {
			t.Fatalf("fresh recomputed update failed: updated=%v err=%v pod=%v", updated, err, gotPod)
		}
	})
}

func Test_realControl_UpdatePodLifecycleWithMetadataReadiness(t *testing.T) {
	for _, tt := range []struct {
		name    string
		state   appspub.LifecycleStateType
		mark    bool
		handler *appspub.LifecycleHook
		wantAdd bool
		wantDel bool
		wantErr bool
	}{
		{name: "preparing delete", state: appspub.LifecycleStatePreparingDelete, mark: true, wantAdd: true},
		{name: "preparing update", state: appspub.LifecycleStatePreparingUpdate, mark: true, wantAdd: true},
		{name: "updated", state: appspub.LifecycleStateUpdated, mark: true, wantDel: true},
		{name: "normal", state: appspub.LifecycleStateNormal, mark: true, wantDel: true},
		{name: "disabled", state: appspub.LifecycleStatePreparingUpdate},
		{name: "handler policy", state: appspub.LifecycleStatePreparingUpdate, handler: &appspub.LifecycleHook{MarkPodNotReady: true}, wantAdd: true},
		{name: "readiness failure", state: appspub.LifecycleStatePreparingUpdate, mark: true, wantAdd: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			adp := &fakeAdapter{}
			readiness := &fakePodReadinessControl{}
			if tt.wantErr {
				readiness.err = errors.New("readiness error")
			}
			rc := &realControl{adp: adp, podReadinessControl: readiness}
			pod := lifecycleMetadataTestPod()
			pod.Labels[appspub.LifecycleStateKey] = string(tt.state)
			before := pod.DeepCopy()
			mutateCalled := false
			updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(pod, tt.state, tt.mark, tt.handler, func(_ *corev1.Pod) {
				mutateCalled = true
			})
			if (err != nil) != tt.wantErr || updated || readiness.addCalled != tt.wantAdd || readiness.removeCalled != tt.wantDel {
				t.Fatalf("unexpected readiness policy: updated=%v err=%v readiness=%#v", updated, err, readiness)
			}
			if tt.wantErr && (gotPod != nil || mutateCalled) || !tt.wantErr && (gotPod != pod || !mutateCalled) {
				t.Fatal("readiness policy did not precede mutation/no-op checking")
			}
			if adp.updateCalled || !reflect.DeepEqual(pod, before) {
				t.Fatal("readiness policy changed input or wrote metadata on no-op/failure")
			}
		})
	}
}

func Test_realControl_UpdatePodLifecycleWithMetadataNilPod(t *testing.T) {
	rc := &realControl{}
	updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(nil, appspub.LifecycleStatePreparingUpdate, true, &appspub.LifecycleHook{MarkPodNotReady: true}, func(_ *corev1.Pod) {
		t.Fatal("nil pod must not invoke mutation")
	})
	if updated || gotPod != nil || err != nil {
		t.Fatalf("expected nil pod no-op: updated=%v pod=%v err=%v", updated, gotPod, err)
	}
}

func Test_realControl_UpdatePodLifecycleWithMetadataReadinessRetry(t *testing.T) {
	for _, state := range []appspub.LifecycleStateType{appspub.LifecycleStatePreparingDelete, appspub.LifecycleStatePreparingUpdate, appspub.LifecycleStateUpdated, appspub.LifecycleStateNormal} {
		t.Run(string(state), func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			pod := lifecycleMetadataTestPod()
			pod.Labels[appspub.LifecycleStateKey] = string(appspub.LifecycleStateNormal)
			pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: appspub.KruisePodReadyConditionType}}
			condition := corev1.PodCondition{Type: appspub.KruisePodReadyConditionType, Status: corev1.ConditionTrue}
			wantReady := corev1.ConditionFalse
			if state == appspub.LifecycleStateUpdated || state == appspub.LifecycleStateNormal {
				pod.Labels[appspub.LifecycleStateKey] = string(appspub.LifecycleStateUpdating)
				condition.Status = corev1.ConditionFalse
				message, err := json.Marshal([]podreadiness.Message{getReadinessMessage(preparingUpdateHookKey)})
				if err != nil {
					t.Fatal(err)
				}
				condition.Message = string(message)
				wantReady = corev1.ConditionTrue
			}
			pod.Status.Conditions = []corev1.PodCondition{condition, {Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			c := &lifecycleMetadataTrackingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1.Pod{}).WithObjects(pod).Build()}
			key := client.ObjectKeyFromObject(pod)
			if err := c.Get(context.TODO(), key, pod); err != nil {
				t.Fatal(err)
			}
			before := pod.DeepCopy()
			rc := New(c)
			plan := `{"id":"plan"}`
			mutate := func(clone *corev1.Pod) { clone.Annotations[appspub.InPlaceUpdatePlanKey] = plan }
			_, _, err := rc.UpdatePodLifecycleWithMetadata(pod, state, true, nil, mutate)
			if !apierrors.IsConflict(err) || c.statusUpdates != 1 || c.patchCalls != 1 || !reflect.DeepEqual(pod, before) {
				t.Fatalf("expected one readiness write then surfaced conflict: err=%v status=%d patches=%d", err, c.statusUpdates, c.patchCalls)
			}
			live := &corev1.Pod{}
			if err := c.Get(context.TODO(), key, live); err != nil {
				t.Fatal(err)
			}
			ready := podreadiness.GetReadinessCondition(live)
			if ready == nil || ready.Status != wantReady || live.ResourceVersion == before.ResourceVersion {
				t.Fatalf("readiness not applied before metadata conflict: condition=%v rv=%q", ready, live.ResourceVersion)
			}
			if !reflect.DeepEqual(live.ObjectMeta.Labels, before.Labels) || !reflect.DeepEqual(live.Annotations, before.Annotations) {
				t.Fatal("conflicting metadata patch partially published lifecycle or plan")
			}
			updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(live, state, true, nil, mutate)
			if err != nil || !updated || c.statusUpdates != 1 || c.patchCalls != 2 {
				t.Fatalf("fresh external retry must converge: updated=%v err=%v status=%d patches=%d", updated, err, c.statusUpdates, c.patchCalls)
			}
			if GetPodLifecycleState(gotPod) != state || gotPod.Annotations[appspub.InPlaceUpdatePlanKey] != plan || podreadiness.GetReadinessCondition(gotPod).Status != wantReady {
				t.Fatal("successful metadata retry lost readiness, lifecycle or plan")
			}
			updated, noopPod, err := rc.UpdatePodLifecycleWithMetadata(gotPod, state, true, nil, mutate)
			if err != nil || updated || noopPod != gotPod || c.statusUpdates != 1 || c.patchCalls != 2 {
				t.Fatalf("converged retry must not churn readiness/RV: updated=%v err=%v status=%d patches=%d", updated, err, c.statusUpdates, c.patchCalls)
			}
		})
	}
}

func Test_realControl_UpdatePodLifecycleWithMetadataReservedKeys(t *testing.T) {
	for _, usePatch := range []bool{false, true} {
		for _, key := range []string{appspub.LifecycleStateKey, appspub.LifecycleTimestampKey, appspub.InPlaceUpdatePlanKey} {
			t.Run(fmt.Sprintf("patch=%v/key=%s", usePatch, key), func(t *testing.T) {
				adp := &fakeAdapter{}
				patchAdp := &fakePatchAdapter{}
				readiness := &fakePodReadinessControl{}
				rc := &realControl{adp: adp, podReadinessControl: readiness}
				if usePatch {
					rc.adp = patchAdp
				}
				pod := lifecycleMetadataTestPod()
				pod.Annotations[appspub.InPlaceUpdatePlanKey] = "preserved-plan"
				before := pod.DeepCopy()
				handler := &appspub.LifecycleHook{LabelsHandler: map[string]string{key: "collision"}}
				updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(pod, appspub.LifecycleStatePreparingUpdate, true, handler, func(_ *corev1.Pod) {
					t.Fatal("invalid reserved hook key must fail before mutation")
				})
				if err == nil || updated || gotPod != nil || adp.updateCalled || patchAdp.patchCalled || readiness.addCalled || !reflect.DeepEqual(pod, before) {
					t.Fatalf("reserved hook key must fail without side effects: updated=%v pod=%v err=%v", updated, gotPod, err)
				}
			})
		}
	}

	for _, deleteKey := range []bool{false, true} {
		for _, key := range []string{appspub.LifecycleStateKey, appspub.LifecycleTimestampKey} {
			t.Run(fmt.Sprintf("mutate/delete=%v/key=%s", deleteKey, key), func(t *testing.T) {
				adp := &fakePatchAdapter{}
				rc := &realControl{adp: adp}
				pod := lifecycleMetadataTestPod()
				before := pod.DeepCopy()
				updated, gotPod, err := rc.UpdatePodLifecycleWithMetadata(pod, appspub.LifecycleStateUpdating, false, nil, func(clone *corev1.Pod) {
					metadata := clone.Annotations
					if key == appspub.LifecycleStateKey {
						metadata = clone.Labels
					}
					if deleteKey {
						delete(metadata, key)
					} else {
						metadata[key] = "collision"
					}
				})
				if err == nil || updated || gotPod != nil || adp.patchCalled || !reflect.DeepEqual(pod, before) {
					t.Fatalf("callback lifecycle collision must fail without metadata write: updated=%v pod=%v err=%v", updated, gotPod, err)
				}
			})
		}
	}
}

type lifecycleMetadataTrackingClient struct {
	client.Client
	patchCalls    int
	statusUpdates int
}

func (c *lifecycleMetadataTrackingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.patchCalls++
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *lifecycleMetadataTrackingClient) Status() client.SubResourceWriter {
	return &lifecycleMetadataTrackingStatus{SubResourceWriter: c.Client.Status(), owner: c}
}

type lifecycleMetadataTrackingStatus struct {
	client.SubResourceWriter
	owner *lifecycleMetadataTrackingClient
}

func (s *lifecycleMetadataTrackingStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.owner.statusUpdates++
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func lifecycleMetadataTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pod",
			Namespace:       "default",
			UID:             "pod-uid",
			ResourceVersion: "7",
			Labels: map[string]string{
				appspub.LifecycleStateKey: string(appspub.LifecycleStatePreparingUpdate),
				"example.com/unrelated":   "keep",
			},
			Annotations: map[string]string{
				appspub.LifecycleTimestampKey: "2024-01-02T03:04:05Z",
				"example.com/unrelated":       "keep",
				"example.com/obsolete":        "remove",
			},
			Finalizers:      []string{"example.com/unrelated", "example.com/existing"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "owner", UID: "owner-uid"}},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "image:old"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func silenceKlogForTest(t *testing.T) {
	originalStderr := os.Stderr
	nullFile, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("failed to open /dev/null: %v", err)
	}
	os.Stderr = nullFile

	t.Cleanup(func() {
		os.Stderr = originalStderr
	})
}
