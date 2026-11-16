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

package inplaceupdate

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	appspub "github.com/openkruise/kruise/apis/apps/pub"
	utilclient "github.com/openkruise/kruise/pkg/client"
	"github.com/openkruise/kruise/pkg/features"
	utilcontainerlaunchpriority "github.com/openkruise/kruise/pkg/util/containerlaunchpriority"
	utilcontainermeta "github.com/openkruise/kruise/pkg/util/containermeta"
	utilfeature "github.com/openkruise/kruise/pkg/util/feature"
	"github.com/openkruise/kruise/pkg/util/lifecycle"
	"github.com/openkruise/kruise/pkg/util/podadapter"
	"github.com/openkruise/kruise/pkg/util/revisionadapter"
)

func planTestJSON(t *testing.T, value interface{}) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func planTestPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "plan-pod", Namespace: "default", UID: "pod-uid", Labels: map[string]string{}, Annotations: map[string]string{}},
		Spec:       v1.PodSpec{Containers: []v1.Container{guaranteedInitTestContainer("main", false, "1", "1Gi")}},
	}
}

func preparePlanForSpec(t *testing.T, pod *v1.Pod, spec *UpdateSpec) *PreparedUpdate {
	t.Helper()
	prepared, err := PrepareUpdate(pod, &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}},
		&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "new"}}, &UpdateOptions{
			CalculateSpec: func(_, _ *apps.ControllerRevision, _ *UpdateOptions) *UpdateSpec { return spec },
		})
	if err != nil || prepared == nil {
		t.Fatalf("PrepareUpdate: prepared=%v err=%v", prepared, err)
	}
	return prepared
}

func enablePlanResourceGates(t *testing.T) {
	t.Helper()
	initEnabled := utilfeature.DefaultFeatureGate.Enabled(features.InPlaceUpdateRestartableInitContainer)
	verticalEnabled := utilfeature.DefaultFeatureGate.Enabled(features.InPlaceWorkloadVerticalScaling)
	setRestartableInitResourceGates(t, true, true)
	t.Cleanup(func() { setRestartableInitResourceGates(t, initEnabled, verticalEnabled) })
}

func TestPrepareUpdateDefaultPlan(t *testing.T) {
	enablePlanResourceGates(t)
	originalCapabilityCheck := shouldUpdateSidecarResourceByResize
	shouldUpdateSidecarResourceByResize = func() bool { return true }
	t.Cleanup(func() { shouldUpdateSidecarResourceByResize = originalCapabilityCheck })
	pod := planTestPod()
	sidecar := guaranteedInitTestContainer("sidecar", true, "100m", "128Mi")
	pod.Spec.InitContainers = []v1.Container{sidecar}
	newMain, newSidecar := *pod.Spec.Containers[0].DeepCopy(), *sidecar.DeepCopy()
	newMain.Image, newSidecar.Image = "main:new", "sidecar:new"
	newSidecar.Resources.Requests[v1.ResourceCPU] = resource.MustParse("200m")
	newSidecar.Resources.Limits[v1.ResourceCPU] = resource.MustParse("200m")
	oldRevision := resourceRevision(t, "old", pod.Spec.Containers[0], sidecar)
	newRevision := resourceRevision(t, "new", newMain, newSidecar)
	beforePod, beforeOld, beforeNew := planTestJSON(t, pod), planTestJSON(t, oldRevision), planTestJSON(t, newRevision)
	callbackCalls := 0
	callback := func(*v1.Pod) { callbackCalls++ }
	opts := &UpdateOptions{GracePeriodSeconds: 5, AdditionalFuncs: []func(*v1.Pod){callback}}
	prepared, err := PrepareUpdate(pod, oldRevision, newRevision, opts)
	if err != nil || prepared == nil {
		t.Fatalf("PrepareUpdate: prepared=%v err=%v", prepared, err)
	}
	want := []appspub.InPlaceUpdatePlanBatch{
		{Containers: []appspub.InPlaceUpdatePlanContainer{{Name: "sidecar", RestartRequired: true, Changes: []string{"Image", "Resources"}}}},
		{Containers: []appspub.InPlaceUpdatePlanContainer{{Name: "main", RestartRequired: true, Changes: []string{"Image"}}}},
	}
	if prepared.Plan.PlanID == "" || !reflect.DeepEqual(prepared.Plan.Batches, want) || prepared.Spec.GraceSeconds != 5 {
		t.Fatalf("unexpected preparation: plan=%s spec=%s", planTestJSON(t, prepared.Plan), planTestJSON(t, prepared.Spec))
	}
	if planTestJSON(t, pod) != beforePod || planTestJSON(t, oldRevision) != beforeOld || planTestJSON(t, newRevision) != beforeNew {
		t.Fatal("preparation mutated the Pod or revisions")
	}
	if callbackCalls != 0 || opts.CalculateSpec != nil || opts.PatchSpecToPod != nil || opts.GeneratePlan != nil ||
		opts.CheckPodUpdateCompleted != nil || opts.CheckContainersUpdateCompleted != nil || opts.CheckPodNeedsBeUnready != nil ||
		opts.GracePeriodSeconds != 5 || len(opts.AdditionalFuncs) != 1 || reflect.ValueOf(opts.AdditionalFuncs[0]).Pointer() != reflect.ValueOf(callback).Pointer() {
		t.Fatal("preparation mutated options or invoked an execution callback")
	}
	if err := prepared.validatePod(pod, false); err != nil {
		t.Fatalf("fresh preparation is invalid: %v", err)
	}
}

func TestPrepareUpdateCustomPlanContract(t *testing.T) {
	pod := planTestPod()
	spec := &UpdateSpec{Revision: "new", ContainerImages: map[string]string{"main": "main:new"}}
	beforePod, beforeSpec := planTestJSON(t, pod), planTestJSON(t, spec)
	patchCalls, callbackCalls := 0, 0
	opts := &UpdateOptions{
		CalculateSpec: func(_, _ *apps.ControllerRevision, copied *UpdateOptions) *UpdateSpec {
			copied.AdditionalFuncs[0] = func(*v1.Pod) { t.Fatal("modified callback invoked") }
			return spec
		},
		AdditionalFuncs: []func(*v1.Pod){func(*v1.Pod) { callbackCalls++ }},
		PatchSpecToPod: func(p *v1.Pod, _ *UpdateSpec, _ *appspub.InPlaceUpdateState) (*v1.Pod, map[string]*v1.ResourceRequirements, error) {
			patchCalls++
			return p, nil, nil
		},
	}
	if prepared, err := PrepareUpdate(pod, nil, nil, opts); err == nil || prepared != nil {
		t.Fatalf("custom patcher without planner accepted: prepared=%v err=%v", prepared, err)
	}
	opts.GeneratePlan = func(clone *v1.Pod, effective *UpdateSpec) (*appspub.InPlaceUpdatePlan, error) {
		clone.Spec.Containers[0].Image = "planner-copy-only"
		effective.ContainerImages["main"] = "planner-target"
		return generateDefaultUpdatePlan(clone, effective)
	}
	prepared, err := PrepareUpdate(pod, nil, nil, opts)
	if err != nil || prepared == nil || prepared.Plan.PlanID == "" {
		t.Fatalf("custom planner rejected: prepared=%v err=%v", prepared, err)
	}
	if prepared.Spec == spec || prepared.Spec.ContainerImages["main"] != "planner-target" {
		t.Fatal("custom planner did not receive an independently owned effective spec")
	}
	opts.AdditionalFuncs[0](pod)
	if patchCalls != 0 || callbackCalls != 1 || planTestJSON(t, pod) != beforePod || planTestJSON(t, spec) != beforeSpec {
		t.Fatal("preparation mutated caller inputs, callback slice, or executed the patcher")
	}
	plannerErr := errors.New("planner failed")
	opts.GeneratePlan = func(*v1.Pod, *UpdateSpec) (*appspub.InPlaceUpdatePlan, error) { return nil, plannerErr }
	if _, err := PrepareUpdate(pod, nil, nil, opts); !errors.Is(err, plannerErr) {
		t.Fatalf("planner error lost: %v", err)
	}
	opts.GeneratePlan = func(*v1.Pod, *UpdateSpec) (*appspub.InPlaceUpdatePlan, error) { return nil, nil }
	if _, err := PrepareUpdate(pod, nil, nil, opts); err == nil {
		t.Fatal("nil custom plan accepted")
	}
	if _, err := PrepareUpdate(nil, nil, nil, nil); err == nil {
		t.Fatal("nil Pod accepted")
	}
	if prepared, err := PrepareUpdate(pod, nil, nil, nil); err != nil || prepared != nil {
		t.Fatalf("ineligible update returned a plan: prepared=%v err=%v", prepared, err)
	}
}

func TestCopyUpdateSpecIndependent(t *testing.T) {
	pod := planTestPod()
	spec := &UpdateSpec{
		Revision: "new", ContainerImages: map[string]string{"main": "main:new"}, InitContainerImages: map[string]string{"sidecar": "sidecar:new"},
		ContainerResources:   map[string]v1.ResourceRequirements{"main": pod.Spec.Containers[0].Resources},
		ContainerRefMetadata: map[string]metav1.ObjectMeta{"main": {Labels: map[string]string{"shared": "new"}, Annotations: map[string]string{"config": "new"}}},
		MetaDataPatch:        []byte(`{"metadata":{"labels":{"other":"new"}}}`), UpdateEnvFromMetadata: true, GraceSeconds: 10,
		OldTemplate: &v1.PodTemplateSpec{Spec: pod.Spec}, NewTemplate: &v1.PodTemplateSpec{Spec: pod.Spec},
	}
	before := planTestJSON(t, spec)
	copied, err := copyUpdateSpec(spec)
	if err != nil || planTestJSON(t, copied) != before {
		t.Fatalf("copy differs: copied=%v err=%v", copied, err)
	}
	copied.ContainerImages["main"], copied.InitContainerImages["sidecar"] = "changed", "changed"
	copied.ContainerResources["main"].Requests[v1.ResourceCPU] = resource.MustParse("3")
	copied.ContainerRefMetadata["main"].Labels["shared"] = "changed"
	copied.ContainerRefMetadata["main"].Annotations["config"] = "changed"
	copied.MetaDataPatch[0] = '['
	copied.OldTemplate.Spec.Containers[0].Image, copied.NewTemplate.Spec.Containers[0].Image = "changed", "changed"
	if planTestJSON(t, spec) != before {
		t.Fatal("copy aliases the original update spec")
	}
}

func TestDefaultUpdatePlanMatchesExecutorBatches(t *testing.T) {
	enablePlanResourceGates(t)
	container := func(name string, priority *int) v1.Container {
		c := guaranteedInitTestContainer(name, false, "1", "1Gi")
		if priority != nil {
			c.Env = []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(*priority, "plan-pod")}
		}
		return c
	}
	high, middle, low, zero, negative := 100, 10, 1, 0, -1
	mixed := planTestPod()
	mixed.Spec.InitContainers = []v1.Container{
		{Name: "setup", Image: "setup:old"},
		guaranteedInitTestContainer("sidecar-b", true, "100m", "128Mi"),
		guaranteedInitTestContainer("sidecar-a", true, "100m", "128Mi"),
	}
	mixed.Spec.Containers = []v1.Container{container("high", &high), container("middle", &middle), container("low", &low), container("free", nil), container("last", &negative)}
	mixedSpec := &UpdateSpec{
		ContainerImages:     map[string]string{"high": "high:new", "middle": "middle:new", "low": "low:new", "free": "free:new", "last": "last:new"},
		InitContainerImages: map[string]string{"sidecar-b": "sidecar-b:new"},
		ContainerResources:  map[string]v1.ResourceRequirements{"sidecar-a": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m")}}},
		ContainerRefMetadata: map[string]metav1.ObjectMeta{
			"high":   {Labels: map[string]string{"shared": "new"}},
			"middle": {Labels: map[string]string{"shared": "new"}, Annotations: map[string]string{"config": "new"}},
			"low":    {Annotations: map[string]string{"config": "new"}},
		},
	}
	defaults := planTestPod()
	defaults.Spec.Containers = []v1.Container{container("negative", &negative), container("default", &zero), container("missing", nil), container("invalid", nil)}
	defaults.Spec.Containers[3].Env = []v1.EnvVar{{Name: appspub.ContainerLaunchBarrierEnvName, Value: "not-a-barrier"}}
	for _, tc := range []struct {
		name string
		pod  *v1.Pod
		spec *UpdateSpec
		want [][]string
	}{
		{name: "sidecars before priority batches and transitive metadata closure", pod: mixed, spec: mixedSpec, want: [][]string{{"sidecar-b"}, {"sidecar-a"}, {"free", "high", "low", "middle"}, {"last"}}},
		{name: "missing and invalid barriers join explicit default priority", pod: defaults,
			spec: &UpdateSpec{ContainerImages: map[string]string{"negative": "new", "default": "new", "missing": "new", "invalid": "new"}}, want: [][]string{{"default", "invalid", "missing"}, {"negative"}}},
		{name: "metadata only", pod: planTestPod(), spec: &UpdateSpec{MetaDataPatch: []byte(`{"metadata":{"labels":{"other":"new"}}}`)}, want: [][]string{{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforePod, beforeSpec := planTestJSON(t, tc.pod), planTestJSON(t, tc.spec)
			plan, err := generateDefaultUpdatePlan(tc.pod, tc.spec)
			if err != nil {
				t.Fatal(err)
			}
			wantBatches := []appspub.InPlaceUpdatePlanBatch{}
			for _, names := range tc.want {
				batch := appspub.InPlaceUpdatePlanBatch{Containers: []appspub.InPlaceUpdatePlanContainer{}}
				for _, name := range names {
					entry := appspub.InPlaceUpdatePlanContainer{Name: name, Changes: []string{}}
					if _, ok := tc.spec.ContainerImages[name]; ok {
						entry.Changes, entry.RestartRequired = append(entry.Changes, "Image"), true
					}
					if _, ok := tc.spec.InitContainerImages[name]; ok {
						entry.Changes, entry.RestartRequired = append(entry.Changes, "Image"), true
					}
					if _, ok := tc.spec.ContainerResources[name]; ok {
						entry.Changes = append(entry.Changes, "Resources")
					}
					if _, ok := tc.spec.ContainerRefMetadata[name]; ok {
						entry.Changes, entry.RestartRequired = append(entry.Changes, "MetadataEnv"), true
					}
					batch.Containers = append(batch.Containers, entry)
				}
				wantBatches = append(wantBatches, batch)
			}
			if !reflect.DeepEqual(plan.Batches, wantBatches) {
				t.Fatalf("unexpected exact plan: got=%s want=%s", planTestJSON(t, plan.Batches), planTestJSON(t, wantBatches))
			}
			pending, err := copyUpdateSpec(tc.spec)
			if err != nil {
				t.Fatal(err)
			}
			working, state := tc.pod.DeepCopy(), &appspub.InPlaceUpdateState{}
			for batchIndex, wantNames := range tc.want {
				selected, initSelected := selectNextUpdateBatch(working, pending)
				if !reflect.DeepEqual(selected.List(), wantNames) {
					t.Fatalf("batch %d selector=%v want=%v", batchIndex, selected.List(), wantNames)
				}
				if tc.pod == mixed && batchIndex < 2 && !initSelected.Equal(selected) || (tc.pod != mixed || batchIndex >= 2) && initSelected.Len() != 0 {
					t.Fatalf("batch %d incorrect sidecar selection: %v", batchIndex, initSelected)
				}
				working, _, err = defaultPatchUpdateSpecToPod(working, pending, state)
				if err != nil {
					t.Fatal(err)
				}
				gotNames := state.ContainerBatchesRecord[len(state.ContainerBatchesRecord)-1].Containers
				if !reflect.DeepEqual(gotNames, wantNames) {
					t.Fatalf("batch %d executor=%v plan=%v", batchIndex, gotNames, wantNames)
				}
				pending = &UpdateSpec{ContainerImages: state.NextContainerImages, InitContainerImages: state.NextInitContainerImages,
					ContainerResources: state.NextContainerResources, ContainerRefMetadata: state.NextContainerRefMetadata}
			}
			if len(pending.ContainerImages)+len(pending.InitContainerImages)+len(pending.ContainerResources)+len(pending.ContainerRefMetadata) != 0 {
				t.Fatalf("executor left pending targets: %s", planTestJSON(t, pending))
			}
			if planTestJSON(t, tc.pod) != beforePod || planTestJSON(t, tc.spec) != beforeSpec {
				t.Fatal("planning or execution on a clone mutated caller inputs")
			}
		})
	}
}

func TestDefaultUpdatePlanRestartChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy []v1.ContainerResizePolicy
		target v1.ResourceRequirements
		want   bool
	}{
		{name: "CPU changed with CPU restart policy", policy: []v1.ContainerResizePolicy{{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}}, target: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}, want: true},
		{name: "CPU limit changed", policy: []v1.ContainerResizePolicy{{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}}, target: v1.ResourceRequirements{Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}, want: true},
		{name: "CPU changed with memory-only restart policy", policy: []v1.ContainerResizePolicy{{ResourceName: v1.ResourceMemory, RestartPolicy: v1.RestartContainer}}, target: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}},
		{name: "memory changed with matching policy", policy: []v1.ContainerResizePolicy{{ResourceName: v1.ResourceMemory, RestartPolicy: v1.RestartContainer}}, target: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")}}, want: true},
		{name: "equivalent CPU quantities", policy: []v1.ContainerResizePolicy{{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}}, target: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1000m")}, Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1000m")}}},
		{name: "NotRequired policy", policy: []v1.ContainerResizePolicy{{ResourceName: v1.ResourceCPU, RestartPolicy: v1.NotRequired}}, target: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}},
		{name: "missing policy", target: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := planTestPod()
			pod.Spec.Containers[0].ResizePolicy = tc.policy
			spec := &UpdateSpec{ContainerResources: map[string]v1.ResourceRequirements{"main": tc.target}}
			beforePod, beforeSpec := planTestJSON(t, pod), planTestJSON(t, spec)
			if got := resourceUpdateRequiresRestart(&pod.Spec.Containers[0], tc.target); got != tc.want {
				t.Fatalf("resource restart=%v want=%v", got, tc.want)
			}
			plan, err := generateDefaultUpdatePlan(pod, spec)
			if err != nil {
				t.Fatal(err)
			}
			entry := plan.Batches[0].Containers[0]
			if entry.RestartRequired != tc.want || !reflect.DeepEqual(entry.Changes, []string{"Resources"}) {
				t.Fatalf("unexpected resource plan: %+v", entry)
			}
			if !tc.want && !strings.Contains(planTestJSON(t, plan), `"restartRequired":false`) {
				t.Fatal("false restartRequired omitted from JSON")
			}
			if planTestJSON(t, pod) != beforePod || planTestJSON(t, spec) != beforeSpec {
				t.Fatal("restart calculation mutated caller inputs")
			}
		})
	}
	for _, tc := range []struct {
		name string
		spec *UpdateSpec
		want appspub.InPlaceUpdatePlanContainer
	}{
		{name: "image and resources are separate changes", spec: &UpdateSpec{ContainerImages: map[string]string{"main": "main:new"}, ContainerResources: map[string]v1.ResourceRequirements{"main": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}}, want: appspub.InPlaceUpdatePlanContainer{Name: "main", RestartRequired: true, Changes: []string{"Image", "Resources"}}},
		{name: "image restart intent survives partially applied target", spec: &UpdateSpec{ContainerImages: map[string]string{"main": "main:latest"}}, want: appspub.InPlaceUpdatePlanContainer{Name: "main", RestartRequired: true, Changes: []string{"Image"}}},
		{name: "metadata-derived env changes", spec: &UpdateSpec{ContainerRefMetadata: map[string]metav1.ObjectMeta{"main": {Labels: map[string]string{"env": "new"}}}}, want: appspub.InPlaceUpdatePlanContainer{Name: "main", RestartRequired: true, Changes: []string{"MetadataEnv"}}},
		{name: "metadata restart intent survives partially applied target", spec: &UpdateSpec{ContainerRefMetadata: map[string]metav1.ObjectMeta{"main": {Labels: map[string]string{"env": "old"}, Annotations: map[string]string{"config": "old"}}}}, want: appspub.InPlaceUpdatePlanContainer{Name: "main", RestartRequired: true, Changes: []string{"MetadataEnv"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := planTestPod()
			pod.Labels["env"], pod.Annotations["config"] = "old", "old"
			plan, err := generateDefaultUpdatePlan(pod, tc.spec)
			if err != nil || plan == nil {
				t.Fatalf("plan=%v err=%v", plan, err)
			}
			if !reflect.DeepEqual(plan.Batches[0].Containers[0], tc.want) {
				t.Fatalf("entry=%+v want=%+v", plan.Batches[0].Containers[0], tc.want)
			}
		})
	}
}

func TestDefaultUpdatePlanRejectsUnknownTargets(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *UpdateSpec
	}{
		{name: "image", spec: &UpdateSpec{ContainerImages: map[string]string{"unknown": "new"}}},
		{name: "init image", spec: &UpdateSpec{InitContainerImages: map[string]string{"unknown": "new"}}},
		{name: "resources", spec: &UpdateSpec{ContainerResources: map[string]v1.ResourceRequirements{"unknown": {}}}},
		{name: "metadata", spec: &UpdateSpec{ContainerRefMetadata: map[string]metav1.ObjectMeta{"unknown": {}}}},
		{name: "non-restartable init", spec: &UpdateSpec{InitContainerImages: map[string]string{"setup": "new"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := planTestPod()
			pod.Spec.InitContainers = []v1.Container{{Name: "setup", Image: "old"}}
			before := planTestJSON(t, tc.spec)
			if plan, err := generateDefaultUpdatePlan(pod, tc.spec); err == nil || plan != nil {
				t.Fatalf("invalid target accepted: plan=%v err=%v", plan, err)
			}
			if planTestJSON(t, tc.spec) != before {
				t.Fatal("failed planner mutated spec")
			}
		})
	}
}

func TestPreparedUpdatePlanIdentity(t *testing.T) {
	pod := planTestPod()
	pod.Spec.Containers[0].Env = []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(10, pod.Name)}
	pod.Spec.InitContainers = []v1.Container{guaranteedInitTestContainer("sidecar", true, "100m", "128Mi")}
	spec := &UpdateSpec{Revision: "new", ContainerImages: map[string]string{"main": "main:new"},
		ContainerResources: map[string]v1.ResourceRequirements{"main": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}}
	prepared := preparePlanForSpec(t, pod, spec)
	for _, tc := range []struct {
		name       string
		mutatePod  func(*v1.Pod)
		mutateSpec func(*UpdateSpec)
		stable     bool
	}{
		{name: "resourceVersion", mutatePod: func(p *v1.Pod) { p.ResourceVersion = "999" }, stable: true},
		{name: "status", mutatePod: func(p *v1.Pod) {
			p.Status.Phase = v1.PodRunning
			p.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "main", Ready: true, RestartCount: 2}}
		}, stable: true},
		{name: "lifecycle bookkeeping", mutatePod: func(p *v1.Pod) {
			lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(p)
			p.Annotations[appspub.InPlaceUpdatePlanKey] = prepared.planJSON
			p.Labels["hook"] = "released"
			p.Finalizers = []string{"unrelated"}
		}, stable: true},
		{name: "UID", mutatePod: func(p *v1.Pod) { p.UID = "replacement" }},
		{name: "sidecar restart policy", mutatePod: func(p *v1.Pod) { p.Spec.InitContainers[0].RestartPolicy = nil }},
		{name: "input image", mutatePod: func(p *v1.Pod) { p.Spec.Containers[0].Image = "unexpected" }},
		{name: "input resources", mutatePod: func(p *v1.Pod) { p.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("3") }},
		{name: "resize policy", mutatePod: func(p *v1.Pod) {
			p.Spec.Containers[0].ResizePolicy = []v1.ContainerResizePolicy{{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}}
		}},
		{name: "priority", mutatePod: func(p *v1.Pod) {
			p.Spec.Containers[0].Env[0] = utilcontainerlaunchpriority.GeneratePriorityEnv(20, p.Name)
		}},
		{name: "container topology", mutatePod: func(p *v1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, v1.Container{Name: "extra", Image: "extra:old"})
		}},
		{name: "target image", mutateSpec: func(s *UpdateSpec) { s.ContainerImages["main"] = "main:newer" }},
		{name: "target resources", mutateSpec: func(s *UpdateSpec) { s.ContainerResources["main"].Requests[v1.ResourceCPU] = resource.MustParse("3") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changedPod := pod.DeepCopy()
			changedSpec, err := copyUpdateSpec(spec)
			if err != nil {
				t.Fatal(err)
			}
			if tc.mutatePod != nil {
				tc.mutatePod(changedPod)
			}
			if tc.mutateSpec != nil {
				tc.mutateSpec(changedSpec)
			}
			changed := preparePlanForSpec(t, changedPod, changedSpec)
			if (changed.Plan.PlanID == prepared.Plan.PlanID) != tc.stable {
				t.Fatalf("plan identity stable=%v want=%v", changed.Plan.PlanID == prepared.Plan.PlanID, tc.stable)
			}
			if tc.mutatePod != nil {
				before := planTestJSON(t, changedPod)
				if err := prepared.validatePod(changedPod, false); (err == nil) != tc.stable {
					t.Fatalf("validation err=%v stable=%v", err, tc.stable)
				}
				if planTestJSON(t, changedPod) != before {
					t.Fatal("validation mutated the Pod")
				}
			}
		})
	}
}

func TestPreparedUpdateValidateAppliedResources(t *testing.T) {
	pod := planTestPod()
	pod.Spec.Containers[0].ResizePolicy = []v1.ContainerResizePolicy{{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}}
	pod.Spec.InitContainers = []v1.Container{guaranteedInitTestContainer("sidecar", true, "100m", "128Mi")}
	spec := &UpdateSpec{Revision: "new", ContainerResources: map[string]v1.ResourceRequirements{
		"main":    {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}, Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}},
		"sidecar": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m")}},
	}}
	prepared := preparePlanForSpec(t, pod, spec)
	for _, tc := range []struct {
		name    string
		mutate  func(*v1.Pod)
		allow   bool
		wantErr bool
	}{
		{name: "original Pod", mutate: func(*v1.Pod) {}},
		{name: "already-applied regular target rejected by initial validation", mutate: func(p *v1.Pod) { p.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2") }, wantErr: true},
		{name: "already-applied requests and limits allowed on retry", mutate: func(p *v1.Pod) {
			p.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2000m")
			p.Spec.Containers[0].Resources.Limits[v1.ResourceCPU] = resource.MustParse("2")
		}, allow: true},
		{name: "already-applied sidecar target allowed on retry", mutate: func(p *v1.Pod) {
			p.Spec.InitContainers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("200m")
		}, allow: true},
		{name: "non-target resource value rejected on retry", mutate: func(p *v1.Pod) { p.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("3") }, allow: true, wantErr: true},
		{name: "unrelated resource change rejected on retry", mutate: func(p *v1.Pod) {
			p.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2")
			p.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("2Gi")
		}, allow: true, wantErr: true},
		{name: "stale UID rejected", mutate: func(p *v1.Pod) { p.UID = "replacement" }, allow: true, wantErr: true},
		{name: "stale plan rejected", mutate: func(p *v1.Pod) { p.Annotations[appspub.InPlaceUpdatePlanKey] = `{"planID":"other","batches":[]}` }, allow: true, wantErr: true},
		{name: "stale resize policy rejected", mutate: func(p *v1.Pod) { p.Spec.Containers[0].ResizePolicy[0].RestartPolicy = v1.NotRequired }, allow: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := pod.DeepCopy()
			tc.mutate(changed)
			beforePod, beforeSpec := planTestJSON(t, changed), planTestJSON(t, prepared.Spec)
			if err := prepared.validatePod(changed, tc.allow); (err != nil) != tc.wantErr {
				t.Fatalf("validation err=%v wantErr=%v", err, tc.wantErr)
			}
			if planTestJSON(t, changed) != beforePod || planTestJSON(t, prepared.Spec) != beforeSpec {
				t.Fatal("retry validation mutated the Pod or prepared spec")
			}
		})
	}
}

type planMetadataRecordingClient struct {
	client.Client
	patches [][]byte
}

func (c *planMetadataRecordingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	c.patches = append(c.patches, append([]byte(nil), data...))
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestSyncUpdatePlanLifecycle(t *testing.T) {
	pod := planTestPod()
	pod.Spec.ReadinessGates = []v1.PodReadinessGate{{ConditionType: appspub.KruisePodReadyConditionType}}
	pod.Status.Conditions = []v1.PodCondition{{Type: appspub.KruisePodReadyConditionType, Status: v1.ConditionTrue}, {Type: v1.PodReady, Status: v1.ConditionTrue}}
	lifecycle.SetPodLifecycle(appspub.LifecycleStateNormal)(pod)
	hook := &appspub.LifecycleHook{LabelsHandler: map[string]string{"example.com/hook": "held"}, FinalizersHandler: []string{"example.com/hook"}, MarkPodNotReady: true}
	pod.Labels["example.com/hook"], pod.Finalizers = "held", []string{"example.com/unrelated", "example.com/hook"}
	spec := &UpdateSpec{Revision: "new", ContainerImages: map[string]string{"main": "main:new"}}
	prepared := preparePlanForSpec(t, pod, spec)
	cli := &planMetadataRecordingClient{Client: fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()}
	control := lifecycle.New(cli)
	get := func() *v1.Pod {
		t.Helper()
		current := &v1.Pod{}
		if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
			t.Fatal(err)
		}
		return current
	}
	pod = get()
	before := planTestJSON(t, pod)
	updated, _, err := SyncUpdatePlan(pod, prepared, hook, control)
	if apierrors.IsConflict(err) {
		if planTestJSON(t, pod) != before || get().Annotations[appspub.InPlaceUpdatePlanKey] != "" || lifecycle.GetPodLifecycleState(get()) != appspub.LifecycleStateNormal {
			t.Fatal("readiness conflict mutated input or partially published lifecycle metadata")
		}
		pod = get()
		fresh := preparePlanForSpec(t, pod, spec)
		if fresh.Plan.PlanID != prepared.Plan.PlanID {
			t.Fatal("readiness conflict changed the plan identity")
		}
		prepared, cli.patches = fresh, nil
		before = planTestJSON(t, pod)
		updated, _, err = SyncUpdatePlan(pod, prepared, hook, control)
	}
	if err != nil || !updated || len(cli.patches) != 1 || planTestJSON(t, pod) != before {
		t.Fatalf("initial sync: updated=%v err=%v patches=%d inputChanged=%v", updated, err, len(cli.patches), planTestJSON(t, pod) != before)
	}
	var payload struct {
		Metadata struct {
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(cli.patches[0], &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Metadata.Labels[appspub.LifecycleStateKey] != string(appspub.LifecycleStatePreparingUpdate) || payload.Metadata.Annotations[appspub.InPlaceUpdatePlanKey] != prepared.planJSON {
		t.Fatalf("plan and PreparingUpdate were not published atomically: %s", cli.patches[0])
	}
	current := get()
	if current.Annotations[appspub.InPlaceUpdatePlanKey] != prepared.planJSON || lifecycle.GetPodLifecycleState(current) != appspub.LifecycleStatePreparingUpdate {
		t.Fatal("initial plan missing from stored Pod")
	}
	markedNotReady := false
	for _, condition := range current.Status.Conditions {
		if condition.Type == appspub.KruisePodReadyConditionType && condition.Status == v1.ConditionFalse && strings.Contains(condition.Message, "preUpdateHook") {
			markedNotReady = true
		}
	}
	if !markedNotReady {
		t.Fatal("PreparingUpdate did not mark the lifecycle readiness hook")
	}
	delete(current.Labels, "example.com/hook")
	current.Finalizers = []string{"example.com/unrelated"}
	if err := cli.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	current = get()
	patches := len(cli.patches)
	if updated, _, err := SyncUpdatePlan(current, prepared, hook, control); err != nil || updated || len(cli.patches) != patches {
		t.Fatalf("same plan rearmed a released hook: updated=%v err=%v", updated, err)
	}
	if lifecycle.IsPodHooked(hook, get()) {
		t.Fatal("same-plan reconcile restored hook handlers")
	}
	changedSpec, err := copyUpdateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	changedSpec.ContainerImages["main"] = "main:newer"
	changed := preparePlanForSpec(t, current, changedSpec)
	if updated, _, err := SyncUpdatePlan(current, changed, hook, control); err != nil || !updated {
		t.Fatalf("changed plan was not synced: updated=%v err=%v", updated, err)
	}
	current = get()
	if !lifecycle.IsPodHooked(hook, current) || current.Annotations[appspub.InPlaceUpdatePlanKey] != changed.planJSON || !reflect.DeepEqual(current.Finalizers, []string{"example.com/unrelated", "example.com/hook"}) {
		t.Fatal("changed target did not rearm hook while retaining unrelated finalizers")
	}
	delete(current.Annotations, appspub.InPlaceUpdatePlanKey)
	if err := cli.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	current = get()
	if updated, _, err := SyncUpdatePlan(current, changed, hook, control); err != nil || !updated {
		t.Fatalf("missing plan not repaired: updated=%v err=%v", updated, err)
	}
	current = get()
	if current.Annotations[appspub.InPlaceUpdatePlanKey] != changed.planJSON {
		t.Fatal("repaired Pod still has no plan")
	}
	state, labels, finalizers := lifecycle.GetPodLifecycleState(current), planTestJSON(t, current.Labels), planTestJSON(t, current.Finalizers)
	if updated, _, err := ClearUpdatePlan(current, control); err != nil || !updated {
		t.Fatalf("clear plan failed: updated=%v err=%v", updated, err)
	}
	current = get()
	if _, exists := current.Annotations[appspub.InPlaceUpdatePlanKey]; exists || lifecycle.GetPodLifecycleState(current) != state || planTestJSON(t, current.Labels) != labels || planTestJSON(t, current.Finalizers) != finalizers {
		t.Fatal("clear plan changed unrelated lifecycle metadata or left the annotation")
	}
	if updated, _, err := ClearUpdatePlan(current, control); err != nil || updated {
		t.Fatalf("clear absent plan was not a no-op: updated=%v err=%v", updated, err)
	}
	lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)(current)
	current.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, appspub.InPlaceUpdateState{Revision: changed.Spec.Revision})
	if updated, _, err := SyncUpdatePlan(current, changed, hook, control); err != nil || updated {
		t.Fatalf("execution changed the approved plan: updated=%v err=%v", updated, err)
	}
	if updated, _, err := SyncUpdatePlan(current, nil, hook, control); err != nil || updated {
		t.Fatalf("nil preparation was not a no-op: updated=%v err=%v", updated, err)
	}
	if updated, _, err := ClearUpdatePlan(nil, control); err != nil || updated {
		t.Fatalf("clear nil Pod was not a no-op: updated=%v err=%v", updated, err)
	}
}

func TestPreparedUpdateWholeCyclePlanIsImmutable(t *testing.T) {
	enablePlanResourceGates(t)
	version := utilclient.GetCurrentServerVersion()
	originalVersion := *version
	version.GitVersion = "v1.35.2"
	t.Cleanup(func() { *version = originalVersion })
	for _, grace := range []int32{0, 60} {
		name := "immediate"
		if grace > 0 {
			name = "grace"
		}
		t.Run(name, func(t *testing.T) {
			pod := planTestPod()
			pod.Spec.InitContainers = []v1.Container{guaranteedInitTestContainer("sidecar", true, "100m", "128Mi")}
			pod.Spec.Containers[0].Env = []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(10, pod.Name)}
			low := guaranteedInitTestContainer("low", false, "1", "1Gi")
			low.Env = []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(0, pod.Name)}
			pod.Spec.Containers = append(pod.Spec.Containers, low)
			started := true
			pod.Status.InitContainerStatuses = []v1.ContainerStatus{{Name: "sidecar", Started: &started}}
			pod.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "main", Ready: true}, {Name: "low", Ready: true}}
			spec := &UpdateSpec{Revision: "new", GraceSeconds: grace,
				InitContainerImages: map[string]string{"sidecar": "sidecar:new"},
				ContainerImages:     map[string]string{"main": "main:new", "low": "low:new"},
				ContainerResources: map[string]v1.ResourceRequirements{
					"sidecar": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("200m")}},
					"main":    {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}},
				}}
			prepared := preparePlanForSpec(t, pod, spec)
			pod.Annotations[appspub.InPlaceUpdatePlanKey] = prepared.planJSON
			lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(pod)
			cli := fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()
			adapter := &planDetachedResizeAdapter{resourceBatchTestAdapter: &resourceBatchTestAdapter{AdapterRuntimeClient: &podadapter.AdapterRuntimeClient{Client: cli}}}
			ctrl := New(cli, revisionadapter.NewDefaultImpl()).(*realControl)
			ctrl.podAdapter = adapter
			opts := &UpdateOptions{PreparedUpdate: prepared, GracePeriodSeconds: grace,
				AdditionalFuncs:                []func(*v1.Pod){lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)},
				CheckPodNeedsBeUnready:         func(*v1.Pod, *UpdateSpec) bool { return false },
				CheckContainersUpdateCompleted: func(*v1.Pod, *appspub.InPlaceUpdateState) error { return nil },
			}
			get := func() *v1.Pod {
				t.Helper()
				current := &v1.Pod{}
				if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
					t.Fatal(err)
				}
				if current.Annotations[appspub.InPlaceUpdatePlanKey] != prepared.planJSON {
					t.Fatal("execution changed or removed the full approved plan")
				}
				return current
			}
			stateOf := func(current *v1.Pod) appspub.InPlaceUpdateState {
				t.Helper()
				state := appspub.InPlaceUpdateState{}
				if err := json.Unmarshal([]byte(current.Annotations[appspub.InPlaceUpdateStateKey]), &state); err != nil {
					t.Fatal(err)
				}
				return state
			}
			assertFrozen := func(current *v1.Pod) {
				t.Helper()
				lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(current)
				if err := cli.Update(context.Background(), current); err != nil {
					t.Fatal(err)
				}
				current = get()
				before, resizes := planTestJSON(t, current), adapter.resizePatches
				if result := ctrl.Refresh(current, opts); result.RefreshErr != nil || result.DelayDuration != 0 {
					t.Fatalf("PreparingUpdate Refresh was not frozen: %+v", result)
				}
				if updated, err := ctrl.updateNextBatch(current, opts); err != nil || updated {
					t.Fatalf("PreparingUpdate next batch was not frozen: updated=%v err=%v", updated, err)
				}
				if delay, err := ctrl.finishGracePeriod(current, opts); err != nil || delay != 0 {
					t.Fatalf("PreparingUpdate grace was not frozen: delay=%v err=%v", delay, err)
				}
				if planTestJSON(t, get()) != before || adapter.resizePatches != resizes {
					t.Fatal("PreparingUpdate changed pending work or performed a resize")
				}
				lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)(current)
				if err := cli.Update(context.Background(), current); err != nil {
					t.Fatal(err)
				}
			}
			assertFrozen(get())
			current := get()
			delete(current.Annotations, appspub.InPlaceUpdatePlanKey)
			if err := cli.Update(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			result := ctrl.Update(current, &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}},
				&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "new"}}, opts)
			if result.UpdateErr != nil || !result.InPlaceUpdate {
				t.Fatalf("prepared execution failed: %+v", result)
			}
			current = get()
			if grace > 0 {
				state := stateOf(current)
				state.UpdateTimestamp = metav1.NewTime(time.Now().Add(time.Hour))
				current.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, state)
				if err := cli.Update(context.Background(), current); err != nil {
					t.Fatal(err)
				}
				if refresh := ctrl.Refresh(get(), opts); refresh.RefreshErr != nil || refresh.DelayDuration <= 0 {
					t.Fatalf("grace hold failed: %+v", refresh)
				}
				if len(stateOf(get()).ContainerBatchesRecord) != 0 {
					t.Fatal("grace hold executed a batch")
				}
				current = get()
				state = stateOf(current)
				state.UpdateTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
				current.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, state)
				if err := cli.Update(context.Background(), current); err != nil {
					t.Fatal(err)
				}
				assertFrozen(get())
				if refresh := ctrl.Refresh(get(), opts); refresh.RefreshErr != nil {
					t.Fatalf("expired grace failed: %+v", refresh)
				}
				if get().Annotations[appspub.InPlaceUpdateGraceKey] != "" {
					t.Fatal("expired grace annotation was not removed")
				}
			}
			wantBatches := [][]string{{"sidecar"}, {"main"}, {"low"}}
			for i, names := range wantBatches {
				current = get()
				state := stateOf(current)
				if len(state.ContainerBatchesRecord) != i+1 || !reflect.DeepEqual(state.ContainerBatchesRecord[i].Containers, names) {
					t.Fatalf("batch %d progress=%s want=%v", i, planTestJSON(t, state.ContainerBatchesRecord), names)
				}
				if i+1 < len(wantBatches) {
					assertFrozen(current)
					if refresh := ctrl.Refresh(get(), opts); refresh.RefreshErr != nil {
						t.Fatalf("next batch failed: %+v", refresh)
					}
				}
			}
			current = get()
			if current.Spec.InitContainers[0].Image != "sidecar:new" || current.Spec.Containers[0].Image != "main:new" || current.Spec.Containers[1].Image != "low:new" {
				t.Fatal("whole-cycle execution did not apply all target images")
			}
			if !current.Spec.InitContainers[0].Resources.Requests[v1.ResourceCPU].Equal(resource.MustParse("200m")) || !current.Spec.Containers[0].Resources.Requests[v1.ResourceCPU].Equal(resource.MustParse("2")) {
				t.Fatal("whole-cycle execution did not apply all target resources")
			}
			if refresh := ctrl.Refresh(current, opts); refresh.RefreshErr != nil || len(stateOf(get()).ContainerBatchesRecord) != len(wantBatches) {
				t.Fatalf("completion changed progress or failed: %+v", refresh)
			}
			if adapter.resizePatches != 2 {
				t.Fatalf("expected exactly two resource resize patches, got %d", adapter.resizePatches)
			}
		})
	}
}

func TestSyncUpdatePlanWithoutHook(t *testing.T) {
	for _, state := range []appspub.LifecycleStateType{appspub.LifecycleStateNormal, appspub.LifecycleStateUpdated} {
		t.Run(string(state), func(t *testing.T) {
			pod := planTestPod()
			lifecycle.SetPodLifecycle(state)(pod)
			pod.Annotations[appspub.InPlaceUpdatePlanKey] = `{"planID":"completed","batches":[]}`
			spec := &UpdateSpec{Revision: "new", ContainerImages: map[string]string{"main": "main:new"}}
			prepared := preparePlanForSpec(t, pod, spec)
			if err := prepared.validatePod(pod, false); err == nil {
				t.Fatal("direct preparation accepted a prior plan without lifecycle authorization")
			}
			cli := &planMetadataRecordingClient{Client: fake.NewClientBuilder().WithObjects(pod).Build()}
			control := lifecycle.New(cli)
			before := planTestJSON(t, pod)
			updated, _, err := SyncUpdatePlan(pod, prepared, nil, control)
			if err != nil || updated != (state == appspub.LifecycleStateUpdated) || planTestJSON(t, pod) != before {
				t.Fatalf("no-hook prior-plan sync: updated=%v err=%v", updated, err)
			}
			current := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
				t.Fatal(err)
			}
			if err := prepared.validatePod(current, false); err != nil {
				t.Fatalf("authorized no-hook preparation rejected: %v", err)
			}
			fresh := preparePlanForSpec(t, current, spec)
			patches := len(cli.patches)
			if updated, _, err := SyncUpdatePlan(current, fresh, nil, control); err != nil || updated || len(cli.patches) != patches {
				t.Fatalf("repeated no-hook plan churned metadata: updated=%v err=%v", updated, err)
			}
			ctrl := New(cli, revisionadapter.NewDefaultImpl())
			result := ctrl.Update(current, &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}},
				&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "new"}}, &UpdateOptions{PreparedUpdate: fresh,
					AdditionalFuncs:        []func(*v1.Pod){lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)},
					CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false },
				})
			if result.UpdateErr != nil || !result.InPlaceUpdate {
				t.Fatalf("no-hook execution failed: %+v", result)
			}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
				t.Fatal(err)
			}
			if current.Annotations[appspub.InPlaceUpdatePlanKey] != fresh.planJSON || current.Spec.Containers[0].Image != "main:new" {
				t.Fatal("no-hook execution did not replace the completed plan and apply its target")
			}
			patches = len(cli.patches)
			if updated, _, err := SyncUpdatePlan(current, fresh, nil, control); err != nil || updated || len(cli.patches) != patches {
				t.Fatalf("same no-hook executing revision churned its plan: updated=%v err=%v", updated, err)
			}
		})
	}
}

func TestPreparedUpdateApprovalGuards(t *testing.T) {
	pod := planTestPod()
	lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(pod)
	spec := &UpdateSpec{Revision: "new", ContainerImages: map[string]string{"main": "main:new"}}
	prepared := preparePlanForSpec(t, pod, spec)
	pod.Annotations[appspub.InPlaceUpdatePlanKey] = prepared.planJSON
	hook := &appspub.LifecycleHook{LabelsHandler: map[string]string{"example.com/hook": "held"}, FinalizersHandler: []string{"example.com/hook"}}
	if updated, _, err := SyncUpdatePlan(pod, prepared, hook, nil); err != nil || updated {
		t.Fatalf("approved plan should bind guards without a write: updated=%v err=%v", updated, err)
	}
	if !prepared.requirePlan || prepared.hook != hook || prepared.validatePod(pod, false) != nil {
		t.Fatal("released PreparingUpdate plan was not bound to its approval guards")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*v1.Pod)
	}{
		{name: "approved annotation disappeared", mutate: func(p *v1.Pod) { delete(p.Annotations, appspub.InPlaceUpdatePlanKey) }},
		{name: "approved annotation replaced", mutate: func(p *v1.Pod) { p.Annotations[appspub.InPlaceUpdatePlanKey] = "another-plan" }},
		{name: "hook label restored", mutate: func(p *v1.Pod) { p.Labels["example.com/hook"] = "held" }},
		{name: "hook finalizer restored", mutate: func(p *v1.Pod) { p.Finalizers = []string{"example.com/hook"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			latest := pod.DeepCopy()
			tc.mutate(latest)
			before := planTestJSON(t, latest)
			if err := prepared.validatePod(latest, true); err == nil || planTestJSON(t, latest) != before {
				t.Fatalf("invalid latest approval accepted or mutated: err=%v", err)
			}
			cli := fake.NewClientBuilder().WithObjects(latest).Build()
			result := New(cli, revisionadapter.NewDefaultImpl()).Update(pod,
				&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}},
				&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "new"}}, &UpdateOptions{PreparedUpdate: prepared,
					CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false },
				})
			if result.UpdateErr == nil {
				t.Fatal("executor accepted approval revoked after the informer snapshot")
			}
			stored := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatal(err)
			}
			if stored.Spec.Containers[0].Image != pod.Spec.Containers[0].Image || stored.Annotations[appspub.InPlaceUpdateStateKey] != "" {
				t.Fatal("revoked approval applied an update")
			}
		})
	}
	for _, state := range []appspub.LifecycleStateType{appspub.LifecycleStatePreparingDelete, "unrecognized"} {
		invalid := pod.DeepCopy()
		lifecycle.SetPodLifecycle(state)(invalid)
		before := planTestJSON(t, invalid)
		if updated, _, err := SyncUpdatePlan(invalid, preparePlanForSpec(t, invalid, spec), hook, nil); err == nil || updated || planTestJSON(t, invalid) != before {
			t.Fatalf("invalid lifecycle state accepted or mutated: state=%s updated=%v err=%v", state, updated, err)
		}
	}
}

type planResourceRetryAdapter struct {
	*resourceBatchTestAdapter
	updateCalls    int
	afterResize    func(*v1.Pod) error
	beforeResize   func(*v1.Pod)
	resizeConflict bool
}

func (a *planResourceRetryAdapter) PatchPodResource(pod *v1.Pod, patch client.Patch) (*v1.Pod, error) {
	data, err := patch.Data(pod)
	if err != nil {
		return nil, err
	}
	var body struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	if body.Metadata.UID != pod.UID || body.Metadata.ResourceVersion == "" || body.Metadata.ResourceVersion != pod.ResourceVersion {
		return nil, errors.New("planned resize is missing UID/resourceVersion snapshot preconditions")
	}
	if a.beforeResize != nil {
		latest, err := a.GetPod(pod.Namespace, pod.Name)
		if err != nil {
			return nil, err
		}
		a.beforeResize(latest)
		if _, err := a.AdapterRuntimeClient.UpdatePod(latest); err != nil {
			return nil, err
		}
	}
	patched, err := a.resourceBatchTestAdapter.PatchPodResource(pod, patch)
	a.resizeConflict = apierrors.IsConflict(err)
	return patched, err
}

func (a *planResourceRetryAdapter) UpdatePod(pod *v1.Pod) (*v1.Pod, error) {
	a.updateCalls++
	if a.updateCalls == 1 {
		latest, err := a.GetPod(pod.Namespace, pod.Name)
		if err != nil {
			return nil, err
		}
		if a.resizePatches != 1 || !latest.Spec.Containers[0].Resources.Requests[v1.ResourceCPU].Equal(resource.MustParse("2")) {
			return nil, errors.New("expected a successful resize before the injected metadata conflict")
		}
		if a.afterResize != nil {
			if err := a.afterResize(latest); err != nil {
				return nil, err
			}
			if _, err := a.AdapterRuntimeClient.UpdatePod(latest); err != nil {
				return nil, err
			}
		}
		return nil, apierrors.NewConflict(v1.Resource("pods"), pod.Name, errors.New("injected metadata conflict after resize"))
	}
	return a.AdapterRuntimeClient.UpdatePod(pod)
}

func TestPreparedUpdateResourceRetry(t *testing.T) {
	enablePlanResourceGates(t)
	version := utilclient.GetCurrentServerVersion()
	originalVersion := *version
	version.GitVersion = "v1.35.2"
	t.Cleanup(func() { *version = originalVersion })
	for _, tc := range []struct {
		name    string
		mutate  func(*v1.Pod) error
		wantErr bool
	}{
		{name: "same-call applied target accepted"},
		{name: "approved plan disappears during retry", mutate: func(p *v1.Pod) error { delete(p.Annotations, appspub.InPlaceUpdatePlanKey); return nil }, wantErr: true},
		{name: "hook rearmed during retry", mutate: func(p *v1.Pod) error { p.Labels["example.com/hook"] = "held"; return nil }, wantErr: true},
		{name: "resize policy changes during retry", mutate: func(p *v1.Pod) error { p.Spec.Containers[0].ResizePolicy[0].RestartPolicy = v1.NotRequired; return nil }, wantErr: true},
		{name: "unrelated resource changes during retry", mutate: func(p *v1.Pod) error {
			p.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("2Gi")
			return nil
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := planTestPod()
			pod.Spec.Containers[0].ResizePolicy = []v1.ContainerResizePolicy{{ResourceName: v1.ResourceCPU, RestartPolicy: v1.RestartContainer}}
			lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(pod)
			spec := &UpdateSpec{Revision: "new", ContainerResources: map[string]v1.ResourceRequirements{"main": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}}
			prepared := preparePlanForSpec(t, pod, spec)
			pod.Annotations[appspub.InPlaceUpdatePlanKey] = prepared.planJSON
			hook := &appspub.LifecycleHook{LabelsHandler: map[string]string{"example.com/hook": "held"}}
			if updated, _, err := SyncUpdatePlan(pod, prepared, hook, nil); err != nil || updated {
				t.Fatalf("could not bind released plan: updated=%v err=%v", updated, err)
			}
			cli := fake.NewClientBuilder().WithObjects(pod).Build()
			adapter := &planResourceRetryAdapter{resourceBatchTestAdapter: &resourceBatchTestAdapter{AdapterRuntimeClient: &podadapter.AdapterRuntimeClient{Client: cli}}, afterResize: tc.mutate}
			ctrl := New(cli, revisionadapter.NewDefaultImpl()).(*realControl)
			ctrl.podAdapter = adapter
			opts := &UpdateOptions{PreparedUpdate: prepared,
				CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false },
				AdditionalFuncs:        []func(*v1.Pod){lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)},
			}
			oldRevision, newRevision := &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}}, &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "new"}}
			result := ctrl.Update(pod, oldRevision, newRevision, opts)
			if (result.UpdateErr != nil) != tc.wantErr || adapter.resizePatches != 1 {
				t.Fatalf("retry result=%+v resizes=%d wantErr=%v", result, adapter.resizePatches, tc.wantErr)
			}
			stored := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatal(err)
			}
			if tc.wantErr {
				if adapter.updateCalls != 1 || stored.Annotations[appspub.InPlaceUpdateStateKey] != "" {
					t.Fatal("revoked retry approval persisted update state")
				}
				return
			}
			if adapter.updateCalls != 2 || stored.Annotations[appspub.InPlaceUpdatePlanKey] != prepared.planJSON || result.NewResourceVersion == "" {
				t.Fatalf("successful retry lost its plan or did not retry once: result=%+v updates=%d", result, adapter.updateCalls)
			}
			if second := ctrl.Update(stored, oldRevision, newRevision, opts); second.UpdateErr == nil || adapter.updateCalls != 2 || adapter.resizePatches != 1 {
				t.Fatalf("applied-resource exception leaked into another Update call: %+v", second)
			}
		})
	}
}

func TestPreparedUpdateSourceRevision(t *testing.T) {
	for _, tc := range []struct {
		name             string
		originalRevision string
		latestRevision   string
		wantErr          bool
	}{
		{name: "original source label unchanged", originalRevision: "old", latestRevision: "old"},
		{name: "source label changed after preparation", originalRevision: "old", latestRevision: "another", wantErr: true},
		{name: "unlabeled input keeps legacy compatibility", latestRevision: "another"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := planTestPod()
			if tc.originalRevision != "" {
				pod.Labels[apps.ControllerRevisionHashLabelKey] = tc.originalRevision
			}
			prepared := preparePlanForSpec(t, pod, &UpdateSpec{Revision: "new", ContainerImages: map[string]string{"main": "main:new"}})
			latest := pod.DeepCopy()
			latest.Labels[apps.ControllerRevisionHashLabelKey] = tc.latestRevision
			cli := fake.NewClientBuilder().WithObjects(latest).Build()
			result := New(cli, revisionadapter.NewDefaultImpl()).Update(pod,
				&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}},
				&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "new"}}, &UpdateOptions{PreparedUpdate: prepared,
					CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false },
				})
			if (result.UpdateErr != nil) != tc.wantErr {
				t.Fatalf("source revision result=%+v wantErr=%v", result, tc.wantErr)
			}
			stored := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatal(err)
			}
			if tc.wantErr && (stored.Spec.Containers[0].Image != pod.Spec.Containers[0].Image || stored.Annotations[appspub.InPlaceUpdateStateKey] != "") {
				t.Fatal("source revision changed but execution applied an update")
			}
		})
	}
}

func TestGuardResourcePatch(t *testing.T) {
	pod := planTestPod()
	pod.ResourceVersion = "123"
	patch := []byte(`{"metadata":{"name":"plan-pod","uid":"stale","resourceVersion":"old"},"spec":{"containers":[{"name":"main","resources":{"requests":{"cpu":"2"}}}]}}`)
	if unchanged, err := guardResourcePatch(pod, patch); err != nil || string(unchanged) != string(patch) {
		t.Fatalf("unguarded legacy patch changed: %s err=%v", unchanged, err)
	}
	pod.Annotations[appspub.InPlaceUpdatePlanKey] = `{"planID":"approved","batches":[]}`
	before := planTestJSON(t, pod)
	guarded, err := guardResourcePatch(pod, patch)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Metadata struct {
			Name            string `json:"name"`
			UID             string `json:"uid"`
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Spec json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal(guarded, &body); err != nil {
		t.Fatal(err)
	}
	if body.Metadata.UID != string(pod.UID) || body.Metadata.ResourceVersion != pod.ResourceVersion || body.Metadata.Name != pod.Name || !strings.Contains(string(body.Spec), `"cpu":"2"`) || planTestJSON(t, pod) != before {
		t.Fatalf("resize patch missing snapshot preconditions or changed input: %s", guarded)
	}
	if _, err := guardResourcePatch(pod, []byte("invalid-json")); err == nil {
		t.Fatal("invalid approved resize patch accepted")
	}
}

func TestPreparedUpdateRejectsConcurrentResizeApproval(t *testing.T) {
	enablePlanResourceGates(t)
	version := utilclient.GetCurrentServerVersion()
	originalVersion := *version
	version.GitVersion = "v1.35.2"
	t.Cleanup(func() { *version = originalVersion })
	for _, tc := range []struct {
		name   string
		mutate func(*v1.Pod)
	}{
		{name: "hook rearmed", mutate: func(p *v1.Pod) { p.Labels["example.com/hook"] = "held" }},
		{name: "plan replaced", mutate: func(p *v1.Pod) { p.Annotations[appspub.InPlaceUpdatePlanKey] = `{"planID":"changed","batches":[]}` }},
		{name: "same-name Pod replaced", mutate: func(p *v1.Pod) { p.UID = "replacement" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := planTestPod()
			lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(pod)
			prepared := preparePlanForSpec(t, pod, &UpdateSpec{Revision: "new", ContainerResources: map[string]v1.ResourceRequirements{"main": {Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}})
			pod.Annotations[appspub.InPlaceUpdatePlanKey] = prepared.planJSON
			hook := &appspub.LifecycleHook{LabelsHandler: map[string]string{"example.com/hook": "held"}}
			if updated, _, err := SyncUpdatePlan(pod, prepared, hook, nil); err != nil || updated {
				t.Fatalf("released plan binding failed: updated=%v err=%v", updated, err)
			}
			cli := fake.NewClientBuilder().WithObjects(pod).Build()
			adapter := &planResourceRetryAdapter{resourceBatchTestAdapter: &resourceBatchTestAdapter{AdapterRuntimeClient: &podadapter.AdapterRuntimeClient{Client: cli}}, beforeResize: tc.mutate}
			ctrl := New(cli, revisionadapter.NewDefaultImpl()).(*realControl)
			ctrl.podAdapter = adapter
			result := ctrl.Update(pod, &apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}},
				&apps.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "new"}}, &UpdateOptions{PreparedUpdate: prepared,
					CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false },
				})
			if result.UpdateErr == nil || !adapter.resizeConflict || adapter.resizePatches != 1 || adapter.updateCalls != 0 {
				t.Fatalf("concurrent approval did not reject stale resize: result=%+v conflict=%v resizes=%d updates=%d", result, adapter.resizeConflict, adapter.resizePatches, adapter.updateCalls)
			}
			stored := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatal(err)
			}
			if !stored.Spec.Containers[0].Resources.Requests[v1.ResourceCPU].Equal(resource.MustParse("1")) || stored.Annotations[appspub.InPlaceUpdateStateKey] != "" {
				t.Fatal("stale resize altered resources or persisted update state before rejecting approval")
			}
		})
	}
}

type planDetachedResizeAdapter struct {
	*resourceBatchTestAdapter
}

func (a *planDetachedResizeAdapter) PatchPodResource(pod *v1.Pod, patch client.Patch) (*v1.Pod, error) {
	return a.resourceBatchTestAdapter.PatchPodResource(pod.DeepCopy(), patch)
}

func TestApplyResizeResultPreservesPendingChanges(t *testing.T) {
	pod := planTestPod()
	pod.Annotations[appspub.InPlaceUpdatePlanKey] = "approved-plan"
	pod.Annotations[appspub.InPlaceUpdateStateKey] = "pending-state"
	pod.Labels[apps.ControllerRevisionHashLabelKey] = "new"
	lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)(pod)
	pod.Spec.Containers[0].Image = "main:new"
	pod.Spec.Containers = append(pod.Spec.Containers, guaranteedInitTestContainer("extra", false, "500m", "512Mi"))
	pod.Spec.InitContainers = []v1.Container{guaranteedInitTestContainer("sidecar", true, "100m", "128Mi")}
	resized := pod.DeepCopy()
	resized.ResourceVersion = "after-resize"
	resized.Annotations[appspub.InPlaceUpdatePlanKey], resized.Annotations[appspub.InPlaceUpdateStateKey] = "old-plan", "old-state"
	resized.Labels[apps.ControllerRevisionHashLabelKey] = "old"
	resized.Spec.Containers[0].Image = "main:old"
	resized.Spec.InitContainers[0].Image = "sidecar:old"
	resized.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2")
	resized.Spec.InitContainers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("200m")
	resized.Spec.Containers[0], resized.Spec.Containers[1] = resized.Spec.Containers[1], resized.Spec.Containers[0]
	beforeResponse := planTestJSON(t, resized)
	want := pod.DeepCopy()
	want.ResourceVersion = resized.ResourceVersion
	want.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2")
	want.Spec.InitContainers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("200m")
	applyResizeResult(pod, resized)
	if planTestJSON(t, pod) != planTestJSON(t, want) || planTestJSON(t, resized) != beforeResponse {
		t.Fatal("resize result lost pending images/metadata, mismatched container names, or mutated response")
	}
	resized.Spec.Containers[1].Resources.Requests[v1.ResourceCPU] = resource.MustParse("9")
	resized.Spec.InitContainers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("900m")
	if planTestJSON(t, pod) != planTestJSON(t, want) {
		t.Fatal("resize result resources alias the detached response")
	}
	applyResizeResult(pod, nil)
	if planTestJSON(t, pod) != planTestJSON(t, want) {
		t.Fatal("nil resize response changed the Pod")
	}
	legacy := planTestPod()
	beforeLegacy := planTestJSON(t, legacy)
	applyResizeResult(legacy, resized)
	if planTestJSON(t, legacy) != beforeLegacy {
		t.Fatal("resize result changed legacy no-plan behavior")
	}
}

func TestPreparedUpdateChangedTargetKeepsUnappliedImages(t *testing.T) {
	pod := planTestPod()
	pod.Spec.Containers = []v1.Container{
		{Name: "high", Image: "h1", Env: []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(10, pod.Name)}},
		{Name: "low", Image: "l1", Env: []v1.EnvVar{utilcontainerlaunchpriority.GeneratePriorityEnv(0, pod.Name)}},
	}
	pod.Labels[apps.ControllerRevisionHashLabelKey] = "A"
	pod.Labels["example.com/hook"] = "held"
	lifecycle.SetPodLifecycle(appspub.LifecycleStateNormal)(pod)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "high", Ready: true}, {Name: "low", Ready: true}}
	revision := func(name, high, low string) *apps.ControllerRevision {
		t.Helper()
		template := &v1.PodTemplateSpec{Spec: *pod.Spec.DeepCopy()}
		template.Spec.Containers[0].Image, template.Spec.Containers[1].Image = high, low
		return revisionOf(name, planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": template}}))
	}
	revA, revB, revC := revision("A", "h1", "l1"), revision("B", "h2", "l2"), revision("C", "h3", "l2")
	cli := fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()
	ctrl, life := New(cli, revisionadapter.NewDefaultImpl()), lifecycle.New(cli)
	hook := &appspub.LifecycleHook{LabelsHandler: map[string]string{"example.com/hook": "held"}}
	get := func() *v1.Pod {
		t.Helper()
		current := &v1.Pod{}
		if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
			t.Fatal(err)
		}
		return current
	}
	prepare := func(current *v1.Pod, old, target *apps.ControllerRevision) *PreparedUpdate {
		t.Helper()
		beforePod, beforeOld, beforeTarget := planTestJSON(t, current), planTestJSON(t, old), planTestJSON(t, target)
		prepared, err := PrepareUpdate(current, old, target, nil)
		if err != nil || prepared == nil {
			t.Fatalf("target %s preparation failed: prepared=%v err=%v", target.Name, prepared, err)
		}
		if planTestJSON(t, current) != beforePod || planTestJSON(t, old) != beforeOld || planTestJSON(t, target) != beforeTarget {
			t.Fatal("changed-target preparation mutated Pod or revisions")
		}
		return prepared
	}
	approveAndExecute := func(current *v1.Pod, prepared *PreparedUpdate, old, target *apps.ControllerRevision) {
		t.Helper()
		if updated, _, err := SyncUpdatePlan(current, prepared, hook, life); err != nil || !updated {
			t.Fatalf("target %s plan was not armed: updated=%v err=%v", target.Name, updated, err)
		}
		current = get()
		if current.Annotations[appspub.InPlaceUpdatePlanKey] != prepared.planJSON || !lifecycle.IsPodHooked(hook, current) {
			t.Fatal("whole-cycle target was not published before approval")
		}
		before := planTestJSON(t, current)
		if result := ctrl.Refresh(current, &UpdateOptions{CheckContainersUpdateCompleted: func(*v1.Pod, *appspub.InPlaceUpdateState) error { return nil }}); result.RefreshErr != nil || planTestJSON(t, get()) != before {
			t.Fatalf("new target approval did not freeze old pending work: %+v", result)
		}
		delete(current.Labels, "example.com/hook")
		if err := cli.Update(context.Background(), current); err != nil {
			t.Fatal(err)
		}
		current = get()
		if updated, _, err := SyncUpdatePlan(current, prepared, hook, life); err != nil || updated {
			t.Fatalf("released target plan rearmed: updated=%v err=%v", updated, err)
		}
		result := ctrl.Update(current, old, target, &UpdateOptions{PreparedUpdate: prepared,
			CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false },
			AdditionalFuncs:        []func(*v1.Pod){lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)},
		})
		if result.UpdateErr != nil || !result.InPlaceUpdate {
			t.Fatalf("target %s execution failed: %+v", target.Name, result)
		}
	}
	first := prepare(get(), revA, revB)
	approveAndExecute(get(), first, revA, revB)
	partial := get()
	var oldState appspub.InPlaceUpdateState
	if err := json.Unmarshal([]byte(partial.Annotations[appspub.InPlaceUpdateStateKey]), &oldState); err != nil {
		t.Fatal(err)
	}
	if partial.Spec.Containers[0].Image != "h2" || partial.Spec.Containers[1].Image != "l1" || partial.Labels[apps.ControllerRevisionHashLabelKey] != "B" || oldState.NextContainerImages["low"] != "l2" {
		t.Fatalf("fixture is not partial A-to-B: pod=%s state=%s", planTestJSON(t, partial), planTestJSON(t, oldState))
	}
	partial.Status.ContainerStatuses[0].Image = "h2:latest"
	partial.Status.ContainerStatuses[0].ImageID = "runtime-high-B"
	if err := cli.Status().Update(context.Background(), partial); err != nil {
		t.Fatal(err)
	}
	partial = get()
	next := prepare(partial, revB, revC)
	want := []appspub.InPlaceUpdatePlanBatch{
		{Containers: []appspub.InPlaceUpdatePlanContainer{{Name: "high", RestartRequired: true, Changes: []string{"Image"}}}},
		{Containers: []appspub.InPlaceUpdatePlanContainer{{Name: "low", RestartRequired: true, Changes: []string{"Image"}}}},
	}
	if next.Spec.ContainerImages["high"] != "h3" || next.Spec.ContainerImages["low"] != "l2" || !reflect.DeepEqual(next.Plan.Batches, want) {
		t.Fatalf("new target lost unapplied low image: spec=%s plan=%s", planTestJSON(t, next.Spec), planTestJSON(t, next.Plan))
	}
	approveAndExecute(partial, next, revB, revC)
	if result := ctrl.Refresh(get(), &UpdateOptions{CheckContainersUpdateCompleted: func(*v1.Pod, *appspub.InPlaceUpdateState) error { return nil }}); result.RefreshErr != nil {
		t.Fatalf("new target final batch failed: %+v", result)
	}
	final := get()
	if final.Spec.Containers[0].Image != "h3" || final.Spec.Containers[1].Image != "l2" || final.Labels[apps.ControllerRevisionHashLabelKey] != "C" || final.Annotations[appspub.InPlaceUpdatePlanKey] != next.planJSON {
		t.Fatalf("new whole-cycle target did not converge: %s", planTestJSON(t, final))
	}
}

func TestPreparingUpdateWithoutPlanFreezesDeferredWork(t *testing.T) {
	for _, grace := range []bool{false, true} {
		name := "pending batch"
		if grace {
			name = "expired grace"
		}
		t.Run(name, func(t *testing.T) {
			pod := planTestPod()
			pod.Labels[apps.ControllerRevisionHashLabelKey] = "B"
			lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(pod)
			pod.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "main", Ready: true}}
			state := appspub.InPlaceUpdateState{Revision: "B", UpdateTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
				NextContainerImages:    map[string]string{"main": "main:new"},
				ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Containers: []string{}}},
			}
			pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, state)
			if grace {
				pod.Annotations[appspub.InPlaceUpdateGraceKey] = planTestJSON(t, &UpdateSpec{Revision: "B", GraceSeconds: 60, ContainerImages: map[string]string{"main": "main:new"}})
			}
			cli := fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()
			ctrl := New(cli, revisionadapter.NewDefaultImpl()).(*realControl)
			current := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
				t.Fatal(err)
			}
			before := planTestJSON(t, current)
			opts := SetOptionsDefaults(&UpdateOptions{CheckContainersUpdateCompleted: func(*v1.Pod, *appspub.InPlaceUpdateState) error { return nil }})
			if result := ctrl.Refresh(current, opts); result.RefreshErr != nil || result.DelayDuration != 0 {
				t.Fatalf("missing-plan PreparingUpdate Refresh was not frozen: %+v", result)
			}
			if updated, err := ctrl.updateNextBatch(current, opts); err != nil || updated {
				t.Fatalf("missing-plan PreparingUpdate next batch was not frozen: updated=%v err=%v", updated, err)
			}
			if delay, err := ctrl.finishGracePeriod(current, opts); err != nil || delay != 0 {
				t.Fatalf("missing-plan PreparingUpdate grace was not frozen: delay=%v err=%v", delay, err)
			}
			stored := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatal(err)
			}
			if planTestJSON(t, stored) != before || planTestJSON(t, current) != before {
				t.Fatal("missing approved plan allowed old deferred work before repair")
			}
		})
	}
}

func TestPreparedUpdateDeletesOnlyTemplateMetadata(t *testing.T) {
	enablePlanResourceGates(t)
	version := utilclient.GetCurrentServerVersion()
	originalVersion := *version
	version.GitVersion = "v1.35.2"
	t.Cleanup(func() { *version = originalVersion })
	for _, field := range []string{"labels", "annotations"} {
		for _, empty := range []bool{false, true} {
			name := field + "/nil"
			if empty {
				name = field + "/empty"
			}
			t.Run(name, func(t *testing.T) {
				pod := planTestPod()
				pod.Labels[apps.ControllerRevisionHashLabelKey] = "old"
				pod.Labels["example.com/hook"] = "held"
				pod.Labels["example.com/external"] = "keep"
				pod.Annotations["example.com/external"] = "keep"
				lifecycle.SetPodLifecycle(appspub.LifecycleStateNormal)(pod)
				if field == "labels" {
					pod.Labels["example.com/business"] = "remove"
				} else {
					pod.Annotations["example.com/business"] = "remove"
				}
				oldSpec := pod.Spec.DeepCopy()
				newSpec := oldSpec.DeepCopy()
				newSpec.Containers[0].Image = "main:new"
				newSpec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2")
				newSpec.Containers[0].Resources.Limits[v1.ResourceCPU] = resource.MustParse("2")
				oldMetadata := map[string]interface{}{field: map[string]string{"example.com/business": "remove"}}
				newMetadata := map[string]interface{}{}
				if empty {
					newMetadata[field] = map[string]string{}
				}
				revision := func(name string, spec *v1.PodSpec, metadata map[string]interface{}) *apps.ControllerRevision {
					return revisionOf(name, planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": map[string]interface{}{"metadata": metadata, "spec": spec}}}))
				}
				oldRevision, newRevision := revision("old", oldSpec, oldMetadata), revision("new", newSpec, newMetadata)
				cli := fake.NewClientBuilder().WithObjects(pod).Build()
				ctrl := New(cli, revisionadapter.NewDefaultImpl()).(*realControl)
				adapter := &resourceBatchTestAdapter{AdapterRuntimeClient: &podadapter.AdapterRuntimeClient{Client: cli}}
				ctrl.podAdapter = &planDetachedResizeAdapter{resourceBatchTestAdapter: adapter}
				hook := &appspub.LifecycleHook{LabelsHandler: map[string]string{"example.com/hook": "held"}}
				life := lifecycle.New(cli)
				prepared, err := PrepareUpdate(pod, oldRevision, newRevision, nil)
				if err != nil || prepared == nil {
					t.Fatalf("prepare failed: prepared=%v err=%v", prepared, err)
				}
				if updated, _, err := SyncUpdatePlan(pod, prepared, hook, life); err != nil || !updated {
					t.Fatalf("plan publication failed: updated=%v err=%v", updated, err)
				}
				current := &v1.Pod{}
				if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
					t.Fatal(err)
				}
				approved := current.Annotations[appspub.InPlaceUpdatePlanKey]
				delete(current.Labels, "example.com/hook")
				if err := cli.Update(context.Background(), current); err != nil {
					t.Fatal(err)
				}
				prepared, err = PrepareUpdate(current, oldRevision, newRevision, nil)
				if err != nil || prepared == nil {
					t.Fatalf("released preparation failed: prepared=%v err=%v", prepared, err)
				}
				if updated, _, err := SyncUpdatePlan(current, prepared, hook, life); err != nil || updated {
					t.Fatalf("released approval was changed: updated=%v err=%v", updated, err)
				}
				result := ctrl.Update(current, oldRevision, newRevision, &UpdateOptions{PreparedUpdate: prepared,
					CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false },
					AdditionalFuncs:        []func(*v1.Pod){lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)},
				})
				if result.UpdateErr != nil || !result.InPlaceUpdate {
					t.Fatalf("execution failed: %+v", result)
				}
				if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
					t.Fatal(err)
				}
				if current.Labels["example.com/business"] != "" || current.Annotations["example.com/business"] != "" || current.Labels["example.com/external"] != "keep" || current.Annotations["example.com/external"] != "keep" {
					t.Fatal("metadata deletion removed external keys or retained a deleted template key")
				}
				if current.Labels[apps.ControllerRevisionHashLabelKey] != "new" || lifecycle.GetPodLifecycleState(current) != appspub.LifecycleStateUpdating || current.Annotations[appspub.InPlaceUpdatePlanKey] != approved || current.Annotations[appspub.InPlaceUpdateStateKey] == "" {
					t.Fatal("metadata deletion lost revision, lifecycle, approved plan, or execution state")
				}
				if current.Spec.Containers[0].Image != "main:new" || !current.Spec.Containers[0].Resources.Requests[v1.ResourceCPU].Equal(resource.MustParse("2")) || adapter.resizePatches != 1 {
					t.Fatal("metadata deletion lost the combined image/resource update")
				}
			})
		}
	}
}

func TestPlannedUpdateRejectsLegacyMetadataMapDeletion(t *testing.T) {
	for _, field := range []string{"labels", "annotations"} {
		t.Run(field, func(t *testing.T) {
			pod := planTestPod()
			pod.Annotations[appspub.InPlaceUpdatePlanKey] = "approved"
			patch := []byte(`{"metadata":{"` + field + `":null}}`)
			before := planTestJSON(t, pod)
			if _, _, err := defaultPatchUpdateSpecToPod(pod, &UpdateSpec{MetaDataPatch: patch}, &appspub.InPlaceUpdateState{}); err == nil || planTestJSON(t, pod) != before {
				t.Fatal("legacy whole-map deletion was accepted or mutated the Pod")
			}
			pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, appspub.InPlaceUpdateState{Revision: "old"})
			pod.Annotations[appspub.InPlaceUpdateGraceKey] = planTestJSON(t, UpdateSpec{Revision: "old", MetaDataPatch: patch})
			oldRevision := revisionOf("old", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": &v1.PodTemplateSpec{Spec: pod.Spec}}}))
			if _, err := revisionWithPendingInputs(pod, oldRevision, "new"); err == nil {
				t.Fatal("legacy grace whole-map deletion silently discarded pending metadata")
			}
		})
	}
}

func TestResumeUpdatePlanRestoresApprovedCycle(t *testing.T) {
	original := &appspub.InPlaceUpdatePlan{PlanID: "original-B", Batches: []appspub.InPlaceUpdatePlanBatch{
		{Containers: []appspub.InPlaceUpdatePlanContainer{{Name: "main", RestartRequired: true, Changes: []string{"Image"}}}},
	}}
	for _, missing := range []bool{false, true} {
		for _, grace := range []bool{false, true} {
			name := "replacement"
			if missing {
				name = "missing annotation"
			}
			if grace {
				name += "/grace"
			}
			t.Run(name, func(t *testing.T) {
				pod := planTestPod()
				pod.Labels[apps.ControllerRevisionHashLabelKey] = "B"
				pod.Labels["example.com/hook"] = "held"
				pod.Finalizers = []string{"example.com/hook", "example.com/unrelated"}
				lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(pod)
				state := appspub.InPlaceUpdateState{Revision: "B", UpdatePlan: original, UpdateTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
					NextContainerImages: map[string]string{"main": "main:B"}, ContainerBatchesRecord: []appspub.InPlaceUpdateContainerBatch{{Containers: []string{}}},
				}
				pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, state)
				if !missing {
					pod.Annotations[appspub.InPlaceUpdatePlanKey] = `{"planID":"replacement-C","batches":[]}`
				}
				if grace {
					pod.Annotations[appspub.InPlaceUpdateGraceKey] = planTestJSON(t, UpdateSpec{Revision: "B", GraceSeconds: 60, ContainerImages: map[string]string{"main": "main:B"}})
				}
				cli := &planMetadataRecordingClient{Client: fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()}
				control := lifecycle.New(cli)
				current := &v1.Pod{}
				if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
					t.Fatal(err)
				}
				before, stateJSON, graceJSON := planTestJSON(t, current), current.Annotations[appspub.InPlaceUpdateStateKey], current.Annotations[appspub.InPlaceUpdateGraceKey]
				updated, _, err := ResumeUpdatePlan(current, "B", control)
				if err != nil || !updated || len(cli.patches) != 1 || planTestJSON(t, current) != before {
					t.Fatalf("resume failed: updated=%v err=%v patches=%d", updated, err, len(cli.patches))
				}
				var payload struct {
					Metadata struct {
						Labels      map[string]string `json:"labels"`
						Annotations map[string]string `json:"annotations"`
					} `json:"metadata"`
				}
				if err := json.Unmarshal(cli.patches[0], &payload); err != nil {
					t.Fatal(err)
				}
				if payload.Metadata.Labels[appspub.LifecycleStateKey] != string(appspub.LifecycleStateUpdating) || payload.Metadata.Annotations[appspub.InPlaceUpdatePlanKey] != planTestJSON(t, original) {
					t.Fatalf("resume did not atomically restore original plan and Updating: %s", cli.patches[0])
				}
				stored := &v1.Pod{}
				if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
					t.Fatal(err)
				}
				if stored.Annotations[appspub.InPlaceUpdateStateKey] != stateJSON || stored.Annotations[appspub.InPlaceUpdateGraceKey] != graceJSON || stored.Labels["example.com/hook"] != "held" || !reflect.DeepEqual(stored.Finalizers, pod.Finalizers) {
					t.Fatal("resume changed execution progress/grace or lifecycle handlers")
				}
				if updated, _, err := ResumeUpdatePlan(stored, "B", control); err != nil || updated || len(cli.patches) != 1 {
					t.Fatalf("repeated resume churned metadata: updated=%v err=%v", updated, err)
				}
				result := New(cli, revisionadapter.NewDefaultImpl()).Refresh(stored, &UpdateOptions{CheckContainersUpdateCompleted: func(*v1.Pod, *appspub.InPlaceUpdateState) error { return nil }})
				if result.RefreshErr != nil {
					t.Fatalf("resumed cycle could not execute old pending work: %+v", result)
				}
				if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
					t.Fatal(err)
				}
				if stored.Spec.Containers[0].Image != "main:B" || stored.Annotations[appspub.InPlaceUpdatePlanKey] != planTestJSON(t, original) {
					t.Fatal("resumed pending work ran without its original full plan")
				}
			})
		}
	}
}

func TestResumeUpdatePlanNoopAndErrors(t *testing.T) {
	original := &appspub.InPlaceUpdatePlan{PlanID: "B", Batches: []appspub.InPlaceUpdatePlanBatch{}}
	for _, tc := range []struct {
		name    string
		state   appspub.LifecycleStateType
		raw     string
		target  string
		wantErr bool
	}{
		{name: "not preparing", state: appspub.LifecycleStateUpdating, raw: "invalid", target: "B"},
		{name: "no state", state: appspub.LifecycleStatePreparingUpdate, target: "B"},
		{name: "target mismatch", state: appspub.LifecycleStatePreparingUpdate, raw: planTestJSON(t, appspub.InPlaceUpdateState{Revision: "B", UpdatePlan: original}), target: "C"},
		{name: "legacy state lacks original approval", state: appspub.LifecycleStatePreparingUpdate, raw: planTestJSON(t, appspub.InPlaceUpdateState{Revision: "B"}), target: "B", wantErr: true},
		{name: "invalid original approval", state: appspub.LifecycleStatePreparingUpdate, raw: planTestJSON(t, appspub.InPlaceUpdateState{Revision: "B", UpdatePlan: &appspub.InPlaceUpdatePlan{}}), target: "B", wantErr: true},
		{name: "invalid state", state: appspub.LifecycleStatePreparingUpdate, raw: "invalid", target: "B", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := planTestPod()
			lifecycle.SetPodLifecycle(tc.state)(pod)
			if tc.raw != "" {
				pod.Annotations[appspub.InPlaceUpdateStateKey] = tc.raw
			}
			before := planTestJSON(t, pod)
			if updated, got, err := ResumeUpdatePlan(pod, tc.target, nil); updated || got != pod || (err != nil) != tc.wantErr || planTestJSON(t, pod) != before {
				t.Fatalf("resume no-op/error result: updated=%v got=%v err=%v", updated, got, err)
			}
		})
	}
	if updated, got, err := ResumeUpdatePlan(nil, "B", nil); updated || got != nil || err != nil {
		t.Fatalf("nil resume was not a no-op: updated=%v got=%v err=%v", updated, got, err)
	}
	pod := planTestPod()
	lifecycle.SetPodLifecycle(appspub.LifecycleStatePreparingUpdate)(pod)
	pod.Annotations[appspub.InPlaceUpdatePlanKey] = "replacement-C"
	pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, appspub.InPlaceUpdateState{Revision: "B", UpdatePlan: original})
	cli := &planMetadataRecordingClient{Client: fake.NewClientBuilder().WithObjects(pod).Build()}
	stale := &v1.Pod{}
	if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stale); err != nil {
		t.Fatal(err)
	}
	latest := stale.DeepCopy()
	latest.Annotations["example.com/concurrent"] = "change"
	if err := cli.Update(context.Background(), latest); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ResumeUpdatePlan(stale, "B", lifecycle.New(cli)); !apierrors.IsConflict(err) {
		t.Fatalf("stale resume did not return metadata conflict: %v", err)
	}
	stored := &v1.Pod{}
	if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Annotations[appspub.InPlaceUpdatePlanKey] != "replacement-C" || lifecycle.GetPodLifecycleState(stored) != appspub.LifecycleStatePreparingUpdate {
		t.Fatal("failed resume partially changed approved plan/lifecycle")
	}
}

func TestPreparedUpdateChangedTargetKeepsUnappliedResourcesAndMetadata(t *testing.T) {
	enablePlanResourceGates(t)
	defer utilfeature.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlaceUpdateEnvFromMetadata, true)()
	for _, grace := range []bool{false, true} {
		name := "pending"
		if grace {
			name = "grace"
		}
		t.Run(name, func(t *testing.T) {
			pod := planTestPod()
			pod.Annotations["example.com/config"] = "A"
			pod.Spec.Containers[0].Env = []v1.EnvVar{{Name: "CONFIG", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.annotations['example.com/config']"}}}}
			oldTemplate := &v1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"example.com/config": "B"}}, Spec: *pod.Spec.DeepCopy()}
			oldTemplate.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2")
			oldTemplate.Spec.Containers[0].Resources.Limits[v1.ResourceCPU] = resource.MustParse("2")
			newTemplate := oldTemplate.DeepCopy()
			newTemplate.Spec.Containers[0].Image = "main:new"
			newTemplate.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("2Gi")
			newTemplate.Spec.Containers[0].Resources.Limits[v1.ResourceMemory] = resource.MustParse("2Gi")
			oldRevision := revisionOf("B", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": oldTemplate}}))
			newRevision := revisionOf("C", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": newTemplate}}))
			pending := UpdateSpec{Revision: "B", ContainerResources: map[string]v1.ResourceRequirements{"main": {
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}, Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
			}}, ContainerRefMetadata: map[string]metav1.ObjectMeta{"main": {Annotations: map[string]string{"example.com/config": "B"}}}}
			state := appspub.InPlaceUpdateState{Revision: "B", NextContainerResources: pending.ContainerResources, NextContainerRefMetadata: pending.ContainerRefMetadata}
			pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, state)
			if grace {
				pod.Annotations[appspub.InPlaceUpdateGraceKey] = planTestJSON(t, pending)
			}
			beforePod, beforeOld, beforeNew := planTestJSON(t, pod), planTestJSON(t, oldRevision), planTestJSON(t, newRevision)
			prepared, err := PrepareUpdate(pod, oldRevision, newRevision, nil)
			if err != nil || prepared == nil {
				t.Fatalf("changed target preparation failed: prepared=%v err=%v", prepared, err)
			}
			resources := prepared.Spec.ContainerResources["main"]
			if !resources.Requests[v1.ResourceCPU].Equal(resource.MustParse("2")) || !resources.Limits[v1.ResourceCPU].Equal(resource.MustParse("2")) || !resources.Requests[v1.ResourceMemory].Equal(resource.MustParse("2Gi")) || prepared.Spec.ContainerRefMetadata["main"].Annotations["example.com/config"] != "B" {
				t.Fatalf("changed target lost pending resource/metadata values: %s", planTestJSON(t, prepared.Spec))
			}
			want := []appspub.InPlaceUpdatePlanBatch{{Containers: []appspub.InPlaceUpdatePlanContainer{{Name: "main", RestartRequired: true, Changes: []string{"Image", "Resources", "MetadataEnv"}}}}}
			if !reflect.DeepEqual(prepared.Plan.Batches, want) || planTestJSON(t, pod) != beforePod || planTestJSON(t, oldRevision) != beforeOld || planTestJSON(t, newRevision) != beforeNew {
				t.Fatal("changed target plan mismatched its complete spec or mutated inputs")
			}
		})
	}
}

func TestPreparedUpdateWaitsForPreviousRuntimeConvergence(t *testing.T) {
	enablePlanResourceGates(t)
	for _, kind := range []string{"Resources", "MetadataEnv", "Image"} {
		t.Run(kind, func(t *testing.T) {
			pod := planTestPod()
			pod.Spec.Containers = append(pod.Spec.Containers, guaranteedInitTestContainer("worker", false, "100m", "128Mi"))
			pod.Labels[apps.ControllerRevisionHashLabelKey] = "B"
			lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)(pod)
			previous := appspub.InPlaceUpdateState{Revision: "B", UpdatePlan: &appspub.InPlaceUpdatePlan{PlanID: "approved-B", Batches: []appspub.InPlaceUpdatePlanBatch{}}}
			for _, c := range pod.Spec.Containers {
				pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, v1.ContainerStatus{Name: c.Name, Image: c.Image, ImageID: "before-" + c.Name, ContainerID: "containerd://" + c.Name, Ready: true, Resources: c.Resources.DeepCopy()})
			}
			switch kind {
			case "Resources":
				previous.UpdateResources = true
				pod.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("2")
				pod.Spec.Containers[0].Resources.Limits[v1.ResourceCPU] = resource.MustParse("2")
			case "MetadataEnv":
				previous.UpdateEnvFromMetadata = true
				pod.Annotations["example.com/config"] = "B"
				pod.Spec.Containers[0].Env = []v1.EnvVar{{Name: "CONFIG", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.annotations['example.com/config']"}}}}
			case "Image":
				previous.UpdateImages = true
				previous.LastContainerStatuses = map[string]appspub.InPlaceUpdateContainerStatus{"main": {ImageID: "before-main"}}
				pod.Spec.Containers[0].Image = "main:B"
			}
			pod.Annotations[appspub.InPlaceUpdatePlanKey] = planTestJSON(t, previous.UpdatePlan)
			pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, previous)
			oldTemplate := &v1.PodTemplateSpec{Spec: *pod.Spec.DeepCopy()}
			newTemplate := oldTemplate.DeepCopy()
			newTemplate.Spec.Containers[1].Image = "worker:C"
			oldRevision := revisionOf("B", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": oldTemplate}}))
			newRevision := revisionOf("C", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": newTemplate}}))
			prepared, err := PrepareUpdate(pod, oldRevision, newRevision, nil)
			if err != nil || prepared == nil {
				t.Fatalf("preparation failed: prepared=%v err=%v", prepared, err)
			}
			if prepared.Spec.UpdateEnvFromMetadata || len(prepared.Spec.ContainerResources) != 0 || len(prepared.Spec.ContainerImages) != 1 {
				t.Fatal("fixture does not isolate an old current batch from the new spec")
			}
			cli := fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()
			life := lifecycle.New(cli)
			hook := &appspub.LifecycleHook{LabelsHandler: map[string]string{"example.com/hook": "held"}}
			if updated, _, err := SyncUpdatePlan(pod, prepared, hook, life); err != nil || !updated {
				t.Fatalf("new target was not published before convergence: updated=%v err=%v", updated, err)
			}
			get := func() *v1.Pod {
				t.Helper()
				current := &v1.Pod{}
				if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
					t.Fatal(err)
				}
				return current
			}
			current := get()
			delete(current.Labels, "example.com/hook")
			if err := cli.Update(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			current = get()
			if updated, _, err := SyncUpdatePlan(current, prepared, hook, life); err != nil || updated {
				t.Fatalf("released plan was changed: updated=%v err=%v", updated, err)
			}
			ctrl := New(cli, revisionadapter.NewDefaultImpl())
			opts := &UpdateOptions{PreparedUpdate: prepared, CheckPodNeedsBeUnready: func(*v1.Pod, *UpdateSpec) bool { return false }, AdditionalFuncs: []func(*v1.Pod){lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)}}
			before := planTestJSON(t, current)
			assertBlocked := func(input *v1.Pod) {
				t.Helper()
				result := ctrl.Update(input, oldRevision, newRevision, opts)
				if !result.InPlaceUpdate || result.UpdateErr == nil || !strings.Contains(result.UpdateErr.Error(), "previous in-place update batch has not converged") || planTestJSON(t, get()) != before {
					t.Fatalf("old convergence tracking was bypassed or mutated: %+v", result)
				}
			}
			assertBlocked(current)
			converge := func(current *v1.Pod) {
				current.Status.ContainerStatuses[0].Resources = current.Spec.Containers[0].Resources.DeepCopy()
				current.Status.ContainerStatuses[0].Image = current.Spec.Containers[0].Image
				current.Status.ContainerStatuses[0].ImageID = "after-main"
				if kind == "MetadataEnv" {
					meta := appspub.RuntimeContainerMetaSet{}
					for _, c := range current.Spec.Containers {
						meta.Containers = append(meta.Containers, appspub.RuntimeContainerMeta{Name: c.Name, ContainerID: "containerd://" + c.Name, Hashes: appspub.RuntimeContainerHashes{PlainHash: hashContainer(&c), ExtractedEnvFromMetadataHash: utilcontainermeta.NewEnvFromMetadataHasher().GetExpectHash(&c, current)}})
					}
					current.Annotations[appspub.RuntimeContainerMetaKey] = planTestJSON(t, meta)
				}
			}
			cached := current.DeepCopy()
			converge(cached)
			assertBlocked(cached)
			converge(current)
			if err := cli.Update(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			converge(current)
			if err := cli.Status().Update(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			if result := ctrl.Update(get(), oldRevision, newRevision, opts); result.UpdateErr != nil || !result.InPlaceUpdate {
				t.Fatalf("converged old batch did not release the approved new cycle: %+v", result)
			}
			current = get()
			if current.Spec.Containers[1].Image != "worker:C" || current.Labels[apps.ControllerRevisionHashLabelKey] != "C" || current.Annotations[appspub.InPlaceUpdatePlanKey] != prepared.planJSON {
				t.Fatal("approved target did not execute after old convergence")
			}
			if kind == "Image" && len(prepared.previousState.LastContainerStatuses) != 1 {
				t.Fatal("completion checker mutated the preserved old state")
			}
		})
	}
}

func TestSyncUpdatePlanDoesNotSuspendLegacyIncompleteCycle(t *testing.T) {
	for _, grace := range []bool{false, true} {
		name := "pending"
		if grace {
			name = "grace"
		}
		t.Run(name, func(t *testing.T) {
			pod := planTestPod()
			pod.Labels[apps.ControllerRevisionHashLabelKey] = "B"
			lifecycle.SetPodLifecycle(appspub.LifecycleStateUpdating)(pod)
			pod.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "main", Image: pod.Spec.Containers[0].Image, ImageID: "before", Ready: true}}
			oldTemplate := &v1.PodTemplateSpec{Spec: *pod.Spec.DeepCopy()}
			oldTemplate.Spec.Containers[0].Image = "main:B"
			newTemplate := oldTemplate.DeepCopy()
			newTemplate.Spec.Containers[0].Image = "main:C"
			oldRevision := revisionOf("B", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": oldTemplate}}))
			newRevision := revisionOf("C", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": newTemplate}}))
			state := appspub.InPlaceUpdateState{Revision: "B", NextContainerImages: map[string]string{"main": "main:B"}, UpdateTimestamp: metav1.NewTime(time.Now().Add(-time.Hour))}
			pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, state)
			if grace {
				pod.Annotations[appspub.InPlaceUpdateGraceKey] = planTestJSON(t, UpdateSpec{Revision: "B", GraceSeconds: 60, ContainerImages: state.NextContainerImages})
			}
			prepared, err := PrepareUpdate(pod, oldRevision, newRevision, nil)
			if err != nil || prepared == nil {
				t.Fatalf("preparation failed: prepared=%v err=%v", prepared, err)
			}
			before := planTestJSON(t, pod)
			if updated, _, err := SyncUpdatePlan(pod, prepared, &appspub.LifecycleHook{FinalizersHandler: []string{"example.com/hook"}}, nil); updated || err == nil || planTestJSON(t, pod) != before {
				t.Fatal("legacy cycle without a recoverable plan was silently frozen")
			}
			cli := fake.NewClientBuilder().WithStatusSubresource(&v1.Pod{}).WithObjects(pod).Build()
			ctrl := New(cli, revisionadapter.NewDefaultImpl())
			if result := ctrl.Refresh(pod, nil); result.RefreshErr != nil {
				t.Fatalf("legacy cycle could not continue: %+v", result)
			}
			current := &v1.Pod{}
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
				t.Fatal(err)
			}
			if current.Spec.Containers[0].Image != "main:B" || lifecycle.GetPodLifecycleState(current) != appspub.LifecycleStateUpdating {
				t.Fatal("rejected replacement prevented the original legacy cycle from progressing")
			}
			current.Status.ContainerStatuses[0].Image, current.Status.ContainerStatuses[0].ImageID = "main:B", "after"
			if err := cli.Status().Update(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			if err := DefaultCheckInPlaceUpdateCompleted(current); err != nil {
				t.Fatalf("legacy cycle did not complete: %v", err)
			}
			prepared, err = PrepareUpdate(current, oldRevision, newRevision, nil)
			if err != nil || prepared == nil {
				t.Fatalf("completed legacy preparation failed: prepared=%v err=%v", prepared, err)
			}
			if updated, _, err := SyncUpdatePlan(current, prepared, nil, lifecycle.New(cli)); err != nil || !updated {
				t.Fatalf("completed legacy cycle could not start a new approval: updated=%v err=%v", updated, err)
			}
		})
	}
}

func TestPrepareUpdateRejectsFailedPendingRecalculation(t *testing.T) {
	pod := planTestPod()
	pod.Annotations[appspub.InPlaceUpdateStateKey] = planTestJSON(t, appspub.InPlaceUpdateState{Revision: "B", NextContainerImages: map[string]string{"main": "main:B"}})
	template := &v1.PodTemplateSpec{Spec: *pod.Spec.DeepCopy()}
	template.Spec.Containers[0].Image = "main:B"
	oldRevision := revisionOf("B", planTestJSON(t, map[string]interface{}{"spec": map[string]interface{}{"template": template}}))
	calls := 0
	before := planTestJSON(t, pod)
	prepared, err := PrepareUpdate(pod, oldRevision, revisionOf("C", "{}"), &UpdateOptions{CalculateSpec: func(_, _ *apps.ControllerRevision, _ *UpdateOptions) *UpdateSpec {
		calls++
		if calls == 1 {
			return &UpdateSpec{Revision: "C", ContainerImages: map[string]string{"main": "main:C"}}
		}
		return nil
	}})
	if err == nil || prepared != nil || calls != 2 || planTestJSON(t, pod) != before {
		t.Fatalf("failed pending recovery silently allowed recreate: prepared=%v err=%v calls=%d", prepared, err, calls)
	}
}
