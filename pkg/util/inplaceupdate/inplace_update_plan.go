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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	appspub "github.com/openkruise/kruise/apis/apps/pub"
	"github.com/openkruise/kruise/pkg/util"
	utilcontainerlaunchpriority "github.com/openkruise/kruise/pkg/util/containerlaunchpriority"
	"github.com/openkruise/kruise/pkg/util/lifecycle"
)

func normalizeTemplateMetadataPatch(metadata metav1.ObjectMeta, patch []byte) ([]byte, error) {
	var body map[string]interface{}
	if err := json.Unmarshal(patch, &body); err != nil {
		return nil, err
	}
	if changes, ok := body["metadata"].(map[string]interface{}); ok {
		for field, keys := range map[string]map[string]string{"labels": metadata.Labels, "annotations": metadata.Annotations} {
			if value, exists := changes[field]; exists && value == nil {
				deletions := map[string]interface{}{}
				for key := range keys {
					deletions[key] = nil
				}
				if len(deletions) > 0 {
					changes[field] = deletions
				} else {
					delete(changes, field)
				}
			}
		}
	}
	return json.Marshal(body)
}

func validatePlannedMetadataPatch(pod *v1.Pod, spec *UpdateSpec) error {
	if pod.Annotations[appspub.InPlaceUpdatePlanKey] == "" || len(spec.MetaDataPatch) == 0 {
		return nil
	}
	var body map[string]interface{}
	if err := json.Unmarshal(spec.MetaDataPatch, &body); err != nil {
		return err
	}
	if metadata, exists := body["metadata"]; exists {
		changes, ok := metadata.(map[string]interface{})
		if !ok {
			return fmt.Errorf("planned update cannot delete all Pod metadata")
		}
		for _, field := range []string{"labels", "annotations"} {
			if value, exists := changes[field]; exists && value == nil {
				return fmt.Errorf("planned update requires explicit %s deletion keys", field)
			}
		}
	}
	return nil
}

// PreparedUpdate binds the preview and the effective update spec to the same Pod inputs.
// It must be regenerated after a conflict changes those inputs, but not between execution batches.
type PreparedUpdate struct {
	Plan *appspub.InPlaceUpdatePlan
	Spec *UpdateSpec

	podUID         types.UID
	sourceRevision string
	inputPod       *v1.Pod
	planJSON       string
	hook           *appspub.LifecycleHook
	requirePlan    bool
	replaceOldPlan bool
	generator      func(*v1.Pod, *UpdateSpec) (*appspub.InPlaceUpdatePlan, error)
	previousState  *appspub.InPlaceUpdateState
	previousJSON   string
	checkCompleted func(*v1.Pod) error
}

// PrepareUpdate is pure: it does not publish metadata, execute callbacks, or patch resources.
// A nil preparation means the revision change is not eligible for in-place update.
func PrepareUpdate(pod *v1.Pod, oldRevision, newRevision *apps.ControllerRevision, opts *UpdateOptions) (*PreparedUpdate, error) {
	if pod == nil {
		return nil, fmt.Errorf("cannot prepare an in-place update for a nil Pod")
	}
	options := UpdateOptions{}
	if opts != nil {
		options = *opts
		options.AdditionalFuncs = append([]func(*v1.Pod){}, opts.AdditionalFuncs...)
	}
	SetOptionsDefaults(&options)
	spec := options.CalculateSpec(oldRevision, newRevision, &options)
	if spec == nil {
		return nil, nil
	}
	source, err := revisionWithPendingInputs(pod, oldRevision, spec.Revision)
	if err != nil {
		return nil, err
	}
	if source != oldRevision {
		spec = options.CalculateSpec(source, newRevision, &options)
		if spec == nil {
			return nil, fmt.Errorf("cannot calculate an in-place update including the previous cycle's pending inputs")
		}
	}
	spec, err = copyUpdateSpec(spec)
	if err != nil {
		return nil, err
	}
	generator := options.GeneratePlan
	if generator == nil {
		if reflect.ValueOf(options.PatchSpecToPod).Pointer() != reflect.ValueOf(defaultPatchUpdateSpecToPod).Pointer() {
			return nil, fmt.Errorf("a custom in-place patcher must provide GeneratePlan")
		}
		generator = generateDefaultUpdatePlan
	}
	plan, err := generator(pod.DeepCopy(), spec)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, fmt.Errorf("in-place update planner returned no plan")
	}
	prepared := &PreparedUpdate{Plan: plan, Spec: spec, podUID: pod.UID, inputPod: pod.DeepCopy(), generator: generator, checkCompleted: options.CheckPodUpdateCompleted}
	if raw, exists := appspub.GetInPlaceUpdateState(pod); exists {
		previous := &appspub.InPlaceUpdateState{}
		if err := json.Unmarshal([]byte(raw), previous); err != nil {
			return nil, err
		}
		if previous.Revision != "" && previous.Revision != spec.Revision {
			prepared.previousState, prepared.previousJSON = previous, raw
		}
	}
	if oldRevision != nil {
		prepared.sourceRevision = oldRevision.Name
	}
	plan.PlanID, err = calculatePlanID(pod, spec, plan, prepared.sourceRevision)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	prepared.planJSON = string(encoded)
	return prepared, nil
}

// revisionWithPendingInputs restores only unapplied fields to their actual Pod values.
// Recalculation against the new revision retains eligibility checks and never inherits an obsolete target.
func revisionWithPendingInputs(pod *v1.Pod, revision *apps.ControllerRevision, targetRevision string) (*apps.ControllerRevision, error) {
	raw, exists := appspub.GetInPlaceUpdateState(pod)
	if !exists {
		return revision, nil
	}
	state := appspub.InPlaceUpdateState{}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, err
	}
	if state.Revision == "" || state.Revision == targetRevision {
		return revision, nil
	}
	pending := &UpdateSpec{
		ContainerImages: state.NextContainerImages, InitContainerImages: state.NextInitContainerImages,
		ContainerResources: state.NextContainerResources, ContainerRefMetadata: state.NextContainerRefMetadata,
	}
	if grace, exists := appspub.GetInPlaceUpdateGrace(pod); exists {
		if err := json.Unmarshal([]byte(grace), pending); err != nil {
			return nil, err
		}
	}
	if len(pending.ContainerImages)+len(pending.InitContainerImages)+len(pending.ContainerResources)+len(pending.ContainerRefMetadata)+len(pending.MetaDataPatch) == 0 {
		return revision, nil
	}
	if revision == nil {
		return nil, fmt.Errorf("cannot recover pending update inputs without a source revision")
	}
	template, err := GetTemplateFromRevision(revision)
	if err != nil {
		return nil, err
	}
	specPatch := map[string]interface{}{}
	for index, group := range [][]v1.Container{template.Spec.Containers, template.Spec.InitContainers} {
		containers := []map[string]interface{}{}
		images := pending.ContainerImages
		field := "containers"
		if index == 1 {
			images, field = pending.InitContainerImages, "initContainers"
		}
		for _, container := range group {
			actual := util.GetContainer(container.Name, pod)
			if actual == nil {
				return nil, fmt.Errorf("pending update container %s was not found", container.Name)
			}
			entry := map[string]interface{}{"name": container.Name}
			if _, changed := images[container.Name]; changed {
				entry["image"] = actual.Image
			}
			if resources, changed := pending.ContainerResources[container.Name]; changed {
				resourcePatch := map[string]interface{}{}
				for resourceField, keys := range map[string]v1.ResourceList{"requests": resources.Requests, "limits": resources.Limits} {
					values := map[string]interface{}{}
					actualValues := actual.Resources.Requests
					if resourceField == "limits" {
						actualValues = actual.Resources.Limits
					}
					for key := range keys {
						if value, exists := actualValues[key]; exists {
							values[string(key)] = value
						} else {
							values[string(key)] = nil
						}
					}
					if len(values) > 0 {
						resourcePatch[resourceField] = values
					}
				}
				entry["resources"] = resourcePatch
			}
			if len(entry) > 1 {
				containers = append(containers, entry)
			}
		}
		if len(containers) > 0 {
			specPatch[field] = containers
		}
	}
	metadataPatch := map[string]interface{}{"labels": map[string]interface{}{}, "annotations": map[string]interface{}{}}
	if len(pending.MetaDataPatch) > 0 {
		var patch struct {
			Metadata map[string]map[string]interface{} `json:"metadata"`
		}
		if err := json.Unmarshal(pending.MetaDataPatch, &patch); err != nil {
			return nil, err
		}
		for field, keys := range patch.Metadata {
			if field == "labels" || field == "annotations" {
				if keys == nil {
					return nil, fmt.Errorf("cannot recover legacy grace metadata deletion for %s without its original template keys", field)
				}
				for key := range keys {
					metadataPatch[field].(map[string]interface{})[key] = nil
				}
			}
		}
	}
	for _, metadata := range pending.ContainerRefMetadata {
		for key := range metadata.Labels {
			metadataPatch["labels"].(map[string]interface{})[key] = nil
		}
		for key := range metadata.Annotations {
			metadataPatch["annotations"].(map[string]interface{})[key] = nil
		}
	}
	for field, actual := range map[string]map[string]string{"labels": pod.Labels, "annotations": pod.Annotations} {
		for key := range metadataPatch[field].(map[string]interface{}) {
			if value, exists := actual[key]; exists {
				metadataPatch[field].(map[string]interface{})[key] = value
			}
		}
		if len(metadataPatch[field].(map[string]interface{})) == 0 {
			delete(metadataPatch, field)
		}
	}
	templatePatch := map[string]interface{}{"spec": specPatch}
	if len(metadataPatch) > 0 {
		templatePatch["metadata"] = metadataPatch
	}
	patch, err := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"template": templatePatch}})
	if err != nil {
		return nil, err
	}
	updated := revision.DeepCopy()
	updated.Data.Raw, err = strategicpatch.StrategicMergePatch(revision.Data.Raw, patch, &apps.StatefulSet{})
	return updated, err
}

// selectNextUpdateBatch is shared by the preview and executor. Missing barriers participate in
// the highest remaining ordinary batch; metadata-sharing closure can merge priority groups.
func selectNextUpdateBatch(pod *v1.Pod, spec *UpdateSpec) (sets.String, sets.String) {
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		_, image := spec.InitContainerImages[c.Name]
		_, resources := spec.ContainerResources[c.Name]
		if util.IsRestartableInitContainer(c) && (image || resources) {
			return sets.NewString(c.Name), sets.NewString(c.Name)
		}
	}
	selected := sets.NewString()
	var highest *int
	var highestNames []string
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		_, image := spec.ContainerImages[c.Name]
		_, resources := spec.ContainerResources[c.Name]
		_, metadata := spec.ContainerRefMetadata[c.Name]
		if !image && !resources && !metadata {
			continue
		}
		priority := utilcontainerlaunchpriority.GetContainerPriority(c)
		if priority == nil {
			selected.Insert(c.Name)
		} else if highest == nil || *priority > *highest {
			highest = priority
			highestNames = []string{c.Name}
		} else if *priority == *highest {
			highestNames = append(highestNames, c.Name)
		}
	}
	selected.Insert(highestNames...)
	addMetadataSharedContainersToUpdate(pod, selected, spec.ContainerRefMetadata)
	return selected, sets.NewString()
}

func copyUpdateSpec(spec *UpdateSpec) (*UpdateSpec, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	copied := &UpdateSpec{}
	if err := json.Unmarshal(data, copied); err != nil {
		return nil, err
	}
	return copied, nil
}

func generateDefaultUpdatePlan(pod *v1.Pod, spec *UpdateSpec) (*appspub.InPlaceUpdatePlan, error) {
	pending, err := copyUpdateSpec(spec)
	if err != nil {
		return nil, err
	}
	plan := &appspub.InPlaceUpdatePlan{Batches: []appspub.InPlaceUpdatePlanBatch{}}
	for len(pending.ContainerImages)+len(pending.InitContainerImages)+len(pending.ContainerResources)+len(pending.ContainerRefMetadata) > 0 {
		selected, _ := selectNextUpdateBatch(pod, pending)
		if selected.Len() == 0 {
			return nil, fmt.Errorf("in-place update targets do not match updatable Pod containers")
		}
		batch := appspub.InPlaceUpdatePlanBatch{Containers: []appspub.InPlaceUpdatePlanContainer{}}
		for _, name := range selected.List() {
			container := util.GetContainer(name, pod)
			if container == nil {
				return nil, fmt.Errorf("in-place update container %s was not found", name)
			}
			entry := appspub.InPlaceUpdatePlanContainer{Name: name, Changes: []string{}}
			_, regularImage := pending.ContainerImages[name]
			_, initImage := pending.InitContainerImages[name]
			if regularImage || initImage {
				entry.Changes = append(entry.Changes, appspub.InPlaceUpdateChangeImage)
				// A desired image already in PodSpec does not prove runtime convergence,
				// particularly during rollback or recovery of a partially applied update.
				entry.RestartRequired = true
			}
			if resources, exists := pending.ContainerResources[name]; exists {
				entry.Changes = append(entry.Changes, appspub.InPlaceUpdateChangeResources)
				entry.RestartRequired = entry.RestartRequired || resourceUpdateRequiresRestart(container, resources)
			}
			if _, exists := pending.ContainerRefMetadata[name]; exists {
				entry.Changes = append(entry.Changes, appspub.InPlaceUpdateChangeMetadataEnv)
				entry.RestartRequired = true
			}
			batch.Containers = append(batch.Containers, entry)
			delete(pending.ContainerImages, name)
			delete(pending.InitContainerImages, name)
			delete(pending.ContainerResources, name)
			delete(pending.ContainerRefMetadata, name)
		}
		plan.Batches = append(plan.Batches, batch)
	}
	if len(plan.Batches) == 0 {
		// Metadata-only updates produce an empty container batch in the executor as well.
		plan.Batches = append(plan.Batches, appspub.InPlaceUpdatePlanBatch{Containers: []appspub.InPlaceUpdatePlanContainer{}})
	}
	return plan, nil
}

func resourceUpdateRequiresRestart(container *v1.Container, target v1.ResourceRequirements) bool {
	for _, policy := range container.ResizePolicy {
		if policy.RestartPolicy != v1.RestartContainer {
			continue
		}
		if quantity, exists := target.Requests[policy.ResourceName]; exists {
			old, had := container.Resources.Requests[policy.ResourceName]
			if !had || !old.Equal(quantity) {
				return true
			}
		}
		if quantity, exists := target.Limits[policy.ResourceName]; exists {
			old, had := container.Resources.Limits[policy.ResourceName]
			if !had || !old.Equal(quantity) {
				return true
			}
		}
	}
	return false
}

func calculatePlanID(pod *v1.Pod, spec *UpdateSpec, plan *appspub.InPlaceUpdatePlan, sourceRevision string) (string, error) {
	type containerInput struct {
		Name          string
		Init          bool
		Image         string
		Resources     v1.ResourceRequirements
		ResizePolicy  []v1.ContainerResizePolicy
		RestartPolicy *v1.ContainerRestartPolicy
		Priority      *int
	}
	inputs := []containerInput{}
	for groupIndex, group := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for i := range group {
			c := &group[i]
			inputs = append(inputs, containerInput{
				Name: c.Name, Init: groupIndex == 0, Image: c.Image, Resources: c.Resources,
				ResizePolicy: c.ResizePolicy, RestartPolicy: c.RestartPolicy, Priority: utilcontainerlaunchpriority.GetContainerPriority(c),
			})
		}
	}
	metadataInputs := metav1.ObjectMeta{Labels: map[string]string{}, Annotations: map[string]string{}}
	for _, target := range spec.ContainerRefMetadata {
		for key := range target.Labels {
			if value, exists := pod.Labels[key]; exists {
				metadataInputs.Labels[key] = value
			}
		}
		for key := range target.Annotations {
			if value, exists := pod.Annotations[key]; exists {
				metadataInputs.Annotations[key] = value
			}
		}
	}
	data, err := json.Marshal(struct {
		PodUID         types.UID
		SourceRevision string
		Spec           *UpdateSpec
		Inputs         []containerInput
		Metadata       metav1.ObjectMeta
		Batches        []appspub.InPlaceUpdatePlanBatch
	}{pod.UID, sourceRevision, spec, inputs, metadataInputs, plan.Batches})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

// SyncUpdatePlan publishes or repairs the plan before hook release. Material plan changes
// restore the hook; repeated reconciles of the same plan never restore a released hook.
func SyncUpdatePlan(pod *v1.Pod, prepared *PreparedUpdate, hook *appspub.LifecycleHook, control lifecycle.Interface) (bool, *v1.Pod, error) {
	if prepared == nil || pod == nil {
		return false, pod, nil
	}
	prepared.hook = hook
	state := lifecycle.GetPodLifecycleState(pod)
	prepared.requirePlan = state == appspub.LifecycleStatePreparingUpdate
	switch state {
	case "", appspub.LifecycleStateNormal, appspub.LifecycleStatePreparingNormal,
		appspub.LifecycleStatePreparingUpdate, appspub.LifecycleStateUpdating, appspub.LifecycleStateUpdated:
	default:
		return false, pod, fmt.Errorf("cannot prepare an in-place update in lifecycle state %s", state)
	}
	if state == appspub.LifecycleStateUpdating {
		var executing appspub.InPlaceUpdateState
		if raw, exists := appspub.GetInPlaceUpdateState(pod); exists {
			if err := json.Unmarshal([]byte(raw), &executing); err != nil {
				return false, pod, err
			}
		}
		if executing.Revision == prepared.Spec.Revision {
			return false, pod, nil
		}
		if (executing.UpdatePlan == nil || executing.UpdatePlan.PlanID == "") && prepared.checkCompleted(pod.DeepCopy()) != nil {
			return false, pod, fmt.Errorf("cannot suspend an incomplete in-place update without its original approved plan; finish the current cycle first")
		}
		// A rollback or new target during execution needs a fresh whole-cycle approval.
	}
	if state != appspub.LifecycleStatePreparingUpdate && state != appspub.LifecycleStateUpdated &&
		state != appspub.LifecycleStateUpdating && !lifecycle.IsPodHooked(hook, pod) {
		prepared.replaceOldPlan = true
		return false, pod, nil
	}
	if state == appspub.LifecycleStatePreparingUpdate && pod.Annotations[appspub.InPlaceUpdatePlanKey] == prepared.planJSON {
		return false, pod, nil
	}
	markNotReady := hook != nil && hook.MarkPodNotReady
	return control.UpdatePodLifecycleWithMetadata(pod, appspub.LifecycleStatePreparingUpdate, markNotReady, hook, func(clone *v1.Pod) {
		if clone.Annotations == nil {
			clone.Annotations = map[string]string{}
		}
		clone.Annotations[appspub.InPlaceUpdatePlanKey] = prepared.planJSON
	})
}

// ResumeUpdatePlan cancels a replacement approval when the target returns to the executing revision.
// It restores the original approval without altering its grace period or pending batches.
func ResumeUpdatePlan(pod *v1.Pod, targetRevision string, control lifecycle.Interface) (bool, *v1.Pod, error) {
	if pod == nil || lifecycle.GetPodLifecycleState(pod) != appspub.LifecycleStatePreparingUpdate {
		return false, pod, nil
	}
	raw, exists := appspub.GetInPlaceUpdateState(pod)
	if !exists {
		return false, pod, nil
	}
	state := appspub.InPlaceUpdateState{}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return false, pod, err
	}
	if state.Revision != targetRevision {
		return false, pod, nil
	}
	if state.UpdatePlan == nil || state.UpdatePlan.PlanID == "" {
		return false, pod, fmt.Errorf("cannot resume in-place update %s without its original approved plan", targetRevision)
	}
	plan, err := json.Marshal(state.UpdatePlan)
	if err != nil {
		return false, pod, err
	}
	return control.UpdatePodLifecycleWithMetadata(pod, appspub.LifecycleStateUpdating, false, nil, func(clone *v1.Pod) {
		if clone.Annotations == nil {
			clone.Annotations = map[string]string{}
		}
		clone.Annotations[appspub.InPlaceUpdatePlanKey] = string(plan)
	})
}

func ClearUpdatePlan(pod *v1.Pod, control lifecycle.Interface) (bool, *v1.Pod, error) {
	if pod == nil || pod.Annotations[appspub.InPlaceUpdatePlanKey] == "" {
		return false, pod, nil
	}
	return control.UpdatePodLifecycleWithMetadata(pod, lifecycle.GetPodLifecycleState(pod), false, nil, appspub.RemoveInPlaceUpdatePlan)
}

func isUpdatePlanPreparing(pod *v1.Pod) bool {
	return pod != nil && lifecycle.GetPodLifecycleState(pod) == appspub.LifecycleStatePreparingUpdate
}

func (prepared *PreparedUpdate) validateExecution(pod *v1.Pod, allowAppliedResources bool, opts *UpdateOptions) error {
	if err := prepared.validatePod(pod, allowAppliedResources); err != nil {
		return err
	}
	if prepared.previousState == nil {
		return nil
	}
	raw, _ := appspub.GetInPlaceUpdateState(pod)
	if raw != prepared.previousJSON {
		return fmt.Errorf("previous in-place update state changed; prepare the update again")
	}
	if !allowAppliedResources {
		if err := opts.CheckContainersUpdateCompleted(pod.DeepCopy(), prepared.previousState.DeepCopy()); err != nil {
			return fmt.Errorf("previous in-place update batch has not converged: %w", err)
		}
	}
	return nil
}

func (prepared *PreparedUpdate) validatePod(pod *v1.Pod, allowAppliedResources bool) error {
	if pod == nil || pod.UID != prepared.podUID {
		return fmt.Errorf("in-place update plan Pod UID changed")
	}
	if value := pod.Annotations[appspub.InPlaceUpdatePlanKey]; !prepared.replaceOldPlan &&
		(value != "" || prepared.requirePlan) && value != prepared.planJSON {
		return fmt.Errorf("in-place update plan changed; prepare the update again")
	}
	if prepared.hook != nil && lifecycle.IsPodHooked(prepared.hook, pod) {
		return fmt.Errorf("in-place update lifecycle hook is still blocked")
	}
	input := pod.DeepCopy()
	if allowAppliedResources {
		// A successful resize can precede an UpdatePod conflict. Accept only already-applied
		// target quantities, then validate every other input against the original preparation.
		for _, group := range [][]v1.Container{input.Spec.Containers, input.Spec.InitContainers} {
			for i := range group {
				c := &group[i]
				original := util.GetContainer(c.Name, prepared.inputPod)
				if original == nil {
					continue
				}
				if target, exists := prepared.Spec.ContainerResources[c.Name]; exists {
					for key, quantity := range target.Requests {
						if actual, found := c.Resources.Requests[key]; found && actual.Equal(quantity) {
							c.Resources.Requests[key] = original.Resources.Requests[key]
						}
					}
					for key, quantity := range target.Limits {
						if actual, found := c.Resources.Limits[key]; found && actual.Equal(quantity) {
							c.Resources.Limits[key] = original.Resources.Limits[key]
						}
					}
				}
			}
		}
	}
	plan, err := prepared.generator(input, prepared.Spec)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("in-place update planner returned no plan")
	}
	id, err := calculatePlanID(input, prepared.Spec, plan, prepared.sourceRevision)
	if err != nil {
		return err
	}
	if id != prepared.Plan.PlanID {
		return fmt.Errorf("in-place update plan inputs changed; prepare the update again")
	}
	return nil
}

// A plan may be re-armed while a cached Pod is being processed. The resize write must reject
// that stale version before touching resources, not only when the later metadata update runs.
func guardResourcePatch(pod *v1.Pod, patch []byte) ([]byte, error) {
	if pod.Annotations[appspub.InPlaceUpdatePlanKey] == "" {
		return patch, nil
	}
	var body map[string]interface{}
	if err := json.Unmarshal(patch, &body); err != nil {
		return nil, err
	}
	metadata, _ := body["metadata"].(map[string]interface{})
	if metadata == nil {
		metadata = map[string]interface{}{}
		body["metadata"] = metadata
	}
	if pod.ResourceVersion != "" {
		metadata["resourceVersion"] = pod.ResourceVersion
	}
	if pod.UID != "" {
		metadata["uid"] = pod.UID
	}
	return json.Marshal(body)
}

func applyResizeResult(pod, resized *v1.Pod) {
	if pod.Annotations[appspub.InPlaceUpdatePlanKey] == "" || resized == nil {
		return
	}
	pod.ResourceVersion = resized.ResourceVersion
	for _, group := range [][]v1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		for i := range group {
			if actual := util.GetContainer(group[i].Name, resized); actual != nil {
				group[i].Resources = *actual.Resources.DeepCopy()
			}
		}
	}
}
