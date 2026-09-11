# PodUnavailableBudget 与 pkg/util 的 init 容器感知缺陷

> 状态：**缺陷点 1、2 已修复（2026-09-11，master 基线）；缺陷点 3 移入 SidecarSet 链路问题（见 §四）**
>
> 记录时间：2026-09-10
> 关联 feature：`InPlaceUpdateRestartableInitContainer`（restartable init container 原地更新镜像）
> 基线：修复基于 upstream `master`（`cfed56d8`，PUB 已提升为 `policy/v1beta1`）；如需回填 downstream，注意其基线是 `v1.9.1`（PUB 仍为 `policy/v1alpha1`）

## 为什么单独记录

这两处缺陷是在为 `InPlaceUpdateRestartableInitContainer` 做全仓扫描时发现的，但结论是：

1. **不是本 feature 引入的**。`allDigestImage` 提前 return 绕过 `DefaultCheckInPlaceUpdateCompleted` 的问题，在 digest 格式镜像 + SidecarSet 升级场景下早已存在。
2. **不影响本 feature 的功能正确性**。两处缺陷的方向都是"误报一致（false-positive consistent）"，不存在"误报不一致"，因此永远不会阻塞或挂起原地更新。
3. 因此应作为一个**自洽的 bugfix PR** 单独提交，标题聚焦 PUB 缺陷而非 init 容器，评审更快，也不让 feature PR 变得难 review。

## 一、影响面评估（已实测结论）

### 1.1 两条调用链的方向不同

**链路 A — 准入校验：不是漏洞，误报"一致"反而更保守**

```go
// pkg/control/pubcontrol/pub_control_utils.go:70-74
	// If the pod is not ready or state is inconsistent, it doesn't count towards healthy and we should not decrement
} else if !PubControl.IsPodReady(pod) || !PubControl.IsPodStateConsistent(pod) {
	klog.V(3).InfoS("Pod was not ready or state was inconsistent, then didn't need check pub", ...)
	return true, "", nil
}
```

语义与直觉相反：判定"不一致"时 `return true`（放行、跳过配额检查），理由是该 Pod 本就不健康。
所以误报"一致"会让代码**继续**走配额检查并扣减 —— 偏保守，无安全问题。

**链路 B — 可用数统计：真实风险，但窗口很窄**

```go
// pkg/controller/podunavailablebudget/podunavailablebudget_controller.go:380-383
	// pod consistent and ready
	if pubcontrol.PubControl.IsPodStateConsistent(pod) && pubcontrol.PubControl.IsPodReady(pod) {
		currentAvailable++
	}
```

误报"一致"使 `currentAvailable` 虚高 → PUB 允许破坏更多 Pod。

但受 `&& IsPodReady(pod)` 保护：kruise 在 patch Pod spec **之前**就把 `InPlaceUpdateReady` 置为 False 并调用
`SetPodReadyCondition`（`pkg/util/inplaceupdate/inplace_update.go:341-358` → `updateCondition:199-207`），
直到 `Refresh` 确认完成才置回 True（`inplace_update.go:178-189`）。
因此"Pod Ready 为真 **且** 正在原地更新"这个交集在带 readiness gate 的工作负载上基本不存在。

> 注：OpenKruise Game 的 GameServerSet 一定注入该 readiness gate
> （`kruise-game/pkg/util/gameserver.go:139-142`），所以 GSS/ASTS 场景天然被屏蔽。

### 1.2 触发前置条件

链路 B 要真正出问题，需要**同时**满足：

- 存在匹配该 Pod 的 PUB 对象（Pod 带 `kruise.io/related-pub` 注解）
- Pod 的**所有** `spec.containers[*].image` 都是 digest 格式（`repo@sha256:...`），
  使 `allDigestImage` 为 true 从而提前 return
- Pod 的 Ready condition 在原地更新期间为 true（即工作负载**未**配置 `InPlaceUpdateReady` readiness gate）

三条同时成立时，正在原地更新 native sidecar 的 Pod 会被 PUB 误算为"可用"。

## 二、缺陷点 1：`IsPodStateConsistent` 跳过 init 容器且提前 return

**位置**：`pkg/control/pubcontrol/pub_control.go:286-324`

```go
func (c *commonControl) IsPodStateConsistent(pod *corev1.Pod) bool {
	// if all container image is digest format
	// by comparing status.containers[x].ImageID with spec.container[x].Image can determine whether pod is consistent
	allDigestImage := true
	for _, container := range pod.Spec.Containers {        // ← 缺陷 (a)：不含 InitContainers
		if !util.IsImageDigest(container.Image) {
			allDigestImage = false
			continue
		}
		if !util.IsPodContainerDigestEqual(sets.NewString(container.Name), pod) {
			return false
		}
	}
	// If all spec.container[x].image is digest format, only check digest imageId
	if allDigestImage {
		return true                                        // ← 缺陷 (b)：提前 return
	}

	// check whether injected sidecar container is consistent
	sidecarSets, sidecars := getSidecarSetsInPod(pod)
	if sidecarSets.Len() > 0 && sidecars.Len() > 0 {
		if !sidecarcontrol.IsSidecarContainerUpdateCompleted(pod, sidecarSets, sidecars) {
			return false
		}
	}

	// whether other containers is consistent
	if err := inplaceupdate.DefaultCheckInPlaceUpdateCompleted(pod); err != nil {
		return false
	}

	return true
}
```

### 缺陷 (a)：循环只覆盖 `pod.Spec.Containers`

restartable init container（native sidecar）的 image/imageID 一致性完全不被检查。

### 缺陷 (b)：`allDigestImage` 提前 return —— 这是更严重的一个

若业务容器全部使用 digest 格式镜像，函数直接返回 `true`，**永远不会**执行到下面两个检查：

- `sidecarcontrol.IsSidecarContainerUpdateCompleted(...)`
- `inplaceupdate.DefaultCheckInPlaceUpdateCompleted(pod)` ← **这个函数在本 feature 改造后已经是 init 感知的**

```go
// pkg/util/inplaceupdate/inplace_update_defaults.go:657-663（本 feature 已改造）
	if err := checkStatuses(pod.Status.ContainerStatuses); err != nil {
		return err
	}
	// Restartable init containers (native sidecar containers) report their status here.
	if err := checkStatuses(pod.Status.InitContainerStatuses); err != nil {
		return err
	}
```

也就是说：**修好 (b) 即可自动获得 init 容器的完整一致性判定**，因为下游函数已经就绪。
这也是建议把该 PR 定性为"修复 `allDigestImage` 提前 return"而非"增加 init 容器支持"的原因。

### 修复方向

- 把 (b) 的提前 `return true` 改为"仅表示 digest 快路径判定通过"，继续执行后续的
  `IsSidecarContainerUpdateCompleted` 与 `DefaultCheckInPlaceUpdateCompleted`；
  或至少在存在 restartable init container 时不走快路径。
- (a) 的循环额外遍历 `pod.Spec.InitContainers` 并用 `util.IsRestartableInitContainer` 过滤
  （普通 init 容器跑完即退出，不应参与一致性判定）。
- 注意两个缺陷必须**同时**修：只修 (a) 而 (b) 仍提前 return，则 digest 场景依旧漏检；
  只修 (b) 而 (a) 不动，则 digest 快路径里 init 容器仍不被检查。

## 三、缺陷点 2：`pkg/util/pods.go` 底层工具不感知 init 容器

**位置**：`pkg/util/pods.go:257-291`

```go
func GetPodContainerImageIDs(pod *v1.Pod) map[string]string {
	cImageIDs := make(map[string]string, len(pod.Status.ContainerStatuses))
	for i := range pod.Status.ContainerStatuses {        // ← 不含 InitContainerStatuses
		c := &pod.Status.ContainerStatuses[i]
		imageID := c.ImageID
		if strings.Contains(imageID, "://") {
			imageID = strings.Split(imageID, "://")[1]
		}
		cImageIDs[c.Name] = imageID
	}
	return cImageIDs
}

func IsPodContainerDigestEqual(containers sets.String, pod *v1.Pod) bool {
	cImageIDs := GetPodContainerImageIDs(pod)

	for _, container := range pod.Spec.Containers {      // ← 不含 InitContainers
		if !containers.Has(container.Name) {
			continue
		}
		...
	}
	return true                                          // ← 传入 init 容器名时循环体一次都不执行
}
```

### 重要修正：当前**不可触发**

全仓扫描初稿称"传 init 容器名会静默返回 true，是可触发的误判"。经核实**不成立**，所有调用方都传不进 init 容器名：

| 调用方 | 名字来源 | 结论 |
|---|---|---|
| `pkg/control/pubcontrol/pub_control.go:298` | `for _, container := range pod.Spec.Containers` 的 `container.Name` | 恒为普通容器名 |
| `pkg/control/sidecarcontrol/sidecarset_control.go:154`（用 `GetPodContainerImageIDs`） | 循环 `pod.Spec.Containers`，过滤集来自只含 `sidecarSet.Spec.Containers` 的 `GetSidecarContainersInPod` | 恒为普通容器名 |

因此本缺陷点的**性质是"等着被踩的陷阱"，而不是当前的 bug**：
一旦有人（**包括修复缺陷点 1 的人**）把 init 容器名传进去，就会拿到静默的 `true`。

### 修复方向

- 新增 `GetPodContainerImageIDsIncludingInit`（与已有的 `GetContainerStatusIncludingInit` 命名保持一致），
  或让现有函数合并 `pod.Status.InitContainerStatuses`。
- `IsPodContainerDigestEqual` 遍历 `InitContainers` + `Containers`；
  更稳妥的做法是：当 `containers` 集合里存在任何在 Pod spec 中找不到的名字时返回 `false`（fail-closed），
  避免"名字找不到 → 静默判定一致"这类静默误判再次出现。
- **必须与缺陷点 1 同一个 PR 提交**，否则修了上层等于没修。

## 四、缺陷点 3（附带）：`getSidecarSetsInPod` 漏扫 init 容器

**位置**：`pkg/control/pubcontrol/pub_control.go:347-363`

```go
	for _, container := range pod.Spec.Containers {      // ← 只扫 spec.containers 的 IS_INJECTED
		val := util.GetContainerEnvValue(&container, sidecarcontrol.SidecarEnvKey)
		if val == "true" {
			containers.Insert(container.Name)
		}
	}
```

注入到 `spec.initContainers` 的 sidecar 同样带 `IS_INJECTED=true` env，但这里扫不到，
导致 `pub_control.go:309-315` 调用 `IsSidecarContainerUpdateCompleted(pod, sidecarSets, sidecars)` 时
`sidecars` 集合缺少 native sidecar，PUB 无法感知其升级中状态。

### 修正：单独修本缺陷是**无效代码**，已移出本次修复范围

收集到的 init sidecar 名字会作为 `containers` 集合传入
`sidecarcontrol.IsSidecarContainerUpdateCompleted(pod, sidecarSets, sidecars)`，
但该函数只遍历 `pod.Status.ContainerStatuses`（`sidecarset_control.go:249`），
init 容器的名字永远不会被检查。也就是说只修这里，行为没有任何变化。

有效修复需要 `IsSidecarContainerUpdateCompleted` 同时感知 `InitContainerStatuses`，
而这与 SidecarSet 对 init sidecar 的升级链路（hash 已计入、升级写不回 `pod.Spec.InitContainers`）
是同一个问题，因此本缺陷点**并入 P1 的 SidecarSet 链路问题**，单独开 issue/PR 处理。

## 五、附带：需要更新的过期注释

### 5.1 `pkg/control/pubcontrol/pub_control.go:218-231`

```go
func podSpecWithoutResourcesHash(podIn *corev1.Pod) (string, error) {
	pod := podIn.DeepCopy()
	// do not need to process init-containers because they wont change     ← 注释已过期
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Resources = corev1.ResourceRequirements{}
	}
	...
}
```

注释 "init-containers ... wont change" 在本 feature 下已不成立。

**功能上目前是保守安全的**：init 容器 image 变更会使 `podSpecWithoutResourcesHash` 不一致
→ `CanResizeInplace` 返回 false → 走完整 PUB 保护，符合预期。
但注释误导性强，且若将来支持 init 容器 resize 就会出错。

### 5.2 `pkg/control/pubcontrol/pub_control.go:130-133` `canInplaceUpdateResources`

```go
	if len(oldPod.Spec.Containers) != len(newPod.Spec.Containers) {
		return false
	}
```

同上，属于"保守安全但语义待明确"，建议补注释说明只覆盖普通容器。

## 六、建议的 PR 拆分

| PR | 内容 | 优先级 |
|---|---|---|
| PR-1（当前） | `InPlaceUpdateRestartableInitContainer` feature 本体 + DaemonSet 镜像预热 + CRR 打通 | 进行中 |
| PR-2 | 本文档的缺陷点 1 + 2（PUB 一致性判定 + `pods.go` 工具 init 感知） | **已完成（2026-09-11，master）** |
| PR-3 | SidecarSet 对 `spec.initContainers` 注入的 sidecar 全链路误算（升级静默丢失） | 上游既有 bug，先开 issue 讨论 |

## 七、给 PR-2 的测试要点

1. 所有业务容器使用 digest 镜像 + 存在正在原地更新的 restartable init container
   → `IsPodStateConsistent` 必须返回 `false`（当前返回 `true`）
2. 所有业务容器使用 digest 镜像 + 无任何更新中容器 → 仍返回 `true`（不引入回归）
3. 业务容器使用 tag 镜像（`allDigestImage == false`）→ 行为与修复前一致
4. `IsPodContainerDigestEqual` 传入 restartable init container 名 → 能正确比对 imageID
5. `IsPodContainerDigestEqual` 传入 Pod spec 中不存在的容器名 → 返回 `false`（fail-closed）
6. `getSidecarSetsInPod` 能识别注入到 `spec.initContainers` 且带 `IS_INJECTED=true` 的 sidecar

## 八、对 downstream 分支的处置

当前 `downstream/release-1.9` 自用环境**无需立即修复**，实测依据：

- 集群内无任何 PUB 对象（`kubectl get pub -A` → `No resources found`）
- `ipregion-0/1` 的 `kruise.io/related-pub` 注解为空
  → `PodUnavailableBudgetValidatePod` 在 `pub == nil` 分支直接返回 true，
  `IsPodStateConsistent` 的返回值被丢弃
- 业务镜像均为 tag 格式（如 `server-resource:v0.2.9`），`allDigestImage` 恒为 false，
  会正常落到已 init 感知的 `DefaultCheckInPlaceUpdateCompleted`

**若将来 downstream 环境开始使用 PUB 且改用 digest 格式镜像，必须先 cherry-pick PR-2。**
