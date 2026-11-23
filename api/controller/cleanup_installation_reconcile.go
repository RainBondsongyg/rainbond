package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/docker/distribution/reference"
	guard "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	httputil "github.com/goodrain/rainbond/util/http"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var cleanupComponentResource = schema.GroupVersionResource{Group: "rainbond.io", Version: "v1alpha1", Resource: "rbdcomponents"}
var errCleanupPlatformUpgrade = errors.New("cleanup installation requires a compatible platform")

func ownedCleanupInstallation(ctx context.Context, client kubernetes.Interface, resources dynamic.Interface, namespace string) (*corev1.ConfigMap, kubeidentity.InstallationScope, error) {
	var scope kubeidentity.InstallationScope
	config, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, kubeidentity.InstallationConfigName, metav1.GetOptions{})
	if err != nil {
		return nil, scope, err
	}
	var owner struct {
		Plugin    string `json:"plugin"`
		PluginUID string `json:"pluginUID"`
		AppID     string `json:"appID"`
	}
	if json.Unmarshal([]byte(config.Data["installation.json"]), &scope) != nil || scope.Protocol != 1 || json.Unmarshal([]byte(config.Data["owner.json"]), &owner) != nil || owner.PluginUID == "" {
		return nil, scope, kubeidentity.ErrBinding
	}
	plugin, err := resources.Resource(cleanupPluginResource).Get(ctx, owner.Plugin, metav1.GetOptions{})
	if err != nil {
		return nil, scope, err
	}
	pluginNamespace, _, _ := unstructured.NestedString(plugin.Object, "spec", "namespace")
	backend, _, _ := unstructured.NestedString(plugin.Object, "spec", "backend_service")
	if plugin.GetDeletionTimestamp() != nil || string(plugin.GetUID()) != owner.PluginUID || plugin.GetLabels()["app_id"] != owner.AppID || pluginNamespace != scope.Namespace || cleanupInstallationBackendAddress(backend) != scope.BackendService {
		return nil, scope, kubeidentity.ErrBinding
	}
	return config, scope, nil
}

func installationRegistryPod(ctx context.Context, client kubernetes.Interface, namespace, name string) (*corev1.Pod, kubeidentity.RegistryPreparation, error) {
	denied := kubeidentity.RegistryPreparation{}
	service, err := client.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || len(service.Spec.Selector) == 0 {
		return nil, denied, kubeidentity.ErrExecutorStarting
	}
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String()})
	if err != nil {
		return nil, denied, err
	}
	var source *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning {
			if source != nil {
				return nil, denied, kubeidentity.ErrExecutorStarting
			}
			source = pod
		}
	}
	if source == nil {
		return nil, denied, kubeidentity.ErrExecutorStarting
	}
	prepared, err := kubeidentity.InspectNativeRegistry(ctx, client, namespace, name, source.Name, string(source.UID))
	return source, prepared, err
}

func setInstallationEnv(component *unstructured.Unstructured, values map[string]string) {
	items, _, _ := unstructured.NestedSlice(component.Object, "spec", "env")
	remaining := make(map[string]string, len(values))
	for key, value := range values {
		remaining[key] = value
	}
	for i, raw := range items {
		entry, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := entry["name"].(string)
		if value, found := remaining[name]; found {
			items[i] = map[string]interface{}{"name": name, "value": value}
			delete(remaining, name)
		}
	}
	// Fixed order avoids repeated rollouts caused by map iteration order.
	for _, name := range []string{"LOCAL_CLEANUP_ENABLED", "REGISTRY_HTTP_ADDR", "REGISTRY_STORAGE", "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", "REGISTRY_STORAGE_MAINTENANCE_UPLOADPURGING_ENABLED"} {
		if value, found := remaining[name]; found {
			items = append(items, map[string]interface{}{"name": name, "value": value})
		}
	}
	unstructured.SetNestedSlice(component.Object, items, "spec", "env")
}

func installationOperatorSupportsCoordination(ctx context.Context, resources dynamic.Interface) bool {
	crds := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	crd, err := resources.Resource(crds).Get(ctx, "rbdcomponents.rainbond.io", metav1.GetOptions{})
	if err != nil {
		return false
	}
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	for _, raw := range versions {
		version, ok := raw.(map[string]interface{})
		if !ok || version["name"] != "v1alpha1" || version["served"] != true {
			continue
		}
		_, found, _ := unstructured.NestedMap(version, "schema", "openAPIV3Schema", "properties", "spec", "properties", "registryCoordination")
		if found {
			return true
		}
	}
	return false
}

func reconcileInstallationComponents(ctx context.Context, client kubernetes.Interface, resources dynamic.Interface, namespace string, config *corev1.ConfigMap, scope kubeidentity.InstallationScope, source *corev1.Pod, prepared kubeidentity.RegistryPreparation, binding guard.StorageRegistration) error {
	components := resources.Resource(cleanupComponentResource).Namespace(namespace)
	hub, err := components.Get(ctx, "rbd-hub", metav1.GetOptions{})
	if err != nil {
		return errCleanupPlatformUpgrade
	}
	previous := hub.DeepCopy()
	coordination, existing, err := unstructured.NestedMap(hub.Object, "spec", "registryCoordination")
	if err != nil {
		return kubeidentity.ErrBinding
	}
	if existing {
		for name, value := range map[string]string{"enterpriseID": scope.Enterprise, "regionName": scope.Region, "storageID": binding.StorageID, "generation": binding.Generation, "volumeUID": binding.VolumeUID, "registryPath": binding.RootPath} {
			if coordination[name] != value {
				return kubeidentity.ErrBinding
			}
		}
	} else {
		if !installationOperatorSupportsCoordination(ctx, resources) {
			return errCleanupPlatformUpgrade
		}
		coordination = map[string]interface{}{"controlSecret": "rainbond-disk-system-control", "permitSecret": "rainbond-disk-registry-permit"}
	}
	image, _ := coordination["image"].(string)
	if image == "" {
		cluster, err := resources.Resource(cleanupClusterResource).Namespace(namespace).Get(ctx, "rainbondcluster", metav1.GetOptions{})
		if err != nil {
			return errCleanupPlatformUpgrade
		}
		repository, _, _ := unstructured.NestedString(cluster.Object, "spec", "rainbondImageRepository")
		version, _, _ := unstructured.NestedString(cluster.Object, "spec", "installVersion")
		if repository == "" {
			repository = "registry.cn-hangzhou.aliyuncs.com/goodrain"
		}
		if version == "" {
			return errCleanupPlatformUpgrade
		}
		candidate := strings.TrimSuffix(repository, "/") + "/rbd-registry-gc:" + version
		// Reuse a resolved release digest after its probe Job expires.
		if config.Data["helper-candidate"] == candidate {
			image = config.Data["helper-image"]
		}
		if image == "" {
			image, err = kubeidentity.EnsureInstallationImage(ctx, client, namespace, candidate, source.Spec.NodeName)
			if err != nil {
				return err
			}
		}
		config.Data["helper-candidate"], config.Data["helper-image"] = candidate, image
	}
	parsed, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return kubeidentity.ErrBinding
	}
	if _, ok := parsed.(reference.Canonical); !ok {
		return kubeidentity.ErrBinding
	}
	cluster, err := resources.Resource(cleanupClusterResource).Namespace(namespace).Get(ctx, "rainbondcluster", metav1.GetOptions{})
	if err == nil {
		domain, _, _ := unstructured.NestedString(cluster.Object, "spec", "imageHub", "domain")
		if (&url.URL{Host: reference.Domain(parsed)}).Hostname() == (&url.URL{Host: domain}).Hostname() {
			return kubeidentity.ErrBinding
		}
	}
	if reference.Domain(parsed) == "goodrain.me" || strings.HasPrefix(reference.Domain(parsed), "rbd-hub.") || reference.Domain(parsed) == "rbd-hub" {
		return kubeidentity.ErrBinding
	}
	nativeImage := ""
	for _, container := range source.Spec.Containers {
		if container.Name != prepared.Container {
			continue
		}
		for _, status := range source.Status.ContainerStatuses {
			if status.Name == container.Name {
				nativeImage, err = kubeidentity.RuntimeImage(container, status)
				if err != nil {
					return err
				}
			}
		}
	}
	if nativeImage == "" {
		return kubeidentity.ErrExecutorStarting
	}
	control, _ := coordination["controlSecret"].(string)
	permit, _ := coordination["permitSecret"].(string)
	if err := kubeidentity.EnsureInstallationSystemSecrets(ctx, client, namespace, scope, control, permit, existing); err != nil {
		return err
	}
	config.Data["system-secret"], config.Data["permit-secret"] = control, permit
	updated, err := client.CoreV1().ConfigMaps(namespace).Update(ctx, config, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	*config = *updated
	for name, value := range map[string]interface{}{"image": image, "consoleURL": scope.ConsoleURL, "enterpriseID": scope.Enterprise, "regionName": scope.Region, "storageID": binding.StorageID, "generation": binding.Generation, "volumeUID": binding.VolumeUID, "registryPath": binding.RootPath, "allowHTTP": scope.AllowHTTP} {
		coordination[name] = value
	}
	unstructured.SetNestedMap(hub.Object, coordination, "spec", "registryCoordination")
	unstructured.SetNestedField(hub.Object, nativeImage, "spec", "image")
	setInstallationEnv(hub, map[string]string{"REGISTRY_HTTP_ADDR": "127.0.0.1:5000", "REGISTRY_STORAGE": "filesystem", "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY": prepared.Root, "REGISTRY_STORAGE_MAINTENANCE_UPLOADPURGING_ENABLED": "false"})
	if !reflect.DeepEqual(previous.Object["spec"], hub.Object["spec"]) {
		if _, err := components.Update(ctx, hub, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	for _, name := range []string{"rbd-worker", "rbd-chaos", "rbd-api"} {
		component, err := components.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return errCleanupPlatformUpgrade
		}
		previous := component.DeepCopy()
		setInstallationEnv(component, map[string]string{"LOCAL_CLEANUP_ENABLED": "false"})
		if name == "rbd-chaos" {
			args, _, _ := unstructured.NestedStringSlice(component.Object, "spec", "args")
			filtered := []string{}
			for i := 0; i < len(args); i++ {
				flag := strings.SplitN(args[i], "=", 2)[0]
				if flag == "--clean-up" || flag == "-clean-up" {
					if !strings.Contains(args[i], "=") && i+1 < len(args) && (args[i+1] == "true" || args[i+1] == "false") {
						i++
					}
					continue
				}
				filtered = append(filtered, args[i])
			}
			unstructured.SetNestedStringSlice(component.Object, append(filtered, "--clean-up=false"), "spec", "args")
		}
		if !reflect.DeepEqual(previous.Object["spec"], component.Object["spec"]) {
			if _, err := components.Update(ctx, component, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReconcileCleanupInstallation accepts no runtime overrides. The signed plugin
// can reconcile only the installation already approved and recorded by Console.
func (h *CleanupCoordinationHandler) ReconcileCleanupInstallation(w http.ResponseWriter, r *http.Request) {
	var empty struct{}
	if !coordinationDecode(w, r, &empty) {
		return
	}
	if h.installationTarget == nil || h.gcTarget == nil || h.database == nil {
		httputil.ReturnError(r, w, 503, "CLEANUP_INSTALLATION_UNAVAILABLE")
		return
	}
	client, resources, namespace, err := h.installationTarget()
	if err != nil || client == nil || resources == nil {
		httputil.ReturnError(r, w, 503, "CLEANUP_INSTALLATION_UNAVAILABLE")
		return
	}
	config, scope, err := ownedCleanupInstallation(r.Context(), client, resources, namespace)
	if err != nil {
		httputil.ReturnError(r, w, 403, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	phase, code := "initializing", "CLEANUP_INSTALLATION_PREPARING"
	var view struct {
		Binding guard.StorageRegistration `json:"binding"`
	}
	if json.Unmarshal([]byte(config.Data["registry-view.json"]), &view) != nil {
		httputil.ReturnError(r, w, 503, code)
		return
	}
	_, registryNamespace, name, err := h.gcTarget()
	if err != nil || registryNamespace != namespace {
		httputil.ReturnError(r, w, 503, code)
		return
	}
	source, prepared, err := installationRegistryPod(r.Context(), client, namespace, name)
	if err == nil && (view.Binding.VolumeUID != prepared.Mount.VolumeUID || view.Binding.RootPath != prepared.Root) {
		err = kubeidentity.ErrBinding
	}
	if err == nil {
		_, err = guard.StorageBinding(h.database(), view.Binding.StorageID, view.Binding.Generation)
	}
	if err == nil {
		err = reconcileInstallationComponents(r.Context(), client, resources, namespace, config, scope, source, prepared, view.Binding)
	}
	if errors.Is(err, errCleanupPlatformUpgrade) {
		phase, code = "unsupported", "CLEANUP_PLATFORM_UPGRADE_REQUIRED"
	} else if err != nil && !errors.Is(err, kubeidentity.ErrExecutorStarting) {
		phase, code = "error", "CLEANUP_INSTALLATION_RETRYING"
	}
	if err == nil {
		_, nodeErr := kubeidentity.InstalledNodeSettings(r.Context(), client, namespace)
		coordinatorReady := false
		for _, status := range source.Status.ContainerStatuses {
			if status.Name == "registry-coordinator" && status.Ready && status.State.Running != nil {
				coordinatorReady = true
			}
		}
		if coordinatorReady && nodeErr == nil && h.requireReferenceWriterCoverage(r.Context()) == nil {
			phase, code = "ready", ""
		}
	}
	httputil.ReturnSuccess(r, w, map[string]interface{}{"protocol": 1, "phase": phase, "code": code})
}
