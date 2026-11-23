package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/goodrain/rainbond/config/configs"
	guard "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	httputil "github.com/goodrain/rainbond/util/http"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var cleanupClusterResource = schema.GroupVersionResource{Group: "rainbond.io", Version: "v1alpha1", Resource: "rainbondclusters"}

// PrepareCleanupRegistryView prepares private read access to the target registry.
// No caller-provided URL, credential, volume or filesystem path is accepted.
func (h *CleanupCoordinationHandler) PrepareCleanupRegistryView(w http.ResponseWriter, r *http.Request) {
	var empty struct{}
	if !coordinationDecode(w, r, &empty) {
		return
	}
	if h.installationTarget == nil || h.gcTarget == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_UNAVAILABLE")
		return
	}
	client, resources, namespace, err := h.installationTarget()
	if err != nil || client == nil || resources == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_UNAVAILABLE")
		return
	}
	config, err := client.CoreV1().ConfigMaps(namespace).Get(r.Context(), kubeidentity.InstallationConfigName, metav1.GetOptions{})
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_PREPARING")
		return
	}
	var scope kubeidentity.InstallationScope
	var owner struct {
		Plugin    string `json:"plugin"`
		PluginUID string `json:"pluginUID"`
		AppID     string `json:"appID"`
	}
	if json.Unmarshal([]byte(config.Data["installation.json"]), &scope) != nil || json.Unmarshal([]byte(config.Data["owner.json"]), &owner) != nil {
		httputil.ReturnError(r, w, http.StatusForbidden, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	plugin, err := resources.Resource(cleanupPluginResource).Get(r.Context(), owner.Plugin, metav1.GetOptions{})
	if err != nil || plugin.GetDeletionTimestamp() != nil || string(plugin.GetUID()) != owner.PluginUID || plugin.GetLabels()["app_id"] != owner.AppID {
		httputil.ReturnError(r, w, http.StatusForbidden, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	pluginNamespace, _, _ := unstructured.NestedString(plugin.Object, "spec", "namespace")
	backend, _, _ := unstructured.NestedString(plugin.Object, "spec", "backend_service")
	if pluginNamespace != scope.Namespace || cleanupInstallationBackendAddress(backend) != scope.BackendService {
		httputil.ReturnError(r, w, http.StatusForbidden, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	_, registryNamespace, registryName, err := h.gcTarget()
	if err != nil || registryNamespace != namespace {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_UNAVAILABLE")
		return
	}
	service, err := client.CoreV1().Services(namespace).Get(r.Context(), registryName, metav1.GetOptions{})
	if err != nil || len(service.Spec.Selector) == 0 {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_UNAVAILABLE")
		return
	}
	pods, err := client.CoreV1().Pods(namespace).List(r.Context(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String()})
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_UNAVAILABLE")
		return
	}
	var source *corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning {
			if source != nil {
				httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_PREPARING")
				return
			}
			source = pod
		}
	}
	if source == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_PREPARING")
		return
	}
	prepared, err := kubeidentity.InspectNativeRegistry(r.Context(), client, namespace, registryName, source.Name, string(source.UID))
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_PREPARING")
		return
	}
	mount, err := kubeidentity.EnsureRegistryReadMount(r.Context(), client, source, prepared, scope.Namespace)
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_STORAGE_PREPARING")
		return
	}
	domain := ""
	cluster, err := resources.Resource(cleanupClusterResource).Namespace(namespace).Get(r.Context(), "rainbondcluster", metav1.GetOptions{})
	if err == nil {
		domain, _, _ = unstructured.NestedString(cluster.Object, "spec", "imageHub", "domain")
	}
	if domain == "" && configs.Default() != nil && configs.Default().ServerConfig != nil {
		if endpoint, err := url.Parse(configs.Default().ServerConfig.RbdHub); err == nil {
			domain = endpoint.Host
		}
	}
	secret, err := kubeidentity.EnsureRegistryAuthSecret(r.Context(), client, namespace, domain, scope)
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_AUTH_PREPARING")
		return
	}
	port := int32(0)
	for _, entry := range service.Spec.Ports {
		if entry.Port == 5000 {
			port = entry.Port
		}
	}
	if port == 0 && len(service.Spec.Ports) == 1 {
		port = service.Spec.Ports[0].Port
	}
	if port == 0 {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_UNAVAILABLE")
		return
	}
	if h.database == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_PREPARING")
		return
	}
	binding, err := guard.ProvisionRegistryStorage(h.database(), prepared.Mount.VolumeUID, prepared.Root)
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_PREPARING")
		return
	}
	result := map[string]interface{}{"protocol": 1, "mount": mount, "binding": binding, "authSecret": secret, "url": fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", registryName, namespace, port), "aliases": []string{domain}}
	encoded, _ := json.Marshal(result)
	if config.Data["registry-view.json"] != string(encoded) {
		config.Data["registry-view.json"] = string(encoded)
		if _, err := client.CoreV1().ConfigMaps(namespace).Update(r.Context(), config, metav1.UpdateOptions{}); err != nil {
			httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_REGISTRY_PREPARING")
			return
		}
	}
	httputil.ReturnSuccess(r, w, result)
}
