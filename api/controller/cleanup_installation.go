package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/goodrain/rainbond/config/configs"
	"github.com/goodrain/rainbond/db/model"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	"github.com/goodrain/rainbond/pkg/component/k8s"
	httputil "github.com/goodrain/rainbond/util/http"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var cleanupPluginResource = schema.GroupVersionResource{Group: "rainbond.io", Version: "v1alpha1", Resource: "rbdplugins"}

type cleanupInstallationRequest struct {
	Enterprise     string `json:"enterprise"`
	Region         string `json:"region"`
	PluginID       string `json:"plugin_id"`
	AppID          string `json:"app_id"`
	ConsoleURL     string `json:"console_url"`
	ConsolePodName string `json:"console_pod_name,omitempty"`
	ConsolePodUID  string `json:"console_pod_uid,omitempty"`
}

func installationConsoleAddress(ctx context.Context, client kubernetes.Interface, namespace string, request cleanupInstallationRequest) (string, error) {
	if request.ConsolePodName != "" && request.ConsolePodUID != "" {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, request.ConsolePodName, metav1.GetOptions{})
		if err == nil && string(pod.UID) == request.ConsolePodUID && pod.DeletionTimestamp == nil && pod.Labels["name"] == "rbd-app-ui" {
			service, err := client.CoreV1().Services(namespace).Get(ctx, "rbd-app-ui", metav1.GetOptions{})
			if err == nil && service.DeletionTimestamp == nil && len(service.Spec.Selector) > 0 && labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(pod.Labels)) {
				for _, port := range service.Spec.Ports {
					if port.Port == 7070 {
						return fmt.Sprintf("http://rbd-app-ui.%s.svc.cluster.local:%d", namespace, port.Port), nil
					}
				}
			}
		}
	}
	origin, err := url.Parse(request.ConsoleURL)
	if err != nil || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") || (origin.Scheme != "http" && origin.Scheme != "https") {
		return "", kubeidentity.ErrBinding
	}
	return request.ConsoleURL, nil
}

func systemCleanupInstallationTarget() (kubernetes.Interface, dynamic.Interface, string, error) {
	component, configuration := k8s.Default(), configs.Default()
	if component == nil || component.RestConfig == nil || configuration == nil || configuration.PublicConfig == nil || configuration.PublicConfig.RbdNamespace == "" {
		return nil, nil, "", kubeidentity.ErrBinding
	}
	config := cleanupReferenceRestConfig(component.RestConfig)
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, "", err
	}
	resources, err := dynamic.NewForConfig(config)
	return client, resources, configuration.PublicConfig.RbdNamespace, err
}

func installationStorageClass(ctx context.Context, client kubernetes.Interface, namespace string) string {
	for _, name := range []string{"rbd-hub", "rbd-app-ui-data"} {
		claim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil && claim.Spec.StorageClassName != nil && *claim.Spec.StorageClassName != "" {
			return *claim.Spec.StorageClassName
		}
	}
	classes, err := client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return ""
	}
	selected := ""
	var latest metav1.Time
	for _, class := range classes.Items {
		if class.Annotations["storageclass.kubernetes.io/is-default-class"] != "true" && class.Annotations["storageclass.beta.kubernetes.io/is-default-class"] != "true" {
			continue
		}
		if selected == "" || latest.Before(&class.CreationTimestamp) {
			selected, latest = class.Name, class.CreationTimestamp
		}
	}
	return selected
}

// PrepareCleanupInstallation is called by Console before the plugin is first
// deployed. Tenant/application ownership is checked before any resource write.
func (h *CleanupCoordinationHandler) PrepareCleanupInstallation(w http.ResponseWriter, r *http.Request) {
	var request cleanupInstallationRequest
	if !coordinationDecode(w, r, &request) {
		return
	}
	pluginName := strings.ToLower(request.PluginID)
	if (pluginName != "rainbond-enterprise-base" && pluginName != "rainbond-enterprise-base-amd64" && pluginName != "rainbond-enterprise-base-arm64") || !registryPodUID.MatchString(request.AppID) || !nodeScopeIdentifier(request.Enterprise) || !nodeScopeIdentifier(request.Region) {
		httputil.ReturnError(r, w, http.StatusBadRequest, "INVALID_CLEANUP_INSTALLATION")
		return
	}
	if h.installationTarget == nil || h.database == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_UNAVAILABLE")
		return
	}
	client, resources, namespace, err := h.installationTarget()
	if err != nil || client == nil || resources == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_UNAVAILABLE")
		return
	}
	plugin, err := resources.Resource(cleanupPluginResource).Get(r.Context(), pluginName, metav1.GetOptions{})
	if err != nil || plugin.GetDeletionTimestamp() != nil || string(plugin.GetUID()) == "" || plugin.GetLabels()["app_id"] != request.AppID {
		httputil.ReturnError(r, w, http.StatusForbidden, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	var app model.Application
	if err := h.database().Where("app_id = ?", request.AppID).First(&app).Error; err != nil || app.EID != request.Enterprise {
		httputil.ReturnError(r, w, http.StatusForbidden, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	var tenant model.Tenants
	if err := h.database().Where("uuid = ?", app.TenantID).First(&tenant).Error; err != nil || tenant.EID != request.Enterprise {
		httputil.ReturnError(r, w, http.StatusForbidden, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	pluginNamespace, ok, err := unstructured.NestedString(plugin.Object, "spec", "namespace")
	if err != nil || !ok || (tenant.Namespace != pluginNamespace && !(tenant.Namespace == "" && tenant.Name == pluginNamespace)) {
		httputil.ReturnError(r, w, http.StatusForbidden, "CLEANUP_INSTALLATION_OWNER_MISMATCH")
		return
	}
	backend, ok, err := unstructured.NestedString(plugin.Object, "spec", "backend_service")
	if err != nil || !ok {
		httputil.ReturnError(r, w, http.StatusBadRequest, "INVALID_CLEANUP_INSTALLATION")
		return
	}
	consoleAddress, err := installationConsoleAddress(r.Context(), client, namespace, request)
	if err != nil {
		httputil.ReturnError(r, w, http.StatusBadRequest, "INVALID_CLEANUP_INSTALLATION")
		return
	}
	console, _ := url.Parse(consoleAddress)
	scope := kubeidentity.InstallationScope{Protocol: 1, Enterprise: request.Enterprise, Region: request.Region, Namespace: pluginNamespace, BackendService: backend, ConsoleURL: consoleAddress, AllowHTTP: console.Scheme == "http", StatePerNode: true, StateStorageClass: installationStorageClass(r.Context(), client, namespace)}
	prepared, err := kubeidentity.EnsureInstallationResources(r.Context(), client, namespace, scope)
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_PREPARING")
		return
	}
	owner, _ := json.Marshal(map[string]string{"plugin": pluginName, "pluginUID": string(plugin.GetUID()), "appID": request.AppID})
	config, err := client.CoreV1().ConfigMaps(namespace).Get(r.Context(), kubeidentity.InstallationConfigName, metav1.GetOptions{})
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_PREPARING")
		return
	}
	if config.Data["owner.json"] != string(owner) {
		config.Data["owner.json"] = string(owner)
		if _, err := client.CoreV1().ConfigMaps(namespace).Update(r.Context(), config, metav1.UpdateOptions{}); err != nil {
			httputil.ReturnError(r, w, http.StatusServiceUnavailable, "CLEANUP_INSTALLATION_PREPARING")
			return
		}
	}
	httputil.ReturnSuccess(r, w, map[string]interface{}{"protocol": 1, "phase": "prepared", "scope": prepared, "serviceAccount": "rainbond-enterprise-base", "stateClaim": "enterprise-base-disk-state"})
}

func nodeScopeIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, ch := range value {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}
