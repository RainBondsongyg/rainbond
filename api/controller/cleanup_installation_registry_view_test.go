package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goodrain/rainbond/db/model"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	"github.com/jinzhu/gorm"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
)

// capability_id: rainbond.cleanup.installation-registry-view
func TestInstallationRegistryViewUsesOwnedTargetResources(t *testing.T) {
	database, err := gorm.Open("sqlite3", filepath.Join(t.TempDir(), "installation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.AutoMigrate(&model.CleanupStorage{}).Error; err != nil {
		t.Fatal(err)
	}
	client, _, _ := gcAdmissionFixture(t)
	ctx := context.Background()
	hub, _ := client.CoreV1().Pods("system").Get(ctx, "hub", metav1.GetOptions{})
	hub.Status.Phase = corev1.PodRunning
	client.CoreV1().Pods("system").Update(ctx, hub, metav1.UpdateOptions{})
	pv, _ := client.CoreV1().PersistentVolumes().Get(ctx, "pv", metav1.GetOptions{})
	pv.Spec.NFS = &corev1.NFSVolumeSource{Server: "nfs.test", Path: "/registry"}
	pv.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
	client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	scope, _ := json.Marshal(kubeidentity.InstallationScope{Protocol: 1, Enterprise: "enterprise", Region: "region", Namespace: "plugins", BackendService: "http://base.plugins.svc:8080"})
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: kubeidentity.InstallationConfigName, Namespace: "system"}, Data: map[string]string{"installation.json": string(scope), "owner.json": `{"plugin":"rainbond-enterprise-base","pluginUID":"installed-plugin","appID":"app"}`}}
	client.CoreV1().ConfigMaps("system").Create(ctx, config, metav1.CreateOptions{})
	credential := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "rbd-hub-credentials", Namespace: "system"}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{"registry.test":{"username":"fixture-user","password":"public-test-password"}}}`)}}
	client.CoreV1().Secrets("system").Create(ctx, credential, metav1.CreateOptions{})
	plugin := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rainbond.io/v1alpha1", "kind": "RBDPlugin", "metadata": map[string]interface{}{"name": "rainbond-enterprise-base", "uid": "installed-plugin", "labels": map[string]interface{}{"app_id": "app"}}, "spec": map[string]interface{}{"namespace": "plugins", "backend_service": "base.plugins.svc:8080"}}}
	cluster := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rainbond.io/v1alpha1", "kind": "RainbondCluster", "metadata": map[string]interface{}{"name": "rainbondcluster", "namespace": "system"}, "spec": map[string]interface{}{"imageHub": map[string]interface{}{"domain": "registry.test"}}}}
	resources := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), plugin, cluster)
	h := &CleanupCoordinationHandler{database: func() *gorm.DB { return database }, installationTarget: func() (kubernetes.Interface, dynamic.Interface, string, error) {
		return client, resources, "system", nil
	}, gcTarget: func() (kubernetes.Interface, string, string, error) { return client, "system", "rbd-hub", nil }}
	post := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		h.PrepareCleanupRegistryView(r, httptest.NewRequest(http.MethodPost, "/v2/cleanup/installations/registry-view", strings.NewReader(body)))
		return r
	}
	for i := 0; i < 2; i++ {
		r := post("{}")
		if r.Code != http.StatusOK || !strings.Contains(r.Body.String(), "rainbond-disk-registry-") || !strings.Contains(r.Body.String(), "http://rbd-hub.system.svc.cluster.local:5000") || strings.Contains(r.Body.String(), "public-test-password") {
			t.Fatal("target Registry references were not prepared", r.Code, r.Body.String())
		}
		if !strings.Contains(r.Body.String(), "storage_id") || !strings.Contains(r.Body.String(), "generation") {
			t.Fatal("missing persistent Registry binding")
		}
	}
	if r := post(`{"url":"https://foreign.test"}`); r.Code != http.StatusBadRequest {
		t.Fatal("caller Registry override was accepted", r.Code)
	}
	unstructured.SetNestedField(plugin.Object, "other", "spec", "namespace")
	resources.Resource(cleanupPluginResource).Update(ctx, plugin, metav1.UpdateOptions{})
	if r := post("{}"); r.Code != http.StatusForbidden {
		t.Fatal("changed plugin scope was accepted", r.Code)
	}
}
