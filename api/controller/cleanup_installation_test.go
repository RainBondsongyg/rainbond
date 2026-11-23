package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/go-chi/chi"
	"github.com/goodrain/rainbond/api/middleware"
	"github.com/goodrain/rainbond/db/model"
	"github.com/jinzhu/gorm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.installation-api
func TestCleanupInstallationRequiresAnOwnedRegisteredPlugin(t *testing.T) {
	database, err := gorm.Open("sqlite3", filepath.Join(t.TempDir(), "installation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.DB().SetMaxOpenConns(1)
	if err := database.AutoMigrate(&model.Tenants{}, &model.Application{}).Error; err != nil {
		t.Fatal(err)
	}
	database.Create(&model.Tenants{UUID: "tenant", EID: "enterprise", Namespace: "plugins", Name: "team"})
	database.Create(&model.Application{AppID: "app", TenantID: "tenant", EID: "enterprise"})
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "plugins"}})
	plugin := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rainbond.io/v1alpha1", "kind": "RBDPlugin", "metadata": map[string]interface{}{"name": "rainbond-enterprise-base", "uid": "installed-plugin", "labels": map[string]interface{}{"app_id": "app"}}, "spec": map[string]interface{}{"namespace": "plugins", "backend_service": "base.plugins.svc:8080"}}}
	resources := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), plugin)
	h := &CleanupCoordinationHandler{database: func() *gorm.DB { return database }, installationTarget: func() (kubernetes.Interface, dynamic.Interface, string, error) {
		return client, resources, "system", nil
	}}
	router := chi.NewRouter()
	router.Use(middleware.CleanupIdentity)
	router.Post("/v2/cleanup/installations", h.PrepareCleanupInstallation)
	t.Setenv("TOKEN", "installation-test-fixture")
	post := func(body map[string]interface{}, authorized bool) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/v2/cleanup/installations", bytes.NewReader(raw))
		if authorized {
			req.Header.Set("Authorization", "Token installation-test-fixture")
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	body := map[string]interface{}{"enterprise": "enterprise", "region": "region", "plugin_id": "rainbond-enterprise-base", "app_id": "app", "console_url": "https://console.example"}
	if response := post(body, false); response.Code != http.StatusUnauthorized && response.Code != http.StatusForbidden {
		t.Fatal("unauthenticated installation", response.Code)
	}
	if response := post(body, true); response.Code != http.StatusOK {
		t.Fatal("owned plugin installation failed", response.Code, response.Body.String())
	}
	control, _ := client.CoreV1().Secrets("system").Get(context.Background(), "rainbond-disk-node-control", metav1.GetOptions{})
	key := append([]byte(nil), control.Data["key"]...)
	if response := post(body, true); response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), key) {
		t.Fatal("installation changed or exposed a credential", response.Code)
	}
	before := len(client.Actions())
	body["enterprise"] = "different-enterprise"
	if response := post(body, true); response.Code != http.StatusForbidden {
		t.Fatal("cross-enterprise installation accepted", response.Code)
	}
	for _, action := range client.Actions()[before:] {
		if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "patch" {
			t.Fatal("foreign request changed platform resources")
		}
	}
}

func TestInstallationConsoleAddressUsesInternalServiceOnlyForItsActualPod(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "console-0", Namespace: "system", UID: "actual-console", Labels: map[string]string{"name": "rbd-app-ui"}}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "rbd-app-ui", Namespace: "system"}, Spec: corev1.ServiceSpec{Selector: pod.Labels, Ports: []corev1.ServicePort{{Port: 7070, Protocol: corev1.ProtocolTCP}}}}
	client := fake.NewSimpleClientset(pod, service)
	request := cleanupInstallationRequest{ConsoleURL: "https://public.console.example", ConsolePodName: pod.Name, ConsolePodUID: "actual-console"}
	address, err := installationConsoleAddress(ctx, client, "system", request)
	if err != nil || address != "http://rbd-app-ui.system.svc.cluster.local:7070" {
		t.Fatal("same-cluster Console was not discovered automatically", address, err)
	}
	request.ConsolePodUID = "different-cluster-console"
	address, err = installationConsoleAddress(ctx, client, "system", request)
	if err != nil || address != request.ConsoleURL {
		t.Fatal("remote region was sent to an unrelated local Console", address, err)
	}
}
