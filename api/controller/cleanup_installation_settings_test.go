package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.automatic-node-settings
func TestAutomaticNodeSettingsReplaceLegacyPinnedImageAfterInstallation(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	legacy := kubeidentity.NodeJobSettings{Region: "region", Image: "images.example/old@sha256:" + strings.Repeat("a", 64)}
	settings, automatic, err := installedOrLegacyNodeSettings(ctx, client, "system", legacy)
	if err != nil || automatic || settings != legacy {
		t.Fatal("existing explicit installation changed before migration", settings, automatic, err)
	}
	scope := kubeidentity.InstallationScope{Protocol: 1, Enterprise: "enterprise", Region: "region", Namespace: "plugins", BackendService: "http://base.plugins.svc:8080", StateClaim: "state", CredentialSecret: "control", ConsoleURL: "http://console.system.svc:7070", AllowHTTP: true}
	raw, _ := json.Marshal(scope)
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: kubeidentity.InstallationConfigName, Namespace: "system"}, Data: map[string]string{"installation.json": string(raw)}}
	client.CoreV1().ConfigMaps("system").Create(ctx, config, metav1.CreateOptions{})
	client.CoreV1().Services("plugins").Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "base", Namespace: "plugins"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"name": "base"}}}, metav1.CreateOptions{})
	client.CoreV1().Pods("plugins").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "base-0", Namespace: "plugins", Labels: map[string]string{"name": "base"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "base", Image: "images.example/base:two"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "base", ImageID: "images.example/base@sha256:" + strings.Repeat("b", 64)}}}}, metav1.CreateOptions{})
	settings, automatic, err = installedOrLegacyNodeSettings(ctx, client, "system", legacy)
	if err != nil || !automatic || settings.Image != "images.example/base@sha256:"+strings.Repeat("b", 64) {
		t.Fatal("old manual image still overrides the installed upgrade", settings, automatic, err)
	}
	config.Data["installation.json"] = "{}"
	client.CoreV1().ConfigMaps("system").Update(ctx, config, metav1.UpdateOptions{})
	if _, _, err = installedOrLegacyNodeSettings(ctx, client, "system", legacy); err == nil {
		t.Fatal("invalid managed configuration silently used the obsolete image")
	}
}
