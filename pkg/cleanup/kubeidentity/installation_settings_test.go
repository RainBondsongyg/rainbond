package kubeidentity

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.installed-node-settings
func TestInstalledNodeSettingsFollowTheRunningPluginImage(t *testing.T) {
	ctx := context.Background()
	scope := InstallationScope{Protocol: 1, Enterprise: "enterprise", Region: "region", Namespace: "plugins", BackendService: "http://base.plugins.svc.cluster.local:8080", StateClaim: "node-state", CredentialSecret: "node-control", ConsoleURL: "http://console.system.svc:7070", AllowHTTP: true}
	raw, _ := json.Marshal(scope)
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: InstallationConfigName, Namespace: "system"}, Data: map[string]string{"installation.json": string(raw)}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "base", Namespace: "plugins"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"name": "base"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "base-0", Namespace: "plugins", UID: "plugin-pod", Labels: service.Spec.Selector}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "base", Image: "images.example/base:one"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "base", ImageID: "images.example/base@sha256:" + strings.Repeat("a", 64)}}}}
	client := fake.NewSimpleClientset(config, service, pod)
	settings, err := InstalledNodeSettings(ctx, client, "system")
	if err != nil || settings.Region != "region" || settings.Image != pod.Status.ContainerStatuses[0].ImageID || settings.StateClaim != "node-state" || settings.CredentialSecret != "node-control" || settings.ConsoleEnterprise != "enterprise" || !settings.AllowConsoleHTTP {
		t.Fatal("installation did not provide complete node settings", settings, err)
	}
	pod.Spec.Containers[0].Image = "images.example/base:two"
	pod.Status.ContainerStatuses[0].ImageID = "containerd://sha256:" + strings.Repeat("b", 64)
	client.CoreV1().Pods("plugins").Update(ctx, pod, metav1.UpdateOptions{})
	settings, err = InstalledNodeSettings(ctx, client, "system")
	if err != nil || settings.Image != "images.example/base@sha256:"+strings.Repeat("b", 64) {
		t.Fatal("upgraded executor image stayed pinned to the previous release", settings, err)
	}
	pod.Status.ContainerStatuses[0].ImageID = "different.example/foreign@sha256:" + strings.Repeat("b", 64)
	client.CoreV1().Pods("plugins").Update(ctx, pod, metav1.UpdateOptions{})
	if _, err := InstalledNodeSettings(ctx, client, "system"); err == nil {
		t.Fatal("foreign runtime image accepted")
	}
	pod.Status.ContainerStatuses[0].ImageID = "containerd://sha256:" + strings.Repeat("b", 64)
	client.CoreV1().Pods("plugins").Update(ctx, pod, metav1.UpdateOptions{})
	replica := pod.DeepCopy()
	replica.Name, replica.UID = "base-1", "second-plugin-pod"
	replica.Spec.Containers[0].Image = "images.example/base:one"
	replica.Status.ContainerStatuses[0].ImageID = "images.example/base@sha256:" + strings.Repeat("a", 64)
	client.CoreV1().Pods("plugins").Create(ctx, replica, metav1.CreateOptions{})
	if _, err := InstalledNodeSettings(ctx, client, "system"); err != ErrExecutorStarting {
		t.Fatal("mixed plugin revisions were selected during an upgrade", err)
	}
	client.CoreV1().Pods("plugins").Delete(ctx, replica.Name, metav1.DeleteOptions{})
	scope.BackendService = "http://base.other.svc:8080"
	raw, _ = json.Marshal(scope)
	config.Data["installation.json"] = string(raw)
	client.CoreV1().ConfigMaps("system").Update(ctx, config, metav1.UpdateOptions{})
	if _, err := InstalledNodeSettings(ctx, client, "system"); err == nil {
		t.Fatal("foreign namespace selected by the installation")
	}
}
