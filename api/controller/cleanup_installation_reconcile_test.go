package controller

import (
	"bytes"
	"context"
	"strings"
	"testing"

	guard "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// capability_id: rainbond.cleanup.installation-coordinator
func TestInstallationCoordinatorAdoptsOwnedConfigurationAndDisablesLegacyCleaners(t *testing.T) {
	ctx := context.Background()
	client, _, volume := gcAdmissionFixture(t)
	pod, _ := client.CoreV1().Pods("system").Get(ctx, "hub", metav1.GetOptions{})
	pod.Spec.NodeName = "node"
	pod.Spec.Containers[0].Image = pod.Status.ContainerStatuses[0].ImageID
	client.CoreV1().Pods("system").Update(ctx, pod, metav1.UpdateOptions{})
	prepared, err := kubeidentity.InspectNativeRegistry(ctx, client, "system", "rbd-hub", "hub", "hub-uid")
	if err != nil {
		t.Fatal(err)
	}
	binding := guard.StorageRegistration{StorageID: "store", Generation: "original-generation", VolumeUID: volume, RootPath: prepared.Root}
	scope := kubeidentity.InstallationScope{Enterprise: "enterprise", Region: "region", ConsoleURL: "https://console.test"}
	components := schema.GroupVersionResource{Group: "rainbond.io", Version: "v1alpha1", Resource: "rbdcomponents"}
	objects := []runtime.Object{}
	for _, name := range []string{"rbd-api", "rbd-worker", "rbd-chaos", "rbd-hub"} {
		objects = append(objects, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "rainbond.io/v1alpha1", "kind": "RbdComponent", "metadata": map[string]interface{}{"name": name, "namespace": "system"}, "spec": map[string]interface{}{"env": []interface{}{map[string]interface{}{"name": "BUSINESS_SETTING", "value": "keep"}}}}})
	}
	resources := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...)
	hub, _ := resources.Resource(components).Namespace("system").Get(ctx, "rbd-hub", metav1.GetOptions{})
	configuration := map[string]interface{}{"image": "release.test/rbd-registry-gc@sha256:" + strings.Repeat("a", 64), "enterpriseID": scope.Enterprise, "regionName": scope.Region, "controlSecret": "old-control", "permitSecret": "old-permit", "storageID": binding.StorageID, "generation": binding.Generation, "volumeUID": binding.VolumeUID, "registryPath": binding.RootPath}
	unstructured.SetNestedMap(hub.Object, configuration, "spec", "registryCoordination")
	resources.Resource(components).Namespace("system").Update(ctx, hub, metav1.UpdateOptions{})
	for _, name := range []string{"old-control", "old-permit"} {
		client.CoreV1().Secrets("system").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "system"}, Data: map[string][]byte{"key": bytes.Repeat([]byte(name), 8)}}, metav1.CreateOptions{})
	}
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: kubeidentity.InstallationConfigName, Namespace: "system"}, Data: map[string]string{}}
	client.CoreV1().ConfigMaps("system").Create(ctx, config, metav1.CreateOptions{})
	for i := 0; i < 2; i++ {
		if err := reconcileInstallationComponents(ctx, client, resources, "system", config, scope, pod, prepared, binding); err != nil {
			t.Fatal("owned installation could not be reconciled", err)
		}
	}
	hub, _ = resources.Resource(components).Namespace("system").Get(ctx, "rbd-hub", metav1.GetOptions{})
	generation, _, _ := unstructured.NestedString(hub.Object, "spec", "registryCoordination", "generation")
	if generation != binding.Generation || config.Data["system-secret"] != "old-control" || config.Data["permit-secret"] != "old-permit" {
		t.Fatal("upgrade replaced existing coordination identity")
	}
	for _, name := range []string{"rbd-api", "rbd-worker", "rbd-chaos"} {
		component, _ := resources.Resource(components).Namespace("system").Get(ctx, name, metav1.GetOptions{})
		envs, _, _ := unstructured.NestedSlice(component.Object, "spec", "env")
		disabled, kept := false, false
		for _, value := range envs {
			e := value.(map[string]interface{})
			disabled = disabled || e["name"] == "LOCAL_CLEANUP_ENABLED" && e["value"] == "false"
			kept = kept || e["name"] == "BUSINESS_SETTING" && e["value"] == "keep"
		}
		if !disabled || !kept {
			t.Fatal("legacy cleaner was not disabled without losing business settings", name)
		}
	}
	binding.Generation = "replacement"
	if err := reconcileInstallationComponents(ctx, client, resources, "system", config, scope, pod, prepared, binding); err == nil {
		t.Fatal("active storage generation was overwritten")
	}
	// An older Operator must be detected before configuring unsupported fields.
	unstructured.RemoveNestedField(hub.Object, "spec", "registryCoordination")
	resources.Resource(components).Namespace("system").Update(ctx, hub, metav1.UpdateOptions{})
	if err := reconcileInstallationComponents(ctx, client, resources, "system", config, scope, pod, prepared, binding); err != errCleanupPlatformUpgrade {
		t.Fatal("old Operator was not reported as an explicit upgrade requirement", err)
	}
}
