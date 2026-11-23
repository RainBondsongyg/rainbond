package kubeidentity

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.installation-registry-mount
func TestInstallationMirrorsOnlyTheObservedRegistryVolumeReadOnly(t *testing.T) {
	ctx := context.Background()
	service, pod, pvc, pv := bindingObjects()
	pod.Spec.Containers = pod.Spec.Containers[:1]
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", Value: "/var/lib/registry"}}
	pv.Spec.HostPath = &corev1.HostPathVolumeSource{Path: "/data/registry"}
	pv.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
	client := fake.NewSimpleClientset(service, pod, pvc, pv, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", Labels: map[string]string{"kubernetes.io/hostname": "node"}}})
	prepared, err := InspectNativeRegistry(ctx, client, "system", service.Name, pod.Name, string(pod.UID))
	if err != nil {
		t.Fatal(err)
	}
	mount, err := EnsureRegistryReadMount(ctx, client, pod, prepared, "plugins")
	if err != nil || mount.Claim == "" || mount.SubPath != "warehouse/docker/registry/v2" || mount.NodeSelector["kubernetes.io/hostname"] != "node" {
		t.Fatal("read mount was not derived from the target Registry", mount, err)
	}
	again, err := EnsureRegistryReadMount(ctx, client, pod, prepared, "plugins")
	if err != nil || again.Claim != mount.Claim {
		t.Fatal("repeated install changed Registry storage", again, err)
	}
	claim, _ := client.CoreV1().PersistentVolumeClaims("plugins").Get(ctx, mount.Claim, metav1.GetOptions{})
	mirror, _ := client.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
	if mirror.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain || len(mirror.Spec.AccessModes) != 1 || mirror.Spec.AccessModes[0] != corev1.ReadOnlyMany || mirror.Spec.HostPath.Path != pv.Spec.HostPath.Path {
		t.Fatal("read mirror can change or reclaim the Registry source")
	}
	if len(mirror.Spec.MountOptions) != 0 {
		t.Fatal("hostPath mirror received unsupported filesystem mount options")
	}
	before, _ := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	if before.Spec.ClaimRef.UID != pvc.UID {
		t.Fatal("source Registry claim was changed")
	}
	pv.UID = "replacement-source"
	client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	if _, err := EnsureRegistryReadMount(ctx, client, pod, prepared, "other"); err == nil {
		t.Fatal("stale Registry volume identity was reused")
	}
}
