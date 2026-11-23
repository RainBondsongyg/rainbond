package kubeidentity

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.node-state-provisioning
func TestNodeStateClaimsAreAutomaticallyProvisionedForTheirActualNodes(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "uid-a"}}, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", UID: "uid-b"}})
	settings := NodeJobSettings{StateClaim: "rainbond-disk-node-state", StatePerNode: true, StateStorageClass: "local-path"}
	a, err := EnsureNodeStateClaim(ctx, client, "system", "node-a", "uid-a", settings)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		name, err := EnsureNodeStateClaim(ctx, client, "system", "node-a", "uid-a", settings)
		if err != nil || name != a {
			t.Fatal("journal changed during repeated installation or plugin upgrade", name, err)
		}
	}
	b, err := EnsureNodeStateClaim(ctx, client, "system", "node-b", "uid-b", settings)
	if err != nil || a == b {
		t.Fatal("different nodes shared a read-write-once journal", a, b, err)
	}
	claims, _ := client.CoreV1().PersistentVolumeClaims("system").List(ctx, metav1.ListOptions{})
	if len(claims.Items) != 2 {
		t.Fatal("duplicate journal claims", len(claims.Items))
	}
	pvc, _ := client.CoreV1().PersistentVolumeClaims("system").Get(ctx, a, metav1.GetOptions{})
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "local-path" || pvc.Annotations["volume.kubernetes.io/selected-node"] != "node-a" || len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Fatal("journal cannot bind to its intended node", pvc.Spec)
	}
	if _, err := EnsureNodeStateClaim(ctx, client, "system", "node-a", "replacement-uid", settings); err == nil {
		t.Fatal("replaced node accepted under the old identity")
	}
	pvc.Annotations["cleanup.rainbond.io/node-uid"] = "foreign"
	client.CoreV1().PersistentVolumeClaims("system").Update(ctx, pvc, metav1.UpdateOptions{})
	if _, err := EnsureNodeStateClaim(ctx, client, "system", "node-a", "uid-a", settings); err == nil {
		t.Fatal("foreign journal was adopted")
	}
	settings.StatePerNode = false
	legacy, err := EnsureNodeStateClaim(ctx, nil, "system", "node-a", "uid-a", settings)
	if err != nil || legacy != settings.StateClaim {
		t.Fatal("legacy journal changed before migration", legacy, err)
	}
}
