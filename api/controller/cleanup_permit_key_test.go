package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.registry-permit-key-separation
func TestRegistryPermitKeyUsesDedicatedFileAndNeverFallsBackOnFailure(t *testing.T) {
	legacy := bytes.Repeat([]byte("l"), 32)
	dedicated := bytes.Repeat([]byte("p"), 32)
	t.Setenv("TOKEN", string(legacy))
	t.Setenv("CLEANUP_REGISTRY_PERMIT_KEY_FILE", "")
	if !bytes.Equal(systemRegistryPermitKey(), legacy) {
		t.Fatal("lost explicit legacy compatibility")
	}
	path := filepath.Join(t.TempDir(), "permit")
	if err := os.WriteFile(path, dedicated, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLEANUP_REGISTRY_PERMIT_KEY_FILE", path)
	if !bytes.Equal(systemRegistryPermitKey(), dedicated) {
		t.Fatal("did not use independent permit key")
	}
	for _, bad := range [][]byte{[]byte("short"), bytes.Repeat([]byte("x"), 8193), append(bytes.Repeat([]byte("x"), 32), ' ', 'x')} {
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if len(systemRegistryPermitKey()) != 0 {
			t.Fatal("invalid configured key fell back to administrator token")
		}
	}
	t.Setenv("CLEANUP_REGISTRY_PERMIT_KEY_FILE", path+".missing")
	if len(systemRegistryPermitKey()) != 0 {
		t.Fatal("missing configured key fell back")
	}
}

func TestManagedPermitKeyUsesClusterReferenceWithoutAPIMounts(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	if _, configured := managedRegistryPermitKey(ctx, client, "system"); configured {
		t.Fatal("legacy installation was forced into managed mode")
	}
	client.CoreV1().ConfigMaps("system").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: kubeidentity.InstallationConfigName, Namespace: "system"}, Data: map[string]string{"permit-secret": "permit"}}, metav1.CreateOptions{})
	if key, configured := managedRegistryPermitKey(ctx, client, "system"); !configured || len(key) != 0 {
		t.Fatal("missing managed key allowed a legacy fallback")
	}
	wanted := bytes.Repeat([]byte("public-fixture"), 4)
	client.CoreV1().Secrets("system").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "permit", Namespace: "system"}, Data: map[string][]byte{"key": wanted}}, metav1.CreateOptions{})
	if key, configured := managedRegistryPermitKey(ctx, client, "system"); !configured || !bytes.Equal(key, wanted) {
		t.Fatal("managed key did not resolve")
	}
}
