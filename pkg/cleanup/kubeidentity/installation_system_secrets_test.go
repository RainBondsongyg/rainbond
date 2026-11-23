package kubeidentity

import (
	"bytes"
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInstallationSystemCredentialsAreIndependentAndSurviveUpgrade(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	scope := InstallationScope{Enterprise: "enterprise", Region: "region"}
	if err := EnsureInstallationSystemSecrets(ctx, client, "system", scope, "system-control", "permit", false); err != nil {
		t.Fatal(err)
	}
	control, _ := client.CoreV1().Secrets("system").Get(ctx, "system-control", metav1.GetOptions{})
	permit, _ := client.CoreV1().Secrets("system").Get(ctx, "permit", metav1.GetOptions{})
	if len(control.Data["key"]) < 32 || bytes.Equal(control.Data["key"], permit.Data["key"]) {
		t.Fatal("system credentials are not independent")
	}
	if err := EnsureInstallationSystemSecrets(ctx, client, "system", scope, "system-control", "permit", false); err != nil {
		t.Fatal(err)
	}
	after, _ := client.CoreV1().Secrets("system").Get(ctx, "permit", metav1.GetOptions{})
	if !bytes.Equal(after.Data["key"], permit.Data["key"]) {
		t.Fatal("upgrade rotated an active permit key")
	}
	legacy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: "system"}, Data: map[string][]byte{"key": bytes.Repeat([]byte("fixture"), 8)}}
	client.CoreV1().Secrets("system").Create(ctx, legacy, metav1.CreateOptions{})
	if err := EnsureInstallationSystemSecrets(ctx, client, "system", scope, "legacy", "permit", false); err == nil {
		t.Fatal("unproven legacy credential adopted")
	}
	if err := EnsureInstallationSystemSecrets(ctx, client, "system", scope, "legacy", "permit", true); err != nil {
		t.Fatal("owned coordinator could not be adopted", err)
	}
	if err := EnsureInstallationSystemSecrets(ctx, client, "system", scope, "permit", "permit", false); err == nil {
		t.Fatal("shared control/permit credential accepted")
	}
}
