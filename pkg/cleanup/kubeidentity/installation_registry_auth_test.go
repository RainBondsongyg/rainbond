package kubeidentity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.installation-registry-auth
func TestInstallationProjectsTargetRegistryCredentialsWithoutReturningThem(t *testing.T) {
	ctx := context.Background()
	auth := base64.StdEncoding.EncodeToString([]byte("fixture-user:fixture-password"))
	raw, _ := json.Marshal(map[string]interface{}{"auths": map[string]interface{}{"goodrain.me": map[string]string{"auth": auth}}})
	client := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "rbd-hub-credentials", Namespace: "system"}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: raw}})
	scope := InstallationScope{Enterprise: "enterprise", Region: "region", Namespace: "plugins"}
	name, err := EnsureRegistryAuthSecret(ctx, client, "system", "goodrain.me", scope)
	if err != nil || name != "enterprise-base-registry-auth" {
		t.Fatal("Registry credentials were not projected", name, err)
	}
	secret, _ := client.CoreV1().Secrets("plugins").Get(ctx, name, metav1.GetOptions{})
	if string(secret.Data["username"]) != "fixture-user" || string(secret.Data["password"]) != "fixture-password" {
		t.Fatal("wrong target Registry credentials")
	}
	if _, err := EnsureRegistryAuthSecret(ctx, client, "system", "goodrain.me", scope); err != nil {
		t.Fatal("repeated initialization failed", err)
	}
	scope.Enterprise = "foreign"
	if _, err := EnsureRegistryAuthSecret(ctx, client, "system", "goodrain.me", scope); err == nil {
		t.Fatal("foreign installation adopted Registry credentials")
	}
}
