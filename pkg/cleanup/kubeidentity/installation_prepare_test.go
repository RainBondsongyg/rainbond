package kubeidentity

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.installation-resources
func TestInstallationPreparesResourcesWithoutManualConfiguration(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "plugins"}})
	scope := InstallationScope{Protocol: 1, Enterprise: "enterprise", Region: "region", Namespace: "plugins", BackendService: "http://base.plugins.svc:8080", ConsoleURL: "http://console.system.svc:7070", AllowHTTP: true, StatePerNode: true, StateStorageClass: "local-path"}
	prepared, err := EnsureInstallationResources(ctx, client, "system", scope)
	if err != nil {
		t.Fatal("empty installation did not initialize", err)
	}
	if prepared.StateClaim != "rainbond-disk-node-state" || prepared.CredentialSecret != "rainbond-disk-node-control" {
		t.Fatal("system references were not generated", prepared)
	}
	control, err := client.CoreV1().Secrets("system").Get(ctx, prepared.CredentialSecret, metav1.GetOptions{})
	if err != nil || len(control.Data["key"]) < 32 {
		t.Fatal("system identity was not prepared", err)
	}
	key := append([]byte(nil), control.Data["key"]...)
	for i := 0; i < 2; i++ {
		if _, err := EnsureInstallationResources(ctx, client, "system", scope); err != nil {
			t.Fatal("reconciliation failed", err)
		}
	}
	current, _ := client.CoreV1().Secrets("system").Get(ctx, prepared.CredentialSecret, metav1.GetOptions{})
	if string(current.Data["key"]) != string(key) {
		t.Fatal("upgrade rotated an active identity")
	}
	if _, err := client.CoreV1().ServiceAccounts("plugins").Get(ctx, "rainbond-enterprise-base", metav1.GetOptions{}); err != nil {
		t.Fatal("plugin service account missing", err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("plugins").Get(ctx, "enterprise-base-disk-state", metav1.GetOptions{}); err != nil {
		t.Fatal("plugin state volume missing", err)
	}
	roles, _ := client.RbacV1().Roles("system").List(ctx, metav1.ListOptions{})
	if len(roles.Items) != 1 || len(roles.Items[0].Rules) != 1 || len(roles.Items[0].Rules[0].ResourceNames) != 1 || roles.Items[0].Rules[0].ResourceNames[0] != "rbd-license-info" {
		t.Fatal("license access was not scoped to its one ConfigMap")
	}
	config, _ := client.CoreV1().ConfigMaps("system").Get(ctx, InstallationConfigName, metav1.GetOptions{})
	var stored InstallationScope
	if json.Unmarshal([]byte(config.Data["installation.json"]), &stored) != nil || stored.Enterprise != scope.Enterprise || stored.StateClaim != prepared.StateClaim {
		t.Fatal("automatic node settings were not persisted")
	}
	foreign := scope
	foreign.Enterprise = "foreign"
	if _, err := EnsureInstallationResources(ctx, client, "system", foreign); err == nil {
		t.Fatal("a different enterprise adopted the installation")
	}
}
