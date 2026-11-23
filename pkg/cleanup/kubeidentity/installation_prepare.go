package kubeidentity

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const installationManager = "enterprise-base-installer"

func installationAnnotations(scope InstallationScope) map[string]string {
	return map[string]string{"cleanup.rainbond.io/enterprise": scope.Enterprise, "cleanup.rainbond.io/region": scope.Region}
}

func installationSecret(ctx context.Context, client kubernetes.Interface, namespace, name string, scope InstallationScope, seed []byte) ([]byte, error) {
	secrets := client.CoreV1().Secrets(namespace)
	secret, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		key := append([]byte(nil), seed...)
		if len(key) == 0 {
			raw := make([]byte, 32)
			if _, err := rand.Read(raw); err != nil {
				return nil, err
			}
			key = []byte(hex.EncodeToString(raw))
		}
		secret = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Annotations: installationAnnotations(scope)}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"key": key}}
		secret, err = secrets.Create(ctx, secret, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			secret, err = secrets.Get(ctx, name, metav1.GetOptions{})
		}
	}
	if err != nil {
		return nil, err
	}
	if secret.DeletionTimestamp != nil || len(secret.Data["key"]) < 32 || len(secret.Data["key"]) > 4096 {
		return nil, ErrBinding
	}
	if seed != nil && !hmac.Equal(seed, secret.Data["key"]) {
		return nil, ErrBinding
	}
	annotations := secret.Annotations
	if annotations["cleanup.rainbond.io/enterprise"] != scope.Enterprise || annotations["cleanup.rainbond.io/region"] != scope.Region {
		// Adopt a previous manually configured control key only when the already
		// scoped plugin gateway proves it is exactly the same installation key.
		if name != "rainbond-disk-node-control" || len(seed) == 0 || annotations["cleanup.rainbond.io/enterprise"] != "" || annotations["cleanup.rainbond.io/region"] != "" {
			return nil, ErrBinding
		}
		if secret.Annotations == nil {
			secret.Annotations = map[string]string{}
		}
		for key, value := range installationAnnotations(scope) {
			secret.Annotations[key] = value
		}
		if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
			return nil, err
		}
	}
	return append([]byte(nil), secret.Data["key"]...), nil
}

func installationReadRoles(ctx context.Context, client kubernetes.Interface, systemNamespace string, scope InstallationScope) error {
	hash := sha256.Sum256([]byte(scope.Enterprise + "\x00" + scope.Region + "\x00" + scope.Namespace))
	suffix := hex.EncodeToString(hash[:6])
	meta := func(name, namespace string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"cleanup.rainbond.io/managed-by": installationManager}}
	}
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"nodes", "pods", "replicationcontrollers"}, Verbs: []string{"get", "list"}},
		{APIGroups: []string{""}, Resources: []string{"nodes/proxy"}, Verbs: []string{"get"}},
		{APIGroups: []string{"apps"}, Resources: []string{"deployments", "statefulsets", "daemonsets", "replicasets"}, Verbs: []string{"get", "list"}},
		{APIGroups: []string{"batch"}, Resources: []string{"jobs", "cronjobs"}, Verbs: []string{"get", "list"}},
		{APIGroups: []string{"rainbond.io"}, Resources: []string{"rbdplugins"}, ResourceNames: []string{"rainbond-enterprise-logs"}, Verbs: []string{"get"}},
	}
	roleName := "rainbond-disk-reader-" + suffix
	roles := client.RbacV1().ClusterRoles()
	role, err := roles.Get(ctx, roleName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = roles.Create(ctx, &rbacv1.ClusterRole{ObjectMeta: meta(roleName, ""), Rules: rules}, metav1.CreateOptions{})
	} else if err == nil && !reflect.DeepEqual(role.Rules, rules) {
		if role.Labels["cleanup.rainbond.io/managed-by"] != installationManager {
			return ErrBinding
		}
		role.Rules = rules
		_, err = roles.Update(ctx, role, metav1.UpdateOptions{})
	}
	if err != nil {
		return err
	}
	subjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: "rainbond-enterprise-base", Namespace: scope.Namespace}}
	bindings := client.RbacV1().ClusterRoleBindings()
	binding, err := bindings.Get(ctx, roleName, metav1.GetOptions{})
	ref := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName}
	if apierrors.IsNotFound(err) {
		_, err = bindings.Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: meta(roleName, ""), RoleRef: ref, Subjects: subjects}, metav1.CreateOptions{})
	} else if err == nil && (!reflect.DeepEqual(binding.Subjects, subjects) || binding.RoleRef != ref) {
		return ErrBinding
	}
	if err != nil {
		return err
	}
	for _, item := range []struct {
		namespace, name string
		rules           []rbacv1.PolicyRule
	}{
		{systemNamespace, "rainbond-disk-license-" + suffix, []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{"rbd-license-info"}, Verbs: []string{"get"}}}},
		{scope.Namespace, "rainbond-disk-gateway-reader", []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{"rainbond-disk-gateway"}, Verbs: []string{"get"}}}},
	} {
		local := client.RbacV1().Roles(item.namespace)
		current, err := local.Get(ctx, item.name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = local.Create(ctx, &rbacv1.Role{ObjectMeta: meta(item.name, item.namespace), Rules: item.rules}, metav1.CreateOptions{})
		} else if err == nil && !reflect.DeepEqual(current.Rules, item.rules) {
			return ErrBinding
		}
		if err != nil {
			return err
		}
		refs := client.RbacV1().RoleBindings(item.namespace)
		currentRef, err := refs.Get(ctx, item.name, metav1.GetOptions{})
		ref := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: item.name}
		if apierrors.IsNotFound(err) {
			_, err = refs.Create(ctx, &rbacv1.RoleBinding{ObjectMeta: meta(item.name, item.namespace), RoleRef: ref, Subjects: subjects}, metav1.CreateOptions{})
		} else if err == nil && (!reflect.DeepEqual(currentRef.Subjects, subjects) || currentRef.RoleRef != ref) {
			return ErrBinding
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// EnsureInstallationResources prepares an already-authorized installation.
// Its caller must validate the registered plugin/application ownership first.
// It creates no executors and neither deletes data nor clears existing receipts.
func EnsureInstallationResources(ctx context.Context, client kubernetes.Interface, systemNamespace string, scope InstallationScope) (InstallationScope, error) {
	scope.StateClaim, scope.CredentialSecret = "rainbond-disk-node-state", "rainbond-disk-node-control"
	if client == nil || len(validation.IsDNS1123Label(systemNamespace)) != 0 {
		return InstallationScope{}, ErrBinding
	}
	// Existing executors may still need durable receipts on this claim. Keep
	// their journal in place; fresh installations use one claim per node.
	legacyState, err := client.CoreV1().PersistentVolumeClaims(systemNamespace).Get(ctx, scope.StateClaim, metav1.GetOptions{})
	if err == nil && legacyState.Status.Phase == corev1.ClaimBound {
		scope.StatePerNode = false
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return InstallationScope{}, err
	}
	if _, err := installationBackend(scope); err != nil {
		return InstallationScope{}, err
	}
	configs := client.CoreV1().ConfigMaps(systemNamespace)
	config, err := configs.Get(ctx, InstallationConfigName, metav1.GetOptions{})
	missing := apierrors.IsNotFound(err)
	if err != nil && !missing {
		return InstallationScope{}, err
	}
	if !missing {
		var previous InstallationScope
		if json.Unmarshal([]byte(config.Data["installation.json"]), &previous) != nil || previous.Enterprise != scope.Enterprise || previous.Region != scope.Region {
			return InstallationScope{}, ErrBinding
		}
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, scope.Namespace, metav1.GetOptions{}); err != nil {
		return InstallationScope{}, err
	}
	var existingGateway []byte
	gateway, err := client.CoreV1().Secrets(scope.Namespace).Get(ctx, "rainbond-disk-gateway", metav1.GetOptions{})
	if err == nil {
		if gateway.Annotations["cleanup.rainbond.io/enterprise"] != scope.Enterprise || gateway.Annotations["cleanup.rainbond.io/region"] != scope.Region {
			return InstallationScope{}, ErrBinding
		}
		existingGateway = gateway.Data["key"]
	} else if !apierrors.IsNotFound(err) {
		return InstallationScope{}, err
	}
	key, err := installationSecret(ctx, client, systemNamespace, scope.CredentialSecret, scope, existingGateway)
	if err != nil {
		return InstallationScope{}, err
	}
	if _, err = installationSecret(ctx, client, scope.Namespace, "rainbond-disk-gateway", scope, key); err != nil {
		return InstallationScope{}, err
	}
	accounts := client.CoreV1().ServiceAccounts(scope.Namespace)
	account, err := accounts.Get(ctx, "rainbond-enterprise-base", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = accounts.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "rainbond-enterprise-base", Namespace: scope.Namespace}}, metav1.CreateOptions{})
	} else if err == nil && account.AutomountServiceAccountToken != nil && !*account.AutomountServiceAccountToken {
		yes := true
		account.AutomountServiceAccountToken = &yes
		_, err = accounts.Update(ctx, account, metav1.UpdateOptions{})
	}
	if err != nil {
		return InstallationScope{}, err
	}
	if err := installationReadRoles(ctx, client, systemNamespace, scope); err != nil {
		return InstallationScope{}, err
	}
	claims := client.CoreV1().PersistentVolumeClaims(scope.Namespace)
	_, err = claims.Get(ctx, "enterprise-base-disk-state", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "enterprise-base-disk-state", Namespace: scope.Namespace}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}}
		if scope.StateStorageClass != "" {
			class := scope.StateStorageClass
			claim.Spec.StorageClassName = &class
		}
		_, err = claims.Create(ctx, claim, metav1.CreateOptions{})
	}
	if err != nil {
		return InstallationScope{}, err
	}
	encoded, err := json.Marshal(scope)
	if err != nil {
		return InstallationScope{}, err
	}
	if missing {
		config = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: InstallationConfigName, Namespace: systemNamespace, Annotations: installationAnnotations(scope)}, Data: map[string]string{"installation.json": string(encoded)}}
		_, err = configs.Create(ctx, config, metav1.CreateOptions{})
	} else if config.Data["installation.json"] != string(encoded) {
		config.Data["installation.json"] = string(encoded)
		_, err = configs.Update(ctx, config, metav1.UpdateOptions{})
	}
	return scope, err
}
