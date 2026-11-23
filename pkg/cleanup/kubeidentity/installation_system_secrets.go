package kubeidentity

import (
	"context"
	"crypto/hmac"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// EnsureInstallationSystemSecrets keeps native platform control independent of
// plugin gateway credentials. Legacy adoption is allowed only after the caller
// proves an existing coordinator belongs to this enterprise and storage binding.
func EnsureInstallationSystemSecrets(ctx context.Context, client kubernetes.Interface, namespace string, scope InstallationScope, control, permit string, adopt bool) error {
	if client == nil || control == permit || len(validation.IsDNS1123Subdomain(control)) != 0 || len(validation.IsDNS1123Subdomain(permit)) != 0 {
		return ErrBinding
	}
	keys := make([][]byte, 0, 2)
	for _, name := range []string{control, permit} {
		if adopt {
			secret, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			if err == nil && secret.Annotations["cleanup.rainbond.io/enterprise"] == "" && secret.Annotations["cleanup.rainbond.io/region"] == "" {
				if secret.Annotations == nil {
					secret.Annotations = map[string]string{}
				}
				for key, value := range installationAnnotations(scope) {
					secret.Annotations[key] = value
				}
				if _, err := client.CoreV1().Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
					return err
				}
			}
		}
		key, err := installationSecret(ctx, client, namespace, name, scope, nil)
		if err != nil {
			return err
		}
		keys = append(keys, key)
	}
	if hmac.Equal(keys[0], keys[1]) {
		return ErrBinding
	}
	return nil
}
