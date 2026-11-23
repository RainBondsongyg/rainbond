package kubeidentity

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// EnsureRegistryAuthSecret copies the target platform's repository identity into
// a private projected Secret. Only its reference is returned to the installer.
func EnsureRegistryAuthSecret(ctx context.Context, client kubernetes.Interface, systemNamespace, registryDomain string, scope InstallationScope) (string, error) {
	if client == nil || registryDomain == "" || scope.Namespace == "" || !nodeConsoleScope.MatchString(scope.Enterprise) || !nodeConsoleScope.MatchString(scope.Region) {
		return "", ErrBinding
	}
	source, err := client.CoreV1().Secrets(systemNamespace).Get(ctx, "rbd-hub-credentials", metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	var config struct {
		Auths map[string]struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Auth     string `json:"auth"`
		} `json:"auths"`
	}
	if json.Unmarshal(source.Data[corev1.DockerConfigJsonKey], &config) != nil {
		return "", ErrBinding
	}
	identity, ok := config.Auths[registryDomain]
	if !ok {
		return "", ErrBinding
	}
	username, password := identity.Username, identity.Password
	if identity.Auth != "" {
		decoded, err := base64.StdEncoding.DecodeString(identity.Auth)
		if err != nil {
			return "", ErrBinding
		}
		pair := strings.SplitN(string(decoded), ":", 2)
		if len(pair) != 2 {
			return "", ErrBinding
		}
		username, password = pair[0], pair[1]
	}
	if username == "" || password == "" {
		return "", ErrBinding
	}
	name := "enterprise-base-registry-auth"
	secrets := client.CoreV1().Secrets(scope.Namespace)
	existing, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = secrets.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: scope.Namespace, Annotations: installationAnnotations(scope)}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"username": []byte(username), "password": []byte(password)}}, metav1.CreateOptions{})
		return name, err
	}
	if err != nil {
		return "", err
	}
	if existing.DeletionTimestamp != nil {
		return "", ErrBinding
	}
	same := hmac.Equal(existing.Data["username"], []byte(username)) && hmac.Equal(existing.Data["password"], []byte(password))
	owner := existing.Annotations["cleanup.rainbond.io/enterprise"]
	region := existing.Annotations["cleanup.rainbond.io/region"]
	if owner != scope.Enterprise || region != scope.Region {
		if owner != "" || region != "" || !same {
			return "", ErrBinding
		}
	}
	if same && owner == scope.Enterprise && region == scope.Region {
		return name, nil
	}
	if existing.Annotations == nil {
		existing.Annotations = map[string]string{}
	}
	for key, value := range installationAnnotations(scope) {
		existing.Annotations[key] = value
	}
	existing.Data = map[string][]byte{"username": []byte(username), "password": []byte(password)}
	_, err = secrets.Update(ctx, existing, metav1.UpdateOptions{})
	return name, err
}
