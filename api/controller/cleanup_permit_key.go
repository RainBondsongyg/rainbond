package controller

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	"github.com/goodrain/rainbond/pkg/component/k8s"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func managedRegistryPermitKey(ctx context.Context, client kubernetes.Interface, namespace string) ([]byte, bool) {
	config, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, kubeidentity.InstallationConfigName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		return nil, true
	}
	name := config.Data["permit-secret"]
	if name == "" {
		return nil, false
	}
	secret, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, true
	}
	value := strings.TrimSpace(string(secret.Data["key"]))
	if len(value) < 32 || len(value) > 4096 || strings.ContainsAny(value, " \t\r\n\x00") {
		return nil, true
	}
	return []byte(value), true
}

// An explicitly configured key must fail closed; never silently substitute the
// administrator token when the dedicated key is missing or invalid.
func systemRegistryPermitKey() []byte {
	if component := k8s.Default(); component != nil && component.RestConfig != nil {
		client, _, namespace, err := systemCleanupInstallationTarget()
		if err != nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if key, configured := managedRegistryPermitKey(ctx, client, namespace); configured {
			return key
		}
	}
	path := os.Getenv("CLEANUP_REGISTRY_PERMIT_KEY_FILE")
	if path == "" {
		return []byte(os.Getenv("TOKEN"))
	}
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(raw) > 8192 {
		return nil
	}
	value := strings.TrimSpace(string(raw))
	if len(value) < 32 || strings.ContainsAny(value, " \t\r\n\x00") {
		return nil
	}
	return []byte(value)
}
