package controller

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/goodrain/rainbond/config/configs"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	"github.com/goodrain/rainbond/pkg/component/k8s"
	"k8s.io/client-go/kubernetes"
)

// A managed installation follows its actual plugin version. Legacy overrides
// remain supported only until that installation has been adopted by the installer.
func installedOrLegacyNodeSettings(ctx context.Context, client kubernetes.Interface, namespace string, legacy kubeidentity.NodeJobSettings) (kubeidentity.NodeJobSettings, bool, error) {
	settings, err := kubeidentity.InstalledNodeSettings(ctx, client, namespace)
	if errors.Is(err, kubeidentity.ErrInstallationNotConfigured) {
		return legacy, false, nil
	}
	if err != nil {
		return kubeidentity.NodeJobSettings{}, true, err
	}
	return settings, true, nil
}

func systemInstalledNodeSettings() (kubeidentity.NodeJobSettings, bool) {
	legacy := kubeidentity.NodeJobSettings{Region: os.Getenv("REGION_NAME"), Image: os.Getenv("CLEANUP_NODE_EXECUTOR_IMAGE"), Endpoint: os.Getenv("CLEANUP_NODE_CORE_ENDPOINT"), CredentialSecret: os.Getenv("CLEANUP_NODE_CORE_SECRET"), StateClaim: os.Getenv("CLEANUP_NODE_STATE_CLAIM"), ConsoleEnterprise: os.Getenv("CLEANUP_NODE_CONSOLE_ENTERPRISE"), AllowConsoleHTTP: os.Getenv("CLEANUP_NODE_CONSOLE_ALLOW_HTTP") == "true"}
	component, configuration := k8s.Default(), configs.Default()
	if component == nil || component.RestConfig == nil || configuration == nil || configuration.PublicConfig == nil {
		return legacy, false
	}
	client, err := kubernetes.NewForConfig(cleanupReferenceRestConfig(component.RestConfig))
	if err != nil {
		return kubeidentity.NodeJobSettings{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	settings, automatic, err := installedOrLegacyNodeSettings(ctx, client, configuration.PublicConfig.RbdNamespace, legacy)
	if err != nil {
		return kubeidentity.NodeJobSettings{}, automatic
	}
	return settings, automatic
}
