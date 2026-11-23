package kubeidentity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"

	"github.com/docker/distribution/reference"
	digest "github.com/opencontainers/go-digest"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// InstallationConfigName identifies platform-managed cleanup installation settings.
const InstallationConfigName = "rainbond-disk-installation"

// ErrInstallationNotConfigured permits legacy settings before automatic adoption.
var ErrInstallationNotConfigured = errors.New("disk cleanup installation is not configured")

// InstallationScope is written by the platform installer. It contains resource
// references only; credentials never belong in this ConfigMap.
type InstallationScope struct {
	Protocol         int    `json:"protocol"`
	Enterprise       string `json:"enterprise"`
	Region           string `json:"region"`
	Namespace        string `json:"namespace"`
	BackendService   string `json:"backendService"`
	StateClaim       string `json:"stateClaim"`
	CredentialSecret string `json:"credentialSecret"`
	ConsoleURL       string `json:"consoleUrl"`
	AllowHTTP        bool   `json:"allowHttp"`
}

func installationBackend(scope InstallationScope) (string, error) {
	if scope.Protocol != 1 || !nodeConsoleScope.MatchString(scope.Enterprise) || !nodeConsoleScope.MatchString(scope.Region) || len(scope.Region) > 63 || len(validation.IsDNS1123Label(scope.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(scope.StateClaim)) != 0 || len(validation.IsDNS1123Subdomain(scope.CredentialSecret)) != 0 {
		return "", ErrBinding
	}
	console, err := url.Parse(scope.ConsoleURL)
	if err != nil || console.Host == "" || console.User != nil || console.RawQuery != "" || console.Fragment != "" || (console.Path != "" && console.Path != "/") || (console.Scheme != "https" && !(console.Scheme == "http" && scope.AllowHTTP)) {
		return "", ErrBinding
	}
	backend, err := url.Parse(scope.BackendService)
	if err != nil || backend.Host == "" || backend.User != nil || backend.RawQuery != "" || backend.Fragment != "" || (backend.Path != "" && backend.Path != "/") || (backend.Scheme != "http" && backend.Scheme != "https") {
		return "", ErrBinding
	}
	host := backend.Hostname()
	name := strings.Split(host, ".")[0]
	if len(validation.IsDNS1123Label(name)) != 0 {
		return "", ErrBinding
	}
	if host != name+"."+scope.Namespace && host != name+"."+scope.Namespace+".svc" && host != name+"."+scope.Namespace+".svc.cluster.local" {
		return "", ErrBinding
	}
	return name, nil
}

func installedRuntimeImage(container corev1.Container, status corev1.ContainerStatus) (string, error) {
	if status.Name != container.Name || status.ImageID == "" {
		return "", ErrBinding
	}
	declared, err := reference.ParseNormalizedNamed(container.Image)
	if err != nil {
		return "", ErrBinding
	}
	actual := strings.TrimPrefix(strings.TrimPrefix(status.ImageID, "docker-pullable://"), "containerd://")
	var imageDigest digest.Digest
	if strings.HasPrefix(actual, "sha256:") {
		imageDigest = digest.Digest(actual)
	} else {
		observed, err := reference.ParseNormalizedNamed(actual)
		if err != nil || reference.TrimNamed(observed).Name() != reference.TrimNamed(declared).Name() {
			return "", ErrBinding
		}
		pinned, ok := observed.(reference.Canonical)
		if !ok {
			return "", ErrBinding
		}
		imageDigest = pinned.Digest()
	}
	if imageDigest.Algorithm() != digest.SHA256 || imageDigest.Validate() != nil {
		return "", ErrBinding
	}
	if pinned, ok := declared.(reference.Canonical); ok && pinned.Digest() != imageDigest {
		return "", ErrBinding
	}
	image, err := reference.WithDigest(reference.TrimNamed(declared), imageDigest)
	if err != nil {
		return "", ErrBinding
	}
	return image.String(), nil
}

// InstalledNodeSettings follows the actual installed backend version. It does
// not mutate resources or select an image from a browser or deletion request.
func InstalledNodeSettings(ctx context.Context, client kubernetes.Interface, systemNamespace string) (NodeJobSettings, error) {
	denied := NodeJobSettings{}
	if client == nil || len(validation.IsDNS1123Label(systemNamespace)) != 0 {
		return denied, ErrBinding
	}
	config, err := client.CoreV1().ConfigMaps(systemNamespace).Get(ctx, InstallationConfigName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return denied, ErrInstallationNotConfigured
	}
	if err != nil {
		return denied, err
	}
	raw := config.Data["installation.json"]
	if config.DeletionTimestamp != nil || len(raw) > 65536 {
		return denied, ErrBinding
	}
	var scope InstallationScope
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&scope) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return denied, ErrBinding
	}
	name, err := installationBackend(scope)
	if err != nil {
		return denied, err
	}
	service, err := client.CoreV1().Services(scope.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || service.DeletionTimestamp != nil || service.Spec.Type == corev1.ServiceTypeExternalName || len(service.Spec.Selector) == 0 {
		return denied, ErrBinding
	}
	pods, err := client.CoreV1().Pods(scope.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String()})
	if err != nil {
		return denied, err
	}
	image := ""
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if len(pod.Spec.Containers) != 1 || len(pod.Status.ContainerStatuses) != 1 {
			return denied, ErrBinding
		}
		actual, err := installedRuntimeImage(pod.Spec.Containers[0], pod.Status.ContainerStatuses[0])
		if err != nil {
			return denied, err
		}
		if image != "" && image != actual {
			return denied, ErrExecutorStarting
		}
		image = actual
	}
	if image == "" {
		return denied, ErrExecutorStarting
	}
	return NodeJobSettings{Region: scope.Region, Image: image, Endpoint: scope.ConsoleURL, CredentialSecret: scope.CredentialSecret, StateClaim: scope.StateClaim, ConsoleEnterprise: scope.Enterprise, AllowConsoleHTTP: scope.AllowHTTP}, nil
}
