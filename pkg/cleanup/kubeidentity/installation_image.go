package kubeidentity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"

	"github.com/docker/distribution/reference"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// RuntimeImage resolves the digest of one observed container, including runtimes
// which report only sha256:<digest> rather than the repository name.
func RuntimeImage(container corev1.Container, status corev1.ContainerStatus) (string, error) {
	return installedRuntimeImage(container, status)
}

// EnsureInstallationImage resolves a platform release image with a short-lived
// version probe. It receives neither credentials nor mounted application data.
func EnsureInstallationImage(ctx context.Context, client kubernetes.Interface, namespace, candidate, node string) (string, error) {
	if client == nil || namespace == "" || node == "" {
		return "", ErrBinding
	}
	if _, err := reference.ParseNormalizedNamed(candidate); err != nil {
		return "", ErrBinding
	}
	hash := sha256.Sum256([]byte(candidate + "\x00" + node))
	name := "rainbond-disk-image-" + hex.EncodeToString(hash[:6])
	label := map[string]string{"cleanup.rainbond.io/image-probe": name}
	no, yes := false, true
	limit, ttl, retries := int64(120), int32(600), int32(0)
	container := corev1.Container{Name: "version", Image: candidate, Command: []string{"/bin/registry", "--version"}, SecurityContext: &corev1.SecurityContext{ReadOnlyRootFilesystem: &yes, AllowPrivilegeEscalation: &no, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}}
	desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: label}, Spec: batchv1.JobSpec{ActiveDeadlineSeconds: &limit, TTLSecondsAfterFinished: &ttl, BackoffLimit: &retries, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: label}, Spec: corev1.PodSpec{NodeName: node, AutomountServiceAccountToken: &no, RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{container}}}}}
	job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.BatchV1().Jobs(namespace).Create(ctx, desired, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return "", err
		}
		return "", ErrExecutorStarting
	}
	if err != nil {
		return "", err
	}
	spec := job.Spec.Template.Spec
	if job.DeletionTimestamp != nil || len(spec.Containers) != 1 || spec.Containers[0].Image != candidate || !reflect.DeepEqual(spec.Containers[0].Command, container.Command) || len(spec.Volumes) != 0 || len(spec.Containers[0].VolumeMounts) != 0 || spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken || spec.NodeName != node {
		return "", ErrBinding
	}
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(label).String()})
	if err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.Kind != "Job" || owner.UID != job.UID || job.UID == "" || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase == corev1.PodFailed {
			return "", ErrBinding
		}
		if pod.Status.Phase == corev1.PodSucceeded && len(pod.Spec.Containers) == 1 && len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].State.Terminated != nil && pod.Status.ContainerStatuses[0].State.Terminated.ExitCode == 0 {
			return installedRuntimeImage(pod.Spec.Containers[0], pod.Status.ContainerStatuses[0])
		}
	}
	return "", ErrExecutorStarting
}
