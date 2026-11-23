package kubeidentity

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInstallationResolvesReleaseHelperWithoutMountingClusterData(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	candidate := "release.test/rbd-registry-gc:v-next"
	if _, err := EnsureInstallationImage(ctx, client, "system", candidate, "node"); err != ErrExecutorStarting {
		t.Fatal("unobserved image was accepted", err)
	}
	jobs, _ := client.BatchV1().Jobs("system").List(ctx, metav1.ListOptions{})
	if len(jobs.Items) != 1 {
		t.Fatal("missing version probe")
	}
	job := jobs.Items[0]
	job.UID = "probe-uid"
	client.BatchV1().Jobs("system").Update(ctx, &job, metav1.UpdateOptions{})
	spec := job.Spec.Template.Spec
	if len(spec.Volumes) != 0 || spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken || len(spec.Containers[0].VolumeMounts) != 0 || spec.Containers[0].Command[0] != "/bin/registry" {
		t.Fatal("version probe received data or API access")
	}
	yes := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: "system", Labels: job.Spec.Template.Labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &yes}}}, Spec: spec, Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "version", ImageID: "release.test/rbd-registry-gc@sha256:" + strings.Repeat("a", 64), State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}}}
	client.CoreV1().Pods("system").Create(ctx, pod, metav1.CreateOptions{})
	resolved, err := EnsureInstallationImage(ctx, client, "system", candidate, "node")
	if err != nil || resolved != "release.test/rbd-registry-gc@sha256:"+strings.Repeat("a", 64) {
		t.Fatal("observed release digest not resolved", resolved, err)
	}
}
