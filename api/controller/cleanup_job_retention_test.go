package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.finished-helper-retention
func TestFinishedHelperRetentionKeepsReplacementJob(t *testing.T) {
	for _, same := range []bool{true, false} {
		client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "helper", UID: types.UID("current")}})
		handler := &CleanupCoordinationHandler{gcTarget: func() (kubernetes.Interface, string, string, error) { return client, "system", "registry", nil }}
		uid := "current"
		if !same {
			uid = "original"
		}
		handler.scheduleFinishedHelperCleanup(context.Background(), "system", "helper", uid)
		job, err := client.BatchV1().Jobs("system").Get(context.Background(), "helper", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if same && (job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 60) {
			t.Fatal("finished helper retention was not scheduled")
		}
		if !same && job.Spec.TTLSecondsAfterFinished != nil {
			t.Fatal("replacement job was modified")
		}
	}
}
