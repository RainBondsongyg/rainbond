package controller

import (
	"context"

	guard "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// Called only after the native result and final admission state are durable.
// Failed cleanup scheduling must not turn a confirmed operation into unknown.
func (h *CleanupCoordinationHandler) scheduleFinishedHelperCleanup(ctx context.Context, namespace, name, uid string) {
	if h.gcTarget == nil || namespace == "" || name == "" || uid == "" {
		return
	}
	client, systemNamespace, _, err := h.gcTarget()
	if err != nil || client == nil || namespace != systemNamespace {
		return
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if string(job.UID) != uid {
			return guard.ErrCoordinationChanged
		}
		if job.Spec.TTLSecondsAfterFinished != nil && *job.Spec.TTLSecondsAfterFinished <= 60 {
			return nil
		}
		retention := int32(60)
		job.Spec.TTLSecondsAfterFinished = &retention
		_, err = client.BatchV1().Jobs(namespace).Update(ctx, job, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		logrus.Warn("Finished cleanup helper retention could not be scheduled")
	}
}
