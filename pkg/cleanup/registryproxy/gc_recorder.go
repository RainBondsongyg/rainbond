//go:build linux || darwin

package registryproxy

import (
	"context"

	coordination "github.com/goodrain/rainbond/pkg/cleanup"
)

type gcAPIRecorder struct {
	client      *coordination.CoordinationClient
	binding     coordination.StorageRegistration
	request     coordination.CoordinationRequest
	fingerprint string
}

// NewGCRecorder binds executor callbacks to one immutable storage and operation.
// The launcher remains responsible for verifying process and ingress identity.
func NewGCRecorder(client *coordination.CoordinationClient, binding coordination.StorageRegistration, request coordination.CoordinationRequest) (GCExecutionRecorder, error) {
	if client == nil {
		return nil, coordination.ErrCoordinationUnavailable
	}
	if _, _, err := gcReceiptKey(binding, request); err != nil {
		return nil, err
	}
	fingerprint, _ := binding.Fingerprint()
	return &gcAPIRecorder{client: client, binding: binding, request: request, fingerprint: fingerprint}, nil
}

func (r *gcAPIRecorder) valid(value StorageMeasurement) bool {
	return coordination.IsValidStorageMeasurement(value) && value.StorageID == r.binding.StorageID && value.Generation == r.binding.Generation && value.BindingFingerprint == r.fingerprint
}

func (r *gcAPIRecorder) BeginGC(ctx context.Context, before StorageMeasurement) error {
	if !r.valid(before) {
		return ErrStorageIdentity
	}
	if err := r.client.EnterMaintenance(ctx, r.request); err != nil {
		return err
	}
	// A lost response leaves maintenance protected; never reacquire admission.
	return r.client.RecordMaintenanceMeasurement(ctx, r.request, "before", before)
}

func (r *gcAPIRecorder) CompleteGC(ctx context.Context, outcome string) error {
	if outcome != "succeeded" && outcome != "failed" && outcome != "unknown" {
		return coordination.ErrCoordinationChanged
	}
	return r.client.CompleteMaintenanceWork(ctx, r.request, outcome)
}

func (r *gcAPIRecorder) ObserveGC(ctx context.Context, after StorageMeasurement) error {
	if !r.valid(after) {
		return ErrStorageIdentity
	}
	return r.client.RecordMaintenanceMeasurement(ctx, r.request, "after", after)
}
