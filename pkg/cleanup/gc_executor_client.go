package cleanup

import (
	"context"

	"k8s.io/apimachinery/pkg/util/validation"
)

// GCExecutorLocator contains identity locators only, never claimed runtime facts.
type GCExecutorLocator struct {
	Pod    string `json:"pod"`
	PodUID string `json:"pod_uid"`
}

// Valid rejects incomplete locators before contacting the execution gate.
func (l GCExecutorLocator) Valid() bool {
	return l.Pod != "" && len(validation.IsDNS1123Subdomain(l.Pod)) == 0 && coordinationIdentity.MatchString(l.PodUID)
}

// EnterGCJob requests one admission after Core verifies the actual bound Job,
// Pod and volume. It never retries a lost execution grant.
func (c *CoordinationClient) EnterGCJob(ctx context.Context, r CoordinationRequest, executor GCExecutorLocator) error {
	if r.Kind != "gc" || !executor.Valid() {
		return ErrCoordinationChanged
	}
	return c.record(ctx, r, "maintenance/enter-job", struct {
		CoordinationRequest
		GCExecutorLocator
	}{r, executor})
}
