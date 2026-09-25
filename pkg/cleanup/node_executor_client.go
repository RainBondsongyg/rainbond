package cleanup

import "context"

// NodeExecutorLocator carries Pod locators only, not claimed runtime evidence.
type NodeExecutorLocator = GCExecutorLocator

// EnterNodeJob requests the original executor's single native grant. A lost
// response is not retried; Core must inspect the actual runtime and mount.
func (c *CoordinationClient) EnterNodeJob(ctx context.Context, r CoordinationRequest, executor NodeExecutorLocator) error {
	if r.Kind != "delete" || !executor.Valid() {
		return ErrCoordinationChanged
	}
	return c.record(ctx, r, "node/enter-job", struct {
		CoordinationRequest
		NodeExecutorLocator
	}{r, executor})
}
