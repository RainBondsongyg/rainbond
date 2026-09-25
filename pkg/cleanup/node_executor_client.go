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

// RecordNodeJobResult reports original native effects without releasing the scope.
func (c *CoordinationClient) RecordNodeJobResult(ctx context.Context, r CoordinationRequest, executor NodeExecutorLocator, result NodeExecutionResult) error {
	if r.Kind != "delete" || !executor.Valid() || !validNodeResult(result) {
		return ErrCoordinationChanged
	}
	return c.record(ctx, r, "node/result", struct {
		CoordinationRequest
		NodeExecutorLocator
		Result NodeExecutionResult `json:"result"`
	}{r, executor, result})
}

// FinishNodeJob asks Core to verify original termination, never a write toggle.
func (c *CoordinationClient) FinishNodeJob(ctx context.Context, r CoordinationRequest, executor NodeExecutorLocator) error {
	if r.Kind != "delete" || !executor.Valid() {
		return ErrCoordinationChanged
	}
	return c.record(ctx, r, "node/finish", struct {
		CoordinationRequest
		NodeExecutorLocator
	}{r, executor})
}

// NodeJobProgress reads the original immutable task receipt.
func (c *CoordinationClient) NodeJobProgress(ctx context.Context, r CoordinationRequest) (NodeJobProgress, error) {
	if r.Kind != "delete" {
		return NodeJobProgress{}, ErrCoordinationChanged
	}
	response, err := c.call(ctx, r, "node/status", r)
	if err != nil {
		return NodeJobProgress{}, err
	}
	result := response.Bean.NodeJob
	if result == nil || result.StorageID != r.StorageID || result.Generation != r.Generation || result.OperationID != r.OperationID || result.Execution.Protocol != 1 || !validNodeJobIntent(r, result.Execution.NodeJobIntent) {
		return NodeJobProgress{}, ErrCoordinationChanged
	}
	if result.Execution.Result != nil && !validNodeResult(*result.Execution.Result) {
		return NodeJobProgress{}, ErrCoordinationChanged
	}
	if result.State == "finished" && (result.Execution.Result == nil || result.Execution.FinishedAt == nil || result.Outcome != result.Execution.Result.State) {
		return NodeJobProgress{}, ErrCoordinationChanged
	}
	return *result, nil
}
