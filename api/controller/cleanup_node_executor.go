package controller

import (
	"net/http"

	guard "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	httputil "github.com/goodrain/rainbond/util/http"
)

// EnterNodeJob admits only a Core-bound node executor using live Kubernetes facts.
// This route is always protected by FullToken; caller runtime assertions fail decoding.
func (h *CleanupCoordinationHandler) EnterNodeJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		guard.CoordinationRequest
		guard.NodeExecutorLocator
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.CoordinationRequest) {
		return
	}
	if !body.NodeExecutorLocator.Valid() {
		httputil.ReturnError(r, w, 400, "INVALID_NODE_EXECUTOR")
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	if h.gcTarget == nil {
		coordinationError(w, r, guard.ErrCoordinationUnavailable)
		return
	}
	database := h.database()
	execution, err := guard.ReadNodeJobBinding(database, body.CoordinationRequest)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	client, namespace, _, err := h.gcTarget()
	if err != nil || client == nil || execution.Namespace != namespace {
		coordinationError(w, r, guard.ErrCoordinationChanged)
		return
	}
	job, err := guard.ReconcileNodeJob(r.Context(), database, client.BatchV1().Jobs(namespace), body.CoordinationRequest)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	storage, err := guard.StorageBinding(database, body.StorageID, body.Generation)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	observed, err := kubeidentity.InspectNodeExecutor(r.Context(), client, job, body.Pod, body.PodUID, storage, execution)
	if err != nil {
		coordinationError(w, r, guard.ErrCoordinationChanged)
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	if err := guard.EnterNodeExecution(database, body.CoordinationRequest, observed); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}
