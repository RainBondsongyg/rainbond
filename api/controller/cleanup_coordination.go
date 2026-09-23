package controller

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/goodrain/rainbond/db"
	guard "github.com/goodrain/rainbond/pkg/cleanup"
	httputil "github.com/goodrain/rainbond/util/http"
	"github.com/jinzhu/gorm"
)

// CleanupCoordinationHandler is an internal Region API. Routes must always use
// FullToken, including deployments where the general API token is not enabled.
// This API records coordination only; it never performs deletion or enables an
// unverified store. Owner identities come from trusted Region participants.
type CleanupCoordinationHandler struct{ database func() *gorm.DB }

// NewCleanupCoordinationHandler uses the Region database manager.
func NewCleanupCoordinationHandler() *CleanupCoordinationHandler {
	return &CleanupCoordinationHandler{database: func() *gorm.DB { return db.GetManager().DB() }}
}

func coordinationError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := http.StatusServiceUnavailable, "COORDINATION_UNAVAILABLE"
	switch {
	case errors.Is(err, guard.ErrCoordinationBusy):
		status, code = 409, "COORDINATION_BUSY"
	case errors.Is(err, guard.ErrCoordinationChanged):
		status, code = 409, "COORDINATION_CHANGED"
	case errors.Is(err, guard.ErrCoordinationUncertain):
		status, code = 409, "COORDINATION_UNCERTAIN"
	case errors.Is(err, gorm.ErrRecordNotFound):
		status, code = 404, "COORDINATION_NOT_FOUND"
	}
	httputil.ReturnError(r, w, status, code)
}
func coordinationDecode(w http.ResponseWriter, r *http.Request, value interface{}) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		httputil.ReturnError(r, w, 400, "INVALID_COORDINATION_REQUEST")
		return false
	}
	return true
}
func coordinationScopeFromRoute(w http.ResponseWriter, r *http.Request, request *guard.CoordinationRequest) bool {
	request.StorageID = chi.URLParam(r, "storage_id")
	if id := chi.URLParam(r, "operation_id"); id != "" && request.OperationID != id {
		httputil.ReturnError(r, w, 400, "INVALID_COORDINATION_SCOPE")
		return false
	}
	return true
}

// Acquire records an admission bound to the server-routed store.
func (h *CleanupCoordinationHandler) Acquire(w http.ResponseWriter, r *http.Request) {
	var request guard.CoordinationRequest
	if !coordinationDecode(w, r, &request) || !coordinationScopeFromRoute(w, r, &request) {
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	created, err := guard.AcquireOperation(h.database(), request)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol      int  `json:"protocol"`
		NewlyAdmitted bool `json:"newly_admitted"`
	}{1, created})
}

// Finish records an original operation outcome without unlocking uncertain work.
func (h *CleanupCoordinationHandler) Finish(w http.ResponseWriter, r *http.Request) {
	var body struct {
		guard.CoordinationRequest
		Confirmed *bool `json:"confirmed"`
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.CoordinationRequest) {
		return
	}
	if body.Confirmed == nil {
		httputil.ReturnError(r, w, 400, "CONFIRMATION_REQUIRED")
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	if err := guard.FinishOperation(h.database(), body.CoordinationRequest, *body.Confirmed); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}

// Inspect returns only the exact bound operation state.
func (h *CleanupCoordinationHandler) Inspect(w http.ResponseWriter, r *http.Request) {
	var request guard.CoordinationRequest
	if !coordinationDecode(w, r, &request) || !coordinationScopeFromRoute(w, r, &request) {
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	state, err := guard.InspectOperation(h.database(), request)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int    `json:"protocol"`
		State    string `json:"state"`
	}{1, state})
}

// RequestMaintenance closes admission but does not start GC.
func (h *CleanupCoordinationHandler) RequestMaintenance(w http.ResponseWriter, r *http.Request) {
	var request guard.CoordinationRequest
	if !coordinationDecode(w, r, &request) || !coordinationScopeFromRoute(w, r, &request) {
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	created, err := guard.RequestMaintenance(h.database(), request)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol      int  `json:"protocol"`
		NewlyAdmitted bool `json:"newly_admitted"`
	}{1, created})
}
func (h *CleanupCoordinationHandler) maintenanceTransition(w http.ResponseWriter, r *http.Request, transition func(*gorm.DB, guard.CoordinationRequest) error) {
	var request guard.CoordinationRequest
	if !coordinationDecode(w, r, &request) || !coordinationScopeFromRoute(w, r, &request) {
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	if err := transition(h.database(), request); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}

// EnterMaintenance grants one execution after all earlier operations drain.
func (h *CleanupCoordinationHandler) EnterMaintenance(w http.ResponseWriter, r *http.Request) {
	h.maintenanceTransition(w, r, guard.EnterMaintenance)
}

// BeginRestore records intent; it does not declare the native storage writable.
func (h *CleanupCoordinationHandler) BeginRestore(w http.ResponseWriter, r *http.Request) {
	h.maintenanceTransition(w, r, guard.BeginMaintenanceRestore)
}

// CancelDrain only cancels a maintenance request before GC has been granted.
func (h *CleanupCoordinationHandler) CancelDrain(w http.ResponseWriter, r *http.Request) {
	h.maintenanceTransition(w, r, guard.CancelMaintenanceDrain)
}

// CompleteMaintenanceWork records the trusted executor's observed process outcome.
func (h *CleanupCoordinationHandler) CompleteMaintenanceWork(w http.ResponseWriter, r *http.Request) {
	var body struct {
		guard.CoordinationRequest
		Outcome string `json:"outcome"`
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.CoordinationRequest) {
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	if err := guard.CompleteMaintenanceWork(h.database(), body.CoordinationRequest, body.Outcome); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}

// FinishRestore records successful native restoration or persistent uncertainty.
func (h *CleanupCoordinationHandler) FinishRestore(w http.ResponseWriter, r *http.Request) {
	var body struct {
		guard.CoordinationRequest
		Confirmed *bool `json:"confirmed"`
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.CoordinationRequest) {
		return
	}
	if body.Confirmed == nil {
		httputil.ReturnError(r, w, 400, "CONFIRMATION_REQUIRED")
		return
	}
	if err := r.Context().Err(); err != nil {
		coordinationError(w, r, err)
		return
	}
	if err := guard.FinishMaintenanceRestore(h.database(), body.CoordinationRequest, *body.Confirmed); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}
