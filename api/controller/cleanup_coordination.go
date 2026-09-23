package controller

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

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
type CleanupCoordinationHandler struct {
	database  func() *gorm.DB
	permitKey func() []byte
}

// NewCleanupCoordinationHandler uses the Region database manager.
func NewCleanupCoordinationHandler() *CleanupCoordinationHandler {
	return &CleanupCoordinationHandler{database: func() *gorm.DB { return db.GetManager().DB() }, permitKey: func() []byte { return []byte(os.Getenv("TOKEN")) }}
}

func coordinationError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := http.StatusServiceUnavailable, "COORDINATION_UNAVAILABLE"
	switch {
	case errors.Is(err, guard.ErrCoordinationDenied):
		status, code = 403, "COORDINATION_DENIED"
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

// RegistryPermit issues a short-lived credential for an active immutable target.
func (h *CleanupCoordinationHandler) RegistryPermit(w http.ResponseWriter, r *http.Request) {
	var request guard.CoordinationRequest
	if !coordinationDecode(w, r, &request) || !coordinationScopeFromRoute(w, r, &request) {
		return
	}
	if h.permitKey == nil {
		coordinationError(w, r, guard.ErrCoordinationDenied)
		return
	}
	state, err := guard.InspectOperation(h.database(), request)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	if state != "active" {
		coordinationError(w, r, guard.ErrCoordinationUncertain)
		return
	}
	permit, err := guard.IssueRegistryDeletionPermit(h.permitKey(), request, time.Now())
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int    `json:"protocol"`
		Permit   string `json:"permit"`
	}{1, permit})
}

// BeginAttempt consumes the target's one-time execution grant.
func (h *CleanupCoordinationHandler) BeginAttempt(w http.ResponseWriter, r *http.Request) {
	h.maintenanceTransition(w, r, guard.BeginDeletionAttempt)
}

// CompleteAttempt records the observed transport result without releasing scope.
func (h *CleanupCoordinationHandler) CompleteAttempt(w http.ResponseWriter, r *http.Request) {
	var body struct {
		guard.CoordinationRequest
		Outcome string `json:"outcome"`
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.CoordinationRequest) {
		return
	}
	if err := guard.CompleteDeletionAttempt(h.database(), body.CoordinationRequest, body.Outcome); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}

// BindUpload persists the Registry's exact upload identity before forwarding it.
func (h *CleanupCoordinationHandler) BindUpload(w http.ResponseWriter, r *http.Request) {
	var body struct {
		guard.CoordinationRequest
		Repository string `json:"repository"`
		UploadID   string `json:"upload_id"`
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.CoordinationRequest) {
		return
	}
	if err := guard.BindRegistryUpload(h.database(), body.CoordinationRequest, body.Repository, body.UploadID); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}

// LookupUpload resolves an existing upload under the server-selected store.
func (h *CleanupCoordinationHandler) LookupUpload(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Generation string `json:"generation"`
		Repository string `json:"repository"`
		UploadID   string `json:"upload_id"`
	}
	if !coordinationDecode(w, r, &body) {
		return
	}
	binding, err := guard.LookupRegistryUpload(h.database(), chi.URLParam(r, "storage_id"), body.Generation, body.Repository, body.UploadID)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol   int                       `json:"protocol"`
		Binding    guard.CoordinationRequest `json:"binding"`
		Repository string                    `json:"repository"`
		UploadID   string                    `json:"upload_id"`
	}{1, binding, body.Repository, body.UploadID})
}

// AcquireUpload admits only a continuation of a recorded upload session.
func (h *CleanupCoordinationHandler) AcquireUpload(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Parent  guard.CoordinationRequest `json:"parent"`
		Request guard.CoordinationRequest `json:"request"`
		Closing *bool                     `json:"closing"`
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.Parent) {
		return
	}
	body.Request.StorageID = body.Parent.StorageID
	if body.Closing == nil {
		httputil.ReturnError(r, w, 400, "UPLOAD_PHASE_REQUIRED")
		return
	}
	created, err := guard.AcquireUploadRequest(h.database(), body.Parent, body.Request, *body.Closing)
	if err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol      int  `json:"protocol"`
		NewlyAdmitted bool `json:"newly_admitted"`
	}{1, created})
}

// FinishUpload keeps the upload parent until the closing request is confirmed.
func (h *CleanupCoordinationHandler) FinishUpload(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Parent  guard.CoordinationRequest `json:"parent"`
		Request guard.CoordinationRequest `json:"request"`
		Outcome string                    `json:"outcome"`
	}
	if !coordinationDecode(w, r, &body) || !coordinationScopeFromRoute(w, r, &body.Parent) {
		return
	}
	body.Request.StorageID = body.Parent.StorageID
	if body.Outcome == "" {
		httputil.ReturnError(r, w, 400, "CONFIRMATION_REQUIRED")
		return
	}
	if err := guard.RecordUploadRequest(h.database(), body.Parent, body.Request, body.Outcome); err != nil {
		coordinationError(w, r, err)
		return
	}
	httputil.ReturnSuccess(r, w, struct {
		Protocol int  `json:"protocol"`
		Recorded bool `json:"recorded"`
	}{1, true})
}
