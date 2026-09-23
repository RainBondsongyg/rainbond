package cleanup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrCoordinationUnavailable never includes an upstream URL, credential or body.
var ErrCoordinationUnavailable = errors.New("cleanup coordination unavailable")

// ErrCoordinationDenied indicates that the internal participant was rejected.
var ErrCoordinationDenied = errors.New("cleanup coordination authorization denied")

// CoordinationClient is used by participants without direct Region DB access.
// It does not retry writes or follow redirects with the Region credential.
type CoordinationClient struct {
	base  string
	token string
	http  *http.Client
}

// NewCoordinationClient binds one trusted Region API origin. Plain HTTP must be
// explicitly allowed for a trusted internal deployment or an isolated test.
func NewCoordinationClient(endpoint, token string, allowHTTP bool) (*CoordinationClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && !(allowHTTP && u.Scheme == "http")) || token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, ErrCoordinationChanged
	}
	return &CoordinationClient{base: u.Scheme + "://" + u.Host, token: token, http: &http.Client{Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

type coordinationResponse struct {
	Msg  string `json:"msg"`
	Bean struct {
		Protocol      int    `json:"protocol"`
		NewlyAdmitted *bool  `json:"newly_admitted"`
		Recorded      *bool  `json:"recorded"`
		State         string `json:"state"`
	} `json:"bean"`
}

func (c *CoordinationClient) call(ctx context.Context, r CoordinationRequest, action string, body interface{}) (coordinationResponse, error) {
	var result coordinationResponse
	if !r.valid() {
		return result, ErrCoordinationChanged
	}
	path := "/v2/cleanup/stores/" + url.PathEscape(r.StorageID) + "/operations"
	if action != "" {
		path += "/" + url.PathEscape(r.OperationID) + "/" + action
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return result, ErrCoordinationChanged
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return result, ErrCoordinationChanged
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Token "+c.token)
	response, err := c.http.Do(req)
	if err != nil {
		return result, ErrCoordinationUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == 401 || response.StatusCode == 403 {
		return result, ErrCoordinationDenied
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return result, ErrCoordinationUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(&result) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return result, ErrCoordinationUnavailable
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == 409 {
			switch result.Msg {
			case "COORDINATION_UNCERTAIN":
				return result, ErrCoordinationUncertain
			case "COORDINATION_CHANGED":
				return result, ErrCoordinationChanged
			default:
				return result, ErrCoordinationBusy
			}
		}
		return result, ErrCoordinationUnavailable
	}
	if result.Bean.Protocol != 1 {
		return result, ErrCoordinationUnavailable
	}
	return result, nil
}

// Acquire returns true only for an explicitly acknowledged new admission.
func (c *CoordinationClient) Acquire(ctx context.Context, r CoordinationRequest) (bool, error) {
	if r.Kind == "gc" {
		return false, ErrCoordinationChanged
	}
	response, err := c.call(ctx, r, "", r)
	if err != nil {
		return false, err
	}
	if response.Bean.NewlyAdmitted == nil {
		return false, ErrCoordinationUnavailable
	}
	return *response.Bean.NewlyAdmitted, nil
}

// Finish records a confirmed or uncertain outcome; it never releases uncertainty.
func (c *CoordinationClient) Finish(ctx context.Context, r CoordinationRequest, confirmed bool) error {
	if r.Kind == "gc" {
		return ErrCoordinationChanged
	}
	body := struct {
		CoordinationRequest
		Confirmed bool `json:"confirmed"`
	}{r, confirmed}
	response, err := c.call(ctx, r, "finish", body)
	if err != nil {
		return err
	}
	if response.Bean.Recorded == nil || !*response.Bean.Recorded {
		return ErrCoordinationUnavailable
	}
	return nil
}

// Inspect observes the exact previously bound operation without resubmission.
func (c *CoordinationClient) Inspect(ctx context.Context, r CoordinationRequest) (string, error) {
	response, err := c.call(ctx, r, "inspect", r)
	if err != nil {
		return "", err
	}
	switch response.Bean.State {
	case "active", "executing", "applied", "rejected", "uncertain", "finished", "draining", "exclusive", "restore_pending", "restoring":
		return response.Bean.State, nil
	default:
		return "", ErrCoordinationUnavailable
	}
}

// RequestMaintenance closes admission without granting GC execution.
func (c *CoordinationClient) RequestMaintenance(ctx context.Context, r CoordinationRequest) (bool, error) {
	response, err := c.call(ctx, r, "maintenance/request", r)
	if err != nil {
		return false, err
	}
	if response.Bean.NewlyAdmitted == nil {
		return false, ErrCoordinationUnavailable
	}
	return *response.Bean.NewlyAdmitted, nil
}
func (c *CoordinationClient) record(ctx context.Context, r CoordinationRequest, action string, body interface{}) error {
	response, err := c.call(ctx, r, action, body)
	if err != nil {
		return err
	}
	if response.Bean.Recorded == nil || !*response.Bean.Recorded {
		return ErrCoordinationUnavailable
	}
	return nil
}

// EnterMaintenance obtains the one-time execution grant after draining writers.
func (c *CoordinationClient) EnterMaintenance(ctx context.Context, r CoordinationRequest) error {
	return c.record(ctx, r, "maintenance/enter", r)
}

// CancelDrain cancels maintenance only before any GC execution grant.
func (c *CoordinationClient) CancelDrain(ctx context.Context, r CoordinationRequest) error {
	return c.record(ctx, r, "maintenance/cancel", r)
}

// CompleteMaintenanceWork records the observed GC process outcome.
func (c *CoordinationClient) CompleteMaintenanceWork(ctx context.Context, r CoordinationRequest, outcome string) error {
	return c.record(ctx, r, "maintenance/complete", struct {
		CoordinationRequest
		Outcome string `json:"outcome"`
	}{r, outcome})
}

// BeginRestore durably records native restoration intent.
func (c *CoordinationClient) BeginRestore(ctx context.Context, r CoordinationRequest) error {
	return c.record(ctx, r, "maintenance/restore", r)
}

// FinishRestore cannot turn an uncertain restoration into success by retrying.
func (c *CoordinationClient) FinishRestore(ctx context.Context, r CoordinationRequest, confirmed bool) error {
	return c.record(ctx, r, "maintenance/restored", struct {
		CoordinationRequest
		Confirmed bool `json:"confirmed"`
	}{r, confirmed})
}
