package controller

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	httputil "github.com/goodrain/rainbond/util/http"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// VerifyCleanupSystemIdentity validates native platform signatures for Console.
// This endpoint is reachable only through the authenticated Region control API;
// the credential remains in the cluster and never enters a response.
func (h *CleanupCoordinationHandler) VerifyCleanupSystemIdentity(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Enterprise string `json:"enterprise"`
		Region     string `json:"region"`
		IssuedAt   string `json:"issued_at"`
		Signature  string `json:"signature"`
		Body       string `json:"body"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 128<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || !nodeScopeIdentifier(request.Enterprise) || !nodeScopeIdentifier(request.Region) || len(request.Body) > 65536 || !json.Valid([]byte(request.Body)) || len(request.IssuedAt) > 12 {
		httputil.ReturnError(r, w, http.StatusBadRequest, "INVALID_SYSTEM_COORDINATION")
		return
	}
	at, err := strconv.ParseInt(request.IssuedAt, 10, 64)
	if err != nil || at > time.Now().Unix()+5 || time.Now().Unix()-at > 60 {
		httputil.ReturnError(r, w, http.StatusForbidden, "INVALID_SYSTEM_COORDINATION")
		return
	}
	provided, err := hex.DecodeString(request.Signature)
	if err != nil || len(provided) != sha256.Size {
		httputil.ReturnError(r, w, http.StatusForbidden, "INVALID_SYSTEM_COORDINATION")
		return
	}
	if h.installationTarget == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "SYSTEM_COORDINATION_UNAVAILABLE")
		return
	}
	client, _, namespace, err := h.installationTarget()
	if err != nil || client == nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "SYSTEM_COORDINATION_UNAVAILABLE")
		return
	}
	config, err := client.CoreV1().ConfigMaps(namespace).Get(r.Context(), kubeidentity.InstallationConfigName, metav1.GetOptions{})
	if err != nil {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "SYSTEM_COORDINATION_UNAVAILABLE")
		return
	}
	var scope kubeidentity.InstallationScope
	name := config.Data["system-secret"]
	if json.Unmarshal([]byte(config.Data["installation.json"]), &scope) != nil || scope.Enterprise != request.Enterprise || scope.Region != request.Region || len(validation.IsDNS1123Subdomain(name)) != 0 {
		httputil.ReturnError(r, w, http.StatusForbidden, "INVALID_SYSTEM_COORDINATION")
		return
	}
	secret, err := client.CoreV1().Secrets(namespace).Get(r.Context(), name, metav1.GetOptions{})
	if err != nil || len(secret.Data["key"]) < 32 || len(secret.Data["key"]) > 4096 {
		httputil.ReturnError(r, w, http.StatusServiceUnavailable, "SYSTEM_COORDINATION_UNAVAILABLE")
		return
	}
	sum := sha256.Sum256([]byte(request.Body))
	path := "/console/cleanup/internal/system-coordination/" + request.Enterprise + "/" + request.Region
	message := strings.Join([]string{"cleanup-coordination-v1", "POST", path, request.Enterprise, request.Region, request.IssuedAt, hex.EncodeToString(sum[:])}, "\n")
	mac := hmac.New(sha256.New, secret.Data["key"])
	mac.Write([]byte(message))
	if !hmac.Equal(mac.Sum(nil), provided) {
		httputil.ReturnError(r, w, http.StatusForbidden, "INVALID_SYSTEM_COORDINATION")
		return
	}
	httputil.ReturnSuccess(r, w, map[string]interface{}{"protocol": 1, "verified": true})
}
