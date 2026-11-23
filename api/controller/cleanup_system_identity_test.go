package controller

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// capability_id: rainbond.cleanup.system-identity-verification
func TestSystemIdentityVerificationDoesNotReturnItsCredential(t *testing.T) {
	key := []byte(strings.Repeat("isolated-fixture", 4))
	scope, _ := json.Marshal(kubeidentity.InstallationScope{Protocol: 1, Enterprise: "enterprise", Region: "region"})
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: kubeidentity.InstallationConfigName, Namespace: "system"}, Data: map[string]string{"installation.json": string(scope), "system-secret": "system-control"}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "system-control", Namespace: "system"}, Data: map[string][]byte{"key": key}})
	h := &CleanupCoordinationHandler{installationTarget: func() (kubernetes.Interface, dynamic.Interface, string, error) { return client, nil, "system", nil }}
	body := `{"path":"/v2/cleanup/stores/discover","body":"{}"}`
	at := strconv.FormatInt(time.Now().Unix(), 10)
	sum := sha256.Sum256([]byte(body))
	message := strings.Join([]string{"cleanup-coordination-v1", "POST", "/console/cleanup/internal/system-coordination/enterprise/region", "enterprise", "region", at, hex.EncodeToString(sum[:])}, "\n")
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	command := map[string]string{"enterprise": "enterprise", "region": "region", "issued_at": at, "signature": hex.EncodeToString(mac.Sum(nil)), "body": body}
	call := func() *httptest.ResponseRecorder {
		raw, _ := json.Marshal(command)
		req := httptest.NewRequest("POST", "/v2/cleanup/system-identity/verify", bytes.NewReader(raw)).WithContext(context.Background())
		response := httptest.NewRecorder()
		h.VerifyCleanupSystemIdentity(response, req)
		return response
	}
	if response := call(); response.Code != 200 || bytes.Contains(response.Body.Bytes(), key) {
		t.Fatal("verification did not remain server-side", response.Code)
	}
	command["body"] += " "
	if response := call(); response.Code != 403 {
		t.Fatal("changed system envelope accepted", response.Code)
	}
}
