package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coordination "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/goodrain/rainbond/pkg/cleanup/registryproxy"
)

func TestInstallerModeInitializesOnlyIdentityWithoutCredentials(t *testing.T) {
	root := t.TempDir()
	args := []string{"--initialize-storage-identity", "--storage-root", root, "--storage-id", "store", "--storage-generation", "one", "--volume-uid", "volume", "--registry-path", "/var/lib/registry"}
	if err := run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if err := registryproxy.VerifyStorageIdentity(root, coordination.StorageRegistration{StorageID: "store", Generation: "one", VolumeUID: "volume", RootPath: "/var/lib/registry"}); err != nil {
		t.Fatal(err)
	}
}
func TestStartupCannotCreateIdentityOrAcceptInlineCredentials(t *testing.T) {
	root := t.TempDir()
	args := []string{"--storage-root", root, "--storage-id", "store", "--storage-generation", "one", "--volume-uid", "volume", "--registry-path", "/var/lib/registry", "--owner", "instance", "--credential-file", filepath.Join(root, "missing")}
	if err := run(context.Background(), args); err == nil {
		t.Fatal("startup accepted missing credential")
	}
	if err := run(context.Background(), append(args, "--token=fixture-only")); err == nil {
		t.Fatal("inline credential accepted")
	}
	if _, err := (tlsFiles{key: "missing"}).transport(); err == nil {
		t.Fatal("incomplete client TLS accepted")
	}
}

// capability_id: rainbond.cleanup.coordinator-runtime
func TestCoordinatorRunsReadinessAndStopsWithContext(t *testing.T) {
	root := t.TempDir()
	binding := coordination.StorageRegistration{StorageID: "store", Generation: "one", VolumeUID: "volume", RootPath: "/var/lib/registry"}
	if err := registryproxy.InitializeStorageIdentity(root, binding); err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := binding.Fingerprint()
	key := strings.Repeat("fixture-only-", 4)
	keyPath := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(keyPath, []byte(key), 0600); err != nil {
		t.Fatal(err)
	}
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/cleanup/stores/store/status" || r.Header.Get("Authorization") != "Token "+key {
			t.Error("wrong control-plane request")
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"bean": map[string]interface{}{"protocol": 1, "storage": coordination.StorageObservation{StorageID: "store", Generation: "one", RegistrationFingerprint: fingerprint, Mode: "collecting"}}})
	}))
	defer core.Close()
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(401)
	}))
	defer registry.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan error, 1)
	args := []string{"--listen", address, "--storage-root", root, "--storage-id", "store", "--storage-generation", "one", "--volume-uid", "volume", "--registry-path", "/var/lib/registry", "--owner", "fixture-instance", "--credential-file", keyPath, "--coordination-api", core.URL, "--allow-internal-http", "--upstream", registry.URL}
	go func() { exited <- run(ctx, args) }()
	client := &http.Client{Timeout: time.Second}
	ready := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		response, err := client.Get("http://" + address + "/readyz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not stop")
	}
	if !ready {
		t.Fatal("coordinator readiness never succeeded")
	}
}
