//go:build linux

package registryproxy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	coordination "github.com/goodrain/rainbond/pkg/cleanup"
)

type gcRecorderTest struct {
	beginError          error
	completed, observed int
	outcome             string
}

func (r *gcRecorderTest) BeginGC(context.Context, StorageMeasurement) error { return r.beginError }
func (r *gcRecorderTest) CompleteGC(_ context.Context, outcome string) error {
	r.completed++
	r.outcome = outcome
	return nil
}
func (r *gcRecorderTest) ObserveGC(context.Context, StorageMeasurement) error {
	r.observed++
	return nil
}

// capability_id: rainbond.cleanup.registry-gc-executor
func TestGCExecutorDoesNotStartWithoutAdmission(t *testing.T) {
	root := t.TempDir()
	binding := coordination.StorageRegistration{StorageID: "owned", Generation: "one", VolumeUID: "volume", RootPath: "/registry"}
	if err := InitializeStorageIdentity(root, binding); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "owned-command")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n: > \"$0.ran\"\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	denied := errors.New("scope occupied")
	recorder := &gcRecorderTest{beginError: denied}
	if err := ExecuteGC(context.Background(), root, binding, binary, recorder); !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if _, err := os.Stat(binary + ".ran"); !os.IsNotExist(err) {
		t.Fatal("GC started without permission", err)
	}
	if recorder.completed != 0 || recorder.observed != 0 {
		t.Fatal("denied execution produced completion evidence")
	}
}
