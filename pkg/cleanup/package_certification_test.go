package cleanup

import (
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/goodrain/rainbond/db/model"
	"github.com/jinzhu/gorm"
)

func packageCertificationFixture(t *testing.T) (*gorm.DB, StorageRegistration, []ReferenceWriter) {
	t.Helper()
	database, err := gorm.Open("sqlite3", filepath.Join(t.TempDir(), "package-certification.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.AutoMigrate(&model.CleanupStorage{}, &model.CleanupOperation{}, &model.CleanupReferenceWriter{}, &model.PackageUploadUse{}).Error; err != nil {
		t.Fatal(err)
	}
	binding, err := ProvisionManagedPackageStorage(database, "package-volume", "upload_events")
	if err != nil {
		t.Fatal(err)
	}
	writers := []ReferenceWriter{}
	for _, role := range []string{"api", "worker", "builder", "console"} {
		name := map[string]string{"api": "rbd-api", "worker": "rbd-worker", "builder": "rbd-chaos", "console": "rbd-app-ui"}[role]
		writer := ReferenceWriter{Namespace: "rbd-system", PodName: name + "-one", PodUID: "pod-" + role, ContainerName: name, ContainerID: "containerd://" + role, ImageID: "image-" + role, Role: role, Protocol: ReferenceWriterProtocol}
		if err := RegisterReferenceWriter(database, writer); err != nil {
			t.Fatal(err)
		}
		writers = append(writers, writer)
	}
	upload := writers[0]
	upload.Protocol = UploadWriterProtocol
	if err := RegisterReferenceWriter(database, upload); err != nil {
		t.Fatal(err)
	}
	return database, binding, writers
}

// capability_id: rainbond.cleanup.package-writer-readiness
func TestManagedPackageReadinessRequiresCurrentWritersAndNoUnfinishedUse(t *testing.T) {
	database, binding, writers := packageCertificationFixture(t)
	inspect := func() ([]ReferenceWriter, error) { return writers, nil }
	observed, ready, err := CertifyManagedPackageWriters(database, binding, nil, inspect)
	if err != nil || !ready || observed.Mode != "ready" {
		t.Fatal("complete package writers did not enable readiness", observed, ready, err)
	}

	use := model.PackageUploadUse{OperationID: "upload-use", SessionID: "session", EventID: "event", Action: "chunk", State: "uncertain"}
	if err := database.Create(&use).Error; err != nil {
		t.Fatal(err)
	}
	observed, ready, err = CertifyManagedPackageWriters(database, binding, nil, inspect)
	if err != nil || ready || observed.Mode != "collecting" {
		t.Fatal("unfinished upload use retained package readiness", observed, ready, err)
	}
	if err := database.Model(&model.PackageUploadUse{}).Where("operation_id = ?", use.OperationID).Update("state", "finished").Error; err != nil {
		t.Fatal(err)
	}
	missingUploadProof := append([]ReferenceWriter(nil), writers...)
	missingUploadProof[0].ContainerID = "containerd://replacement"
	if _, ready, err = CertifyManagedPackageWriters(database, binding, nil, func() ([]ReferenceWriter, error) { return missingUploadProof, nil }); err != nil || ready {
		t.Fatal("unregistered replacement writer became ready", ready, err)
	}
}

func TestManagedPackageReadinessExemptsOnlyOriginalDeletion(t *testing.T) {
	database, binding, writers := packageCertificationFixture(t)
	target := hex.EncodeToString(make([]byte, 32))
	request := CoordinationRequest{StorageID: binding.StorageID, Generation: binding.Generation, Owner: "disk-cleanup-task", OperationID: "selected-delete", Kind: "delete", Scope: "event", Fingerprint: "scan", Target: target}
	if _, _, err := CertifyManagedPackageWriters(database, binding, nil, func() ([]ReferenceWriter, error) { return writers, nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireOperation(database, request); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := CertifyManagedPackageWriters(database, binding, &request, func() ([]ReferenceWriter, error) { return writers, nil }); err != nil || !ready {
		t.Fatal("original deletion was not revalidated", ready, err)
	}
	other := request
	other.OperationID = "other-delete"
	if _, ready, err := CertifyManagedPackageWriters(database, binding, &other, func() ([]ReferenceWriter, error) { return writers, nil }); err != nil || ready {
		t.Fatal("unrecorded deletion was exempted", ready, err)
	}
}

// capability_id: rainbond.cleanup.retired-package-producer-epoch
func TestManagedPackageReadinessReconcilesRetiredProducerEpoch(t *testing.T) {
	for _, kind := range []string{"upload_events", "upload_components"} {
		t.Run(kind, func(t *testing.T) {
			database, binding, writers := packageCertificationFixture(t)
			if kind != "upload_events" {
				var err error
				binding, err = ProvisionManagedPackageStorage(database, "package-volume", kind)
				if err != nil {
					t.Fatal(err)
				}
			}
			old := time.Now().Add(-2 * time.Hour)
			producer := model.CleanupOperation{OperationID: "retired-package-producer", StorageID: binding.StorageID, Generation: binding.Generation, Kind: "producer", Owner: "native-image-builder", State: "uncertain", CreatedAt: old, UpdatedAt: old}
			if err := database.Create(&producer).Error; err != nil {
				t.Fatal(err)
			}
			observed, ready, err := CertifyManagedPackageWriters(database, binding, nil, func() ([]ReferenceWriter, error) { return writers, nil })
			if err != nil || !ready || observed.Mode != "ready" {
				t.Fatal("retired producer permanently blocked package storage", observed, ready, err)
			}
			if err := database.Where("operation_id = ?", producer.OperationID).First(&producer).Error; err != nil {
				t.Fatal(err)
			}
			if producer.State != "finished" || producer.Outcome != "reconciled" {
				t.Fatal("retired producer was not reconciled", producer.State, producer.Outcome)
			}
		})
	}
}

func TestManagedPackageEpochKeepsCurrentAndUnknownOperations(t *testing.T) {
	for _, scenario := range []string{"current", "unknown-owner", "deletion", "missing-writer", "unfinished-upload"} {
		t.Run(scenario, func(t *testing.T) {
			database, binding, writers := packageCertificationFixture(t)
			old := time.Now().Add(-2 * time.Hour)
			producer := model.CleanupOperation{OperationID: "protected-package-operation", StorageID: binding.StorageID, Generation: binding.Generation, Kind: "producer", Owner: "native-image-builder", State: "active", CreatedAt: old, UpdatedAt: old}
			switch scenario {
			case "current":
				producer.UpdatedAt = time.Now().Add(time.Minute)
			case "unknown-owner":
				producer.Owner = "unknown-writer"
			case "deletion":
				producer.Kind = "delete"
			case "missing-writer":
				writers[0].ContainerID = "containerd://unregistered"
			case "unfinished-upload":
				if err := database.Create(&model.PackageUploadUse{OperationID: "pending-request", State: "active"}).Error; err != nil {
					t.Fatal(err)
				}
			}
			if err := database.Create(&producer).Error; err != nil {
				t.Fatal(err)
			}
			_, ready, err := CertifyManagedPackageWriters(database, binding, nil, func() ([]ReferenceWriter, error) { return writers, nil })
			if err != nil || ready {
				t.Fatal("unresolved package operation became ready", ready, err)
			}
			if err := database.Where("operation_id = ?", producer.OperationID).First(&producer).Error; err != nil {
				t.Fatal(err)
			}
			if producer.State != "active" || producer.Outcome != "" {
				t.Fatal("unresolved operation was changed", producer.State, producer.Outcome)
			}
		})
	}
}
