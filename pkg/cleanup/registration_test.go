package cleanup

import (
	"errors"
	"testing"

	"github.com/goodrain/rainbond/db/model"
)

// capability_id: rainbond.cleanup.storage-enrollment
func TestStorageRegistrationCollectsWritesWithoutEnablingDeletion(t *testing.T) {
	database, _ := coordinationDB(t)
	binding := StorageRegistration{StorageID: "new-store", Generation: "first", VolumeUID: "volume-uid", RootPath: "/var/lib/registry"}
	if err := RegisterStorage(database, binding); err != nil {
		t.Fatal(err)
	}
	if err := RegisterStorage(database, binding); err != nil {
		t.Fatal("idempotent registration failed", err)
	}
	writer := operation("writer", "producer", "app/a")
	writer.StorageID = binding.StorageID
	writer.Generation = binding.Generation
	if created, err := AcquireOperation(database, writer); err != nil || !created {
		t.Fatal("enrollment blocked normal writer", created, err)
	}
	deletion := writer
	deletion.OperationID = "delete"
	deletion.Kind = "delete"
	deletion.Target = "selected"
	if _, err := AcquireOperation(database, deletion); !errors.Is(err, ErrCoordinationBusy) {
		t.Fatal("registration granted deletion", err)
	}
	gc := writer
	gc.OperationID = "gc"
	gc.Kind = "gc"
	gc.Scope = "*"
	if _, err := RequestMaintenance(database, gc); !errors.Is(err, ErrCoordinationBusy) {
		t.Fatal("registration granted GC", err)
	}
	if err := BindRegistryUpload(database, writer, "app/a", "upload-id"); err != nil {
		t.Fatal("enrollment blocked upload binding", err)
	}
	part := writer
	part.OperationID = "part"
	if created, err := AcquireUploadRequest(database, writer, part, true); err != nil || !created {
		t.Fatal("enrollment blocked continuation", created, err)
	}
	if err := FinishUploadRequest(database, writer, part, true); err != nil {
		t.Fatal(err)
	}
	changed := binding
	changed.Generation = "replacement"
	if err := RegisterStorage(database, changed); !errors.Is(err, ErrCoordinationChanged) {
		t.Fatal("registration replaced generation", err)
	}
	changed = binding
	changed.VolumeUID = "other-volume"
	if err := RegisterStorage(database, changed); !errors.Is(err, ErrCoordinationChanged) {
		t.Fatal("registration rebound storage", err)
	}
	observation, err := InspectStorage(database, binding.StorageID, binding.Generation)
	if err != nil || observation.Mode != "collecting" || observation.RegistrationFingerprint == "" {
		t.Fatal(observation, err)
	}
	var store model.CleanupStorage
	if err := database.Where("storage_id = ?", binding.StorageID).First(&store).Error; err != nil || store.Mode != "collecting" {
		t.Fatal(store, err)
	}
}

func TestRepeatedRegistrationDoesNotClearMaintenance(t *testing.T) {
	database, _ := coordinationDB(t)
	binding := StorageRegistration{StorageID: "new-store", Generation: "first", VolumeUID: "volume-uid", RootPath: "/var/lib/registry"}
	if err := RegisterStorage(database, binding); err != nil {
		t.Fatal(err)
	}
	if err := database.Model(&model.CleanupStorage{}).Where("storage_id = ?", binding.StorageID).Updates(map[string]interface{}{"mode": "recovery_required", "maintenance_operation_id": "owned-maintenance"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := RegisterStorage(database, binding); err != nil {
		t.Fatal(err)
	}
	var stored model.CleanupStorage
	if err := database.Where("storage_id = ?", binding.StorageID).First(&stored).Error; err != nil || stored.Mode != "recovery_required" || stored.MaintenanceOperationID != "owned-maintenance" {
		t.Fatal("registration changed maintenance ownership", err)
	}
}
