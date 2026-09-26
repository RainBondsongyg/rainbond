package cleanup

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/goodrain/rainbond/db/model"
	"github.com/jinzhu/gorm"
)

// InspectManagedCacheReadiness observes a coordinated cache with no outstanding
// operation. It grants no permission and does not promote readiness. Deletion
// must still acquire its scope and verify the actual executor independently.
func InspectManagedCacheReadiness(database *gorm.DB, binding StorageRegistration) (StorageObservation, bool, error) {
	var denied StorageObservation
	fingerprint, err := binding.Fingerprint()
	if err != nil {
		return denied, false, err
	}
	expected := sha256.Sum256([]byte("managed-build-cache\x00" + binding.VolumeUID))
	if binding.RootPath != "/cache/build" || binding.StorageID != hex.EncodeToString(expected[:]) {
		return denied, false, ErrCoordinationChanged
	}
	tx := database.Begin()
	if tx.Error != nil {
		return denied, false, tx.Error
	}
	defer tx.Rollback()
	row, err := lockCleanupStorage(tx, CoordinationRequest{StorageID: binding.StorageID, Generation: binding.Generation})
	if err != nil {
		return denied, false, err
	}
	if row.RegistrationFingerprint != fingerprint {
		return denied, false, ErrCoordinationChanged
	}
	observed := StorageObservation{StorageID: row.StorageID, Generation: row.Generation, RegistrationFingerprint: row.RegistrationFingerprint, Mode: row.Mode, Revision: row.Revision}
	if row.Mode != "ready" || row.MaintenanceOperationID != "" {
		return observed, false, nil
	}
	var count int64
	if err := tx.Model(&model.CleanupOperation{}).Where("storage_id = ? AND state <> ?", binding.StorageID, "finished").Count(&count).Error; err != nil {
		return denied, false, err
	}
	return observed, count == 0, nil
}
