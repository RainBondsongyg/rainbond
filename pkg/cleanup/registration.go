package cleanup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"strings"

	"github.com/goodrain/rainbond/db/model"
	"github.com/jinzhu/gorm"
)

// StorageRegistration contains identities observed by the trusted installer.
// Registration alone is not proof that participants or references are complete.
type StorageRegistration struct {
	StorageID  string `json:"storage_id"`
	Generation string `json:"generation"`
	VolumeUID  string `json:"volume_uid"`
	RootPath   string `json:"root_path"`
}

// Fingerprint validates and hashes the immutable registration descriptor.
func (r StorageRegistration) Fingerprint() (string, error) {
	if !coordinationIdentity.MatchString(r.StorageID) || !coordinationIdentity.MatchString(r.Generation) || !coordinationIdentity.MatchString(r.VolumeUID) || !path.IsAbs(r.RootPath) || r.RootPath == "/" || path.Clean(r.RootPath) != r.RootPath || len(r.RootPath) > 1024 || strings.ContainsAny(r.RootPath, "\x00\\") {
		return "", ErrCoordinationChanged
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// RegisterStorage never promotes readiness or changes an existing generation.
// Collecting allows participants to enroll without interrupting normal writes;
// destructive admission stays closed until independent readiness verification.
func RegisterStorage(database *gorm.DB, r StorageRegistration) error {
	fingerprint, err := r.Fingerprint()
	if err != nil {
		return err
	}
	match := func(existing model.CleanupStorage) error {
		if existing.Generation != r.Generation || existing.RegistrationFingerprint != fingerprint {
			return ErrCoordinationChanged
		}
		return nil
	}
	var existing model.CleanupStorage
	err = database.Where("storage_id = ?", r.StorageID).First(&existing).Error
	if err == nil {
		return match(existing)
	}
	if !gorm.IsRecordNotFoundError(err) {
		return err
	}
	created := model.CleanupStorage{StorageID: r.StorageID, Generation: r.Generation, RegistrationFingerprint: fingerprint, Mode: "collecting"}
	if err := database.Create(&created).Error; err != nil {
		// A concurrent identical registration is harmless. Never use an upsert that
		// could overwrite a live generation or clear an existing maintenance state.
		if readErr := database.Where("storage_id = ?", r.StorageID).First(&existing).Error; readErr == nil {
			return match(existing)
		}
		return err
	}
	return nil
}

// StorageObservation is an advisory identity/status response, not a cleanup grant.
type StorageObservation struct {
	StorageID               string `json:"storage_id"`
	Generation              string `json:"generation"`
	RegistrationFingerprint string `json:"registration_fingerprint"`
	Mode                    string `json:"mode"`
	Revision                uint64 `json:"revision"`
}

// InspectStorage reads one committed row without changing readiness or leases.
func InspectStorage(database *gorm.DB, storage, generation string) (StorageObservation, error) {
	if !coordinationIdentity.MatchString(storage) || !coordinationIdentity.MatchString(generation) {
		return StorageObservation{}, ErrCoordinationChanged
	}
	var row model.CleanupStorage
	if err := database.Where("storage_id = ?", storage).First(&row).Error; err != nil {
		return StorageObservation{}, err
	}
	if row.Generation != generation {
		return StorageObservation{}, ErrCoordinationChanged
	}
	return StorageObservation{StorageID: row.StorageID, Generation: row.Generation, RegistrationFingerprint: row.RegistrationFingerprint, Mode: row.Mode, Revision: row.Revision}, nil
}
