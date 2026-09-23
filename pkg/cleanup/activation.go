package cleanup

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"github.com/jinzhu/gorm"
)

func NewActivationRevision() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// TrackServiceActivation keeps the service change and its usage checkpoint in
// one transaction. Existing caller-owned transactions are preserved.
func TrackServiceActivation(database *gorm.DB, serviceID, version string, save func(*gorm.DB) error) error {
	if version == "" {
		return save(database)
	}
	revision, err := NewActivationRevision()
	if err != nil {
		return err
	}
	apply := func(tx *gorm.DB) error {
		// Generic DAO saves and builder completion must serialize with retirement,
		// not only explicit rollback/upgrade handlers. An absent target must not
		// become current again through an older in-memory service object.
		var current serviceRow
		if err := tx.Table("tenant_services").Set("gorm:query_option", "FOR UPDATE").Where("service_id = ?", serviceID).First(&current).Error; err != nil {
			return err
		}
		if current.DeployVersion != version {
			var target versionRow
			if err := tx.Table("tenant_service_version").Set("gorm:query_option", "FOR UPDATE").Where("service_id = ? AND build_version = ?", serviceID, version).First(&target).Error; err != nil {
				return err
			}
		}
		if err := save(tx); err != nil {
			return err
		}
		return tx.Table("tenant_service_version").Where("service_id = ? AND build_version = ?", serviceID, version).Update("activation_revision", revision).Error
	}
	if _, ok := database.CommonDB().(*sql.Tx); ok {
		return apply(database)
	}
	tx := database.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer tx.Rollback()
	if err := apply(tx); err != nil {
		return err
	}
	return tx.Commit().Error
}
