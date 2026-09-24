package cleanup

import (
	"database/sql"

	"github.com/goodrain/rainbond/db/model"
	"github.com/jinzhu/gorm"
)

// WithReferenceMutation serializes a metadata write with cleanup admission.
// The callback must use the supplied transaction, and must not perform network
// work. This protects the metadata commit, not a producer's entire lifetime.
// Empty scopes conservatively mean an unresolved repository dependency.
// Existing transactions remain owned by the caller, which must roll back errors.
func WithReferenceMutation(database *gorm.DB, scopes []string, write func(*gorm.DB) error) error {
	return withResolvedReferenceMutation(database, func(*gorm.DB) ([]string, error) { return scopes, nil }, write)
}

func withResolvedReferenceMutation(database *gorm.DB, prepare func(*gorm.DB) ([]string, error), write func(*gorm.DB) error) error {
	if prepare == nil || write == nil {
		return ErrCoordinationChanged
	}
	_, callerTransaction := database.CommonDB().(*sql.Tx)
	tx := database
	if !callerTransaction {
		tx = database.Begin()
		defer tx.Rollback()
	}
	if tx.Error != nil {
		return tx.Error
	}
	// Acquire write locks before any consistent read, including on MySQL's
	// default REPEATABLE READ isolation. A missing coordination table is an
	// incomplete migration and must not silently bypass protection.
	if err := tx.Model(&model.CleanupStorage{}).Where("storage_id <> ?", "").UpdateColumn("revision", gorm.Expr("revision")).Error; err != nil {
		return err
	}
	var stores []model.CleanupStorage
	current := tx
	if tx.Dialect().GetName() != "sqlite3" {
		// Locking reads see current committed state even when an outer restore
		// transaction established its repeatable-read snapshot before admission.
		current = tx.Set("gorm:query_option", "FOR UPDATE")
	}
	if err := current.Order("storage_id").Find(&stores).Error; err != nil {
		return err
	}
	scopes, err := prepare(tx)
	if err != nil {
		return err
	}
	if len(scopes) == 0 {
		scopes = []string{"*"}
	}
	for _, scope := range scopes {
		request := CoordinationRequest{StorageID: "validation", Generation: "validation", Owner: "reference", OperationID: "validation", Kind: "producer", Scope: scope, Fingerprint: "validation"}
		if !request.valid() {
			return ErrCoordinationChanged
		}
	}
	for _, store := range stores {
		if store.Mode != "ready" && store.Mode != "collecting" {
			return ErrCoordinationBusy
		}
		var operations []model.CleanupOperation
		if err := current.Where("storage_id = ? AND state <> ? AND kind <> ?", store.StorageID, "finished", "producer").Limit(4097).Find(&operations).Error; err != nil {
			return err
		}
		if len(operations) > 4096 {
			return ErrCoordinationBusy
		}
		for _, op := range operations {
			if op.Generation != store.Generation {
				return ErrCoordinationChanged
			}
			for _, scope := range scopes {
				if scope == "*" || op.Scope == "*" || op.Scope == "" || scope == op.Scope {
					return ErrCoordinationBusy
				}
			}
		}
		if err := advanceCleanupRevision(tx, store); err != nil {
			return err
		}
	}
	if err := write(tx); err != nil {
		return err
	}
	if callerTransaction {
		return nil
	}
	return tx.Commit().Error
}
