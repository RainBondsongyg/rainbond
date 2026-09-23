package model

import "time"

// CleanupStorage is provisioned with a verified physical storage generation.
// Migration alone never makes a storage ready for cleanup.
type CleanupStorage struct {
	MaintenanceOperationID string `gorm:"type:varchar(64);not null;default:''"`
	PreviousMode           string `gorm:"type:varchar(24);not null;default:''"`
	StorageID              string `gorm:"column:storage_id;type:varchar(64);primary_key"`
	Generation             string `gorm:"type:varchar(64);not null"`
	Mode                   string `gorm:"type:varchar(24);not null;default:'unverified'"`
	Revision               uint64 `gorm:"not null;default:0"`
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// TableName returns the storage coordination table.
func (CleanupStorage) TableName() string { return "cleanup_storage" }

// CleanupOperation has no lease expiry. Uncertain operations remain protective
// across process restarts until explicit reconciliation proves a safe outcome.
type CleanupOperation struct {
	ParentOperationID string  `gorm:"type:varchar(64);not null;default:'';index:idx_cleanup_parent"`
	ExternalKey       *string `gorm:"type:varchar(64);unique_index:idx_cleanup_external"`
	ExternalScope     string  `gorm:"type:varchar(255);not null;default:''"`
	ExternalID        string  `gorm:"type:varchar(256);not null;default:''"`
	ClosingUpload     bool    `gorm:"not null;default:false"`
	Target            string  `gorm:"type:varchar(1024);not null;default:''"`
	Outcome           string  `gorm:"type:varchar(24);not null;default:''"`
	OperationID       string  `gorm:"column:operation_id;type:varchar(64);primary_key"`
	StorageID         string  `gorm:"type:varchar(64);not null;index:idx_cleanup_store"`
	Generation        string  `gorm:"type:varchar(64);not null"`
	Owner             string  `gorm:"type:varchar(128);not null"`
	Kind              string  `gorm:"type:varchar(24);not null"`
	Scope             string  `gorm:"type:varchar(255);not null"`
	Fingerprint       string  `gorm:"type:varchar(128);not null"`
	State             string  `gorm:"type:varchar(24);not null"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// TableName returns the persistent operation table.
func (CleanupOperation) TableName() string { return "cleanup_operations" }
