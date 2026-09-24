//go:build linux || darwin

package registryproxy

import (
	"fmt"
	"math"
	"time"

	coordination "github.com/goodrain/rainbond/pkg/cleanup"
	"golang.org/x/sys/unix"
)

// StorageMeasurement describes the backing filesystem of a verified Registry
// root. Changes on a shared filesystem are not necessarily attributable to GC.
type StorageMeasurement struct {
	Protocol           int       `json:"protocol"`
	StorageID          string    `json:"storage_id"`
	Generation         string    `json:"generation"`
	BindingFingerprint string    `json:"binding_fingerprint"`
	FilesystemID       string    `json:"filesystem_id"`
	ObservedAt         time.Time `json:"observed_at"`
	TotalBytes         uint64    `json:"total_bytes"`
	FreeBytes          uint64    `json:"free_bytes"`
	AvailableBytes     uint64    `json:"available_bytes"`
	TotalInodes        *uint64   `json:"total_inodes"`
	FreeInodes         *uint64   `json:"free_inodes"`
}

// MeasureStorage observes capacity through the same open directory descriptor
// used to validate the identity marker. It never creates or rewrites metadata.
func MeasureStorage(root string, binding coordination.StorageRegistration) (StorageMeasurement, error) {
	denied := StorageMeasurement{}
	fingerprint, err := binding.Fingerprint()
	if err != nil {
		return denied, ErrStorageIdentity
	}
	rootFD, err := openIdentityRoot(root)
	if err != nil {
		return denied, err
	}
	defer unix.Close(rootFD)
	directory, err := unix.Openat(rootFD, identityDirectory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return denied, ErrStorageIdentity
	}
	defer unix.Close(directory)
	if readIdentity(directory, binding) != nil {
		return denied, ErrStorageIdentity
	}
	var fs unix.Statfs_t
	var file unix.Stat_t
	if unix.Fstatfs(rootFD, &fs) != nil || unix.Fstat(rootFD, &file) != nil {
		return denied, ErrStorageIdentity
	}
	blockSize := measurementBlockSize(&fs)
	if blockSize == 0 || fs.Blocks == 0 || fs.Bfree > fs.Blocks || fs.Bavail > fs.Bfree || fs.Blocks > math.MaxUint64/blockSize {
		return denied, ErrStorageIdentity
	}
	if readIdentity(directory, binding) != nil {
		return denied, ErrStorageIdentity
	}
	result := StorageMeasurement{Protocol: 1, StorageID: binding.StorageID, Generation: binding.Generation,
		BindingFingerprint: fingerprint, FilesystemID: fmt.Sprintf("%x:%x:%x", uint64(file.Dev), uint32(fs.Fsid.Val[0]), uint32(fs.Fsid.Val[1])),
		ObservedAt: time.Now().UTC(), TotalBytes: fs.Blocks * blockSize, FreeBytes: fs.Bfree * blockSize,
		AvailableBytes: fs.Bavail * blockSize}
	if fs.Files > 0 && fs.Ffree <= fs.Files {
		total, free := fs.Files, fs.Ffree
		result.TotalInodes, result.FreeInodes = &total, &free
	}
	return result, nil
}
