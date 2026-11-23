package kubeidentity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// NodeStateClaimName keeps journals stable across plugin versions and separate
// across physical nodes, including when the platform uses local RWO storage.
func NodeStateClaimName(settings NodeJobSettings, nodeUID string) (string, error) {
	if len(validation.IsDNS1123Subdomain(settings.StateClaim)) != 0 {
		return "", ErrBinding
	}
	if !settings.StatePerNode {
		return settings.StateClaim, nil
	}
	if nodeUID == "" || len(nodeUID) > 64 || !nodeConsoleScope.MatchString(nodeUID) {
		return "", ErrBinding
	}
	hash := sha256.Sum256([]byte(nodeUID))
	name := settings.StateClaim + "-" + hex.EncodeToString(hash[:6])
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return "", ErrBinding
	}
	return name, nil
}

// EnsureNodeStateClaim provisions only the original node's journal. It never
// replaces an existing claim, clears receipts, or grants permission to delete.
func EnsureNodeStateClaim(ctx context.Context, client kubernetes.Interface, namespace, nodeName, nodeUID string, settings NodeJobSettings) (string, error) {
	name, err := NodeStateClaimName(settings, nodeUID)
	if err != nil || !settings.StatePerNode {
		return name, err
	}
	if client == nil || len(validation.IsDNS1123Label(namespace)) != 0 || len(validation.IsDNS1123Subdomain(nodeName)) != 0 {
		return "", ErrBinding
	}
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || string(node.UID) != nodeUID || node.DeletionTimestamp != nil {
		return "", ErrBinding
	}
	claims := client.CoreV1().PersistentVolumeClaims(namespace)
	claim, err := claims.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		claim = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"cleanup.rainbond.io/purpose": "node-journal"}, Annotations: map[string]string{"cleanup.rainbond.io/node-uid": nodeUID, "volume.kubernetes.io/selected-node": nodeName}}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}}
		if settings.StateStorageClass != "" {
			class := settings.StateStorageClass
			claim.Spec.StorageClassName = &class
		}
		claim, err = claims.Create(ctx, claim, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			claim, err = claims.Get(ctx, name, metav1.GetOptions{})
		}
	}
	if err != nil {
		return "", err
	}
	if claim.DeletionTimestamp != nil || claim.Annotations["cleanup.rainbond.io/node-uid"] != nodeUID || claim.Annotations["volume.kubernetes.io/selected-node"] != nodeName || len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteOnce || (claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) {
		return "", ErrBinding
	}
	if settings.StateStorageClass != "" && (claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != settings.StateStorageClass) {
		return "", ErrBinding
	}
	return name, nil
}
