package kubeidentity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// RegistryReadMount contains only references to target-cluster storage.
type RegistryReadMount struct {
	Claim        string            `json:"claim"`
	SubPath      string            `json:"subPath"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// EnsureRegistryReadMount gives the plugin a read-only view of the observed
// Registry data. The mirror is retained and never owns or reclaims source data.
func EnsureRegistryReadMount(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, prepared RegistryPreparation, targetNamespace string) (RegistryReadMount, error) {
	denied := RegistryReadMount{}
	if client == nil || pod == nil || len(validation.IsDNS1123Label(targetNamespace)) != 0 || string(pod.UID) != prepared.Mount.PodUID || pod.ResourceVersion != prepared.Mount.PodVersion {
		return denied, ErrBinding
	}
	container, err := containerByName(pod, prepared.Container)
	if err != nil {
		return denied, err
	}
	mount, relative, err := effectiveMount(container, prepared.Root)
	if err != nil {
		return denied, err
	}
	current, err := inspectVolume(ctx, client, pod, mount.Name, relative)
	if err != nil || current.VolumeUID != prepared.Mount.VolumeUID {
		return denied, ErrBinding
	}
	var volume *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == mount.Name {
			volume = &pod.Spec.Volumes[i]
		}
	}
	if volume == nil || volume.PersistentVolumeClaim == nil {
		return denied, ErrBinding
	}
	sourceClaim, err := client.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, volume.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
	if err != nil {
		return denied, err
	}
	source, err := client.CoreV1().PersistentVolumes().Get(ctx, sourceClaim.Spec.VolumeName, metav1.GetOptions{})
	if err != nil || source.UID == "" || source.DeletionTimestamp != nil {
		return denied, ErrBinding
	}
	copy := source.DeepCopy()
	if copy.Spec.NFS == nil && copy.Spec.HostPath == nil && copy.Spec.Local == nil && copy.Spec.CSI == nil {
		return denied, ErrBinding
	}
	if copy.Spec.NFS != nil {
		copy.Spec.NFS.ReadOnly = true
	}
	if copy.Spec.CSI != nil {
		copy.Spec.CSI.ReadOnly = true
	}
	if copy.Spec.HostPath != nil {
		directory := corev1.HostPathDirectory
		copy.Spec.HostPath.Type = &directory
	}
	selector := map[string]string(nil)
	if copy.Spec.HostPath != nil || copy.Spec.Local != nil || copy.Spec.CSI != nil {
		node, err := client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil || node.UID == "" || node.DeletionTimestamp != nil || node.Labels["kubernetes.io/hostname"] == "" {
			return denied, ErrBinding
		}
		selector = map[string]string{"kubernetes.io/hostname": node.Labels["kubernetes.io/hostname"]}
		if copy.Spec.NodeAffinity == nil {
			copy.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{node.Labels["kubernetes.io/hostname"]}}}}}}}
		}
	}
	hash := sha256.Sum256([]byte(prepared.Mount.VolumeUID + "\x00" + targetNamespace))
	name := "rainbond-disk-registry-" + hex.EncodeToString(hash[:6])
	annotations := map[string]string{"cleanup.rainbond.io/source-volume-uid": prepared.Mount.VolumeUID}
	meta := metav1.ObjectMeta{Name: name, Labels: map[string]string{"cleanup.rainbond.io/managed-by": installationManager}, Annotations: annotations}
	desired := &corev1.PersistentVolume{ObjectMeta: meta, Spec: corev1.PersistentVolumeSpec{Capacity: copy.Spec.Capacity, PersistentVolumeSource: copy.Spec.PersistentVolumeSource, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, StorageClassName: "", VolumeMode: copy.Spec.VolumeMode, NodeAffinity: copy.Spec.NodeAffinity, ClaimRef: &corev1.ObjectReference{Namespace: targetNamespace, Name: name}}}
	if copy.Spec.NFS != nil || copy.Spec.CSI != nil {
		for _, option := range copy.Spec.MountOptions {
			if option != "rw" && option != "ro" {
				desired.Spec.MountOptions = append(desired.Spec.MountOptions, option)
			}
		}
		desired.Spec.MountOptions = append(desired.Spec.MountOptions, "ro")
	}
	volumes := client.CoreV1().PersistentVolumes()
	existing, err := volumes.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		existing, err = volumes.Create(ctx, desired, metav1.CreateOptions{})
	}
	if err != nil {
		return denied, err
	}
	if existing.DeletionTimestamp != nil || existing.Annotations["cleanup.rainbond.io/source-volume-uid"] != prepared.Mount.VolumeUID || !reflect.DeepEqual(existing.Spec.PersistentVolumeSource, desired.Spec.PersistentVolumeSource) || existing.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain || existing.Spec.ClaimRef == nil || existing.Spec.ClaimRef.Namespace != targetNamespace || existing.Spec.ClaimRef.Name != name {
		return denied, ErrBinding
	}
	claims := client.CoreV1().PersistentVolumeClaims(targetNamespace)
	claim, err := claims.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		class := ""
		meta.Namespace = targetNamespace
		claim, err = claims.Create(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: meta, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}, VolumeName: name, StorageClassName: &class, VolumeMode: copy.Spec.VolumeMode, Resources: corev1.VolumeResourceRequirements{Requests: copy.Spec.Capacity}}}, metav1.CreateOptions{})
	}
	if err != nil {
		return denied, err
	}
	if claim.DeletionTimestamp != nil || claim.Spec.VolumeName != name || claim.Annotations["cleanup.rainbond.io/source-volume-uid"] != prepared.Mount.VolumeUID {
		return denied, ErrBinding
	}
	return RegistryReadMount{Claim: name, SubPath: path.Join(relative, "docker/registry/v2"), NodeSelector: selector}, nil
}
