package controller

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	"github.com/goodrain/rainbond/api/middleware"
	"github.com/goodrain/rainbond/db/model"
	guard "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/goodrain/rainbond/pkg/cleanup/kubeidentity"
	"github.com/jinzhu/gorm"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedbatch "k8s.io/client-go/kubernetes/typed/batch/v1"
)

// capability_id: rainbond.cleanup.gc-job-execution-api
func TestGCJobAdmissionAPIRequiresVerifiedExecutorAndGrantsOnce(t *testing.T) {
	database, err := gorm.Open("sqlite3", filepath.Join(t.TempDir(), "gc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.DB().SetMaxOpenConns(1)
	if err := database.AutoMigrate(&model.CleanupStorage{}, &model.CleanupOperation{}).Error; err != nil {
		t.Fatal(err)
	}
	kube, template, volume := gcAdmissionFixture(t)
	binding := guard.StorageRegistration{StorageID: "store", Generation: "gen", VolumeUID: volume, RootPath: "/registry"}
	if err := guard.RegisterStorage(database, binding); err != nil {
		t.Fatal(err)
	}
	if err := database.Model(&model.CleanupStorage{}).Where("storage_id = ?", "store").Update("mode", "ready").Error; err != nil {
		t.Fatal(err)
	}
	r := guard.CoordinationRequest{StorageID: "store", Generation: "gen", OperationID: "gc-op", Kind: "gc", Scope: "*", Owner: "executor", Fingerprint: "request"}
	if _, err := guard.RequestMaintenance(database, r); err != nil {
		t.Fatal(err)
	}
	// An unprepared/foreign pod must fail before any execution grant.
	h := &CleanupCoordinationHandler{database: func() *gorm.DB { return database }, gcTarget: func() (kubernetes.Interface, string, string, error) { return kube, "system", "rbd-hub", nil }}
	t.Setenv("TOKEN", "gc-isolated-fixture")
	router := chi.NewRouter()
	router.Use(middleware.FullToken)
	router.Post("/v2/cleanup/stores/{storage_id}/operations/{operation_id}/maintenance/enter-job", h.EnterGCJob)
	server := httptest.NewServer(router)
	defer server.Close()
	client, err := guard.NewCoordinationClient(server.URL, "gc-isolated-fixture", true)
	if err != nil {
		t.Fatal(err)
	}
	locator := guard.GCExecutorLocator{Pod: "gc-pod", PodUID: "gc-pod-uid"}
	if err := client.EnterGCJob(context.Background(), r, locator); err == nil {
		t.Fatal("unprepared executor admitted")
	}
	state, err := guard.InspectOperation(database, r)
	if err != nil || state != "draining" {
		t.Fatal(state, err)
	}
	// The route must be protected even when ordinary region token auth is optional.
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest("POST", "/v2/cleanup/stores/store/operations/gc-op/maintenance/enter-job", strings.NewReader("{}")))
	if response.Code != 401 {
		t.Fatal("unauthenticated admission", response.Code)
	}
	// Store and job identity validation still runs even with valid internal auth.
	job, err := guard.SubmitSuspendedGCJob(context.Background(), database, gcAdmissionJobClient{JobInterface: kube.BatchV1().Jobs("system")}, r, template)
	if err != nil {
		t.Fatal(err)
	}
	no, yes := false, true
	job.Spec.Suspend = &no
	if _, err := kube.BatchV1().Jobs("system").Update(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: locator.Pod, Namespace: "system", UID: "gc-pod-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &yes}}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "gc", ImageID: job.Spec.Template.Spec.Containers[0].Image, ContainerID: "containerd://owned", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	if _, err := kube.CoreV1().Pods("system").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	writer := guard.CoordinationRequest{StorageID: "store", Generation: "gen", OperationID: "old-writer", Owner: "builder", Kind: "producer", Scope: "*", Fingerprint: "old-request"}
	// Simulate an operation accepted before the maintenance request.
	if err := database.Create(&model.CleanupOperation{StorageID: writer.StorageID, Generation: writer.Generation, OperationID: writer.OperationID, Owner: writer.Owner, Kind: writer.Kind, Scope: writer.Scope, Fingerprint: writer.Fingerprint, State: "active"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := client.EnterGCJob(context.Background(), r, locator); err == nil {
		t.Fatal("active writer was ignored")
	}
	if err := guard.FinishOperation(database, writer, true); err != nil {
		t.Fatal(err)
	}
	if err := client.EnterGCJob(context.Background(), r, guard.GCExecutorLocator{Pod: locator.Pod, PodUID: "replacement"}); err == nil {
		t.Fatal("foreign executor admitted")
	}
	if err := client.EnterGCJob(context.Background(), r, locator); err != nil {
		t.Fatal(err)
	}
	if err := client.EnterGCJob(context.Background(), r, locator); err == nil {
		t.Fatal("execution grant replayed")
	}
	if err := guard.EnterMaintenance(database, r); err == nil {
		t.Fatal("generic admission bypassed Job binding")
	}
	state, err = guard.InspectOperation(database, r)
	if err != nil || state != "exclusive" {
		t.Fatal(state, err)
	}
}

func gcAdmissionFixture(t *testing.T) (*fake.Clientset, *batchv1.Job, string) {
	t.Helper()
	no := false
	native := corev1.Container{Name: "gc", Image: "example.test/owned@sha256:" + strings.Repeat("a", 64), Command: []string{"/registry-gc"}, VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/registry", SubPath: "owned"}}}
	spec := corev1.PodSpec{NodeName: "node", RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &no, Containers: []corev1.Container{native}, Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "system"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: spec}}}
	hub := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "hub", Namespace: "system", UID: "hub-uid", Labels: map[string]string{"app": "hub"}}, Spec: *spec.DeepCopy()}
	hub.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", Value: "/registry"}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "rbd-hub", Namespace: "system"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "hub"}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "system", UID: "pvc-uid"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: "pv-uid"}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: "system", Name: "data", UID: "pvc-uid"}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	client := fake.NewSimpleClientset(hub, svc, pvc, pv)
	observed, err := kubeidentity.InspectNativeRegistry(context.Background(), client, "system", "rbd-hub", "hub", "hub-uid")
	if err != nil {
		t.Fatal(err)
	}
	return client, job, observed.Mount.VolumeUID
}

type gcAdmissionJobClient struct{ typedbatch.JobInterface }

func (c gcAdmissionJobClient) Create(ctx context.Context, job *batchv1.Job, options metav1.CreateOptions) (*batchv1.Job, error) {
	observed := job.DeepCopy()
	if len(options.DryRun) > 0 {
		return observed, nil
	}
	observed.UID = "job-uid"
	observed.ResourceVersion = "1"
	return c.JobInterface.Create(ctx, observed, options)
}
