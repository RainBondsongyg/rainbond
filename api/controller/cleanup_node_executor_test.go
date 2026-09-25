package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	"github.com/goodrain/rainbond/api/middleware"
	"github.com/goodrain/rainbond/db/model"
	guard "github.com/goodrain/rainbond/pkg/cleanup"
	"github.com/jinzhu/gorm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// capability_id: rainbond.cleanup.node-executor-admission
func TestNodeAdmissionAPIUsesKubernetesFactsAndGrantsOnce(t *testing.T) {
	database, err := gorm.Open("sqlite3", filepath.Join(t.TempDir(), "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.DB().SetMaxOpenConns(1)
	if err := database.AutoMigrate(&model.CleanupStorage{}, &model.CleanupOperation{}).Error; err != nil {
		t.Fatal(err)
	}
	kube, template, volume := gcAdmissionFixture(t)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid"}}
	if _, err := kube.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	storage, err := guard.ProvisionManagedCacheStorage(database, volume, "/cache/build")
	if err != nil {
		t.Fatal(err)
	}
	// Isolated API fixture: production enrollment never promotes readiness.
	if err := database.Model(&model.CleanupStorage{}).Where("storage_id = ?", storage.StorageID).Update("mode", "ready").Error; err != nil {
		t.Fatal(err)
	}
	intent := guard.NodeJobIntent{Namespace: "system", NodeName: "node", NodeUID: "node-uid", Entry: "entry", Fingerprint: strings.Repeat("a", 64)}
	r, err := guard.ManagedNodeRequest(storage, "executor", "node-http", "request", intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guard.AcquireOperation(database, r); err != nil {
		t.Fatal(err)
	}
	template.Spec.Template.Spec.Containers[0].Name = "node-cleanup"
	template.Spec.Template.Spec.Containers[0].Command = []string{"/app/node-cleanup"}
	template.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = storage.RootPath
	jobs := gcAdmissionKubeClient{Interface: kube}.BatchV1().Jobs("system")
	job, err := guard.SubmitSuspendedNodeJob(context.Background(), database, jobs, r, intent, template)
	if err != nil {
		t.Fatal(err)
	}
	job, err = guard.StartNodeJob(context.Background(), database, jobs, r)
	if err != nil {
		t.Fatal(err)
	}
	yes := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "node-executor", Namespace: "system", UID: "node-pod-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &yes}}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "node-cleanup", ContainerID: "containerd://node-helper", ImageID: job.Spec.Template.Spec.Containers[0].Image, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	if _, err := kube.CoreV1().Pods("system").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	h := &CleanupCoordinationHandler{database: func() *gorm.DB { return database }, gcTarget: func() (kubernetes.Interface, string, string, error) { return kube, "system", "rbd-hub", nil }}
	t.Setenv("TOKEN", "isolated-node-fixture")
	router := chi.NewRouter()
	router.Use(middleware.FullToken)
	router.Post("/v2/cleanup/stores/{storage_id}/operations/{operation_id}/node/enter-job", h.EnterNodeJob)
	endpoint := "/v2/cleanup/stores/" + r.StorageID + "/operations/" + r.OperationID + "/node/enter-job"
	unauth := httptest.NewRecorder()
	router.ServeHTTP(unauth, httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{}`)))
	if unauth.Code != http.StatusUnauthorized && unauth.Code != http.StatusForbidden {
		t.Fatal("unauthenticated request accepted", unauth.Code)
	}
	claimed := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{"image_id":"caller-proof"}`))
	claimed.Header.Set("Authorization", "Token isolated-node-fixture")
	denied := httptest.NewRecorder()
	router.ServeHTTP(denied, claimed)
	if denied.Code != 400 {
		t.Fatal("claimed runtime facts accepted", denied.Code)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	client, err := guard.NewCoordinationClient(server.URL, "isolated-node-fixture", true)
	if err != nil {
		t.Fatal(err)
	}
	locator := guard.NodeExecutorLocator{Pod: pod.Name, PodUID: string(pod.UID)}
	wrong := locator
	wrong.PodUID = "replacement"
	if err := client.EnterNodeJob(context.Background(), r, wrong); err == nil {
		t.Fatal("wrong pod admitted")
	}
	if err := client.EnterNodeJob(context.Background(), r, locator); err != nil {
		t.Fatal(err)
	}
	if err := client.EnterNodeJob(context.Background(), r, locator); err == nil {
		t.Fatal("execution grant repeated")
	}
}
