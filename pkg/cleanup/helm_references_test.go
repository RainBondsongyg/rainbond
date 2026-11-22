package cleanup

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func helmReferenceFixture(t *testing.T, manifest string, compressed bool) string {
	t.Helper()
	raw, err := json.Marshal(map[string]interface{}{"name": "app", "namespace": "team", "version": 2, "manifest": manifest, "hooks": []map[string]string{{"manifest": "apiVersion: batch/v1\nkind: Job\nspec:\n  template:\n    spec:\n      containers:\n      - image: goodrain.me/hooks:v1\n"}}, "config": map[string]string{"fixture-private-value": "must-not-return"}})
	if err != nil {
		t.Fatal(err)
	}
	if compressed {
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		writer.Write(raw)
		writer.Close()
		raw = buf.Bytes()
	}
	return base64.StdEncoding.EncodeToString(raw)
}
func TestHelmRetainedManifestAndHooksProvideReferencesOnly(t *testing.T) {
	manifest := "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n      containers:\n      - image: goodrain.me/retained:v2\n"
	for _, compressed := range []bool{false, true} {
		result := InspectHelmReleaseReferences(helmReferenceFixture(t, manifest, compressed), "app", "team", 2)
		if !result.Complete || len(result.Images) != 2 || strings.Contains(strings.Join(result.Images, " "), "must-not-return") {
			t.Fatal("missing or leaking retained references", result.Complete, len(result.Images))
		}
	}
}
func TestHelmCorruptForeignAndUnknownReleasesAreNotEmptySuccess(t *testing.T) {
	encoded := helmReferenceFixture(t, "apiVersion: example.test/v1\nkind: UnresolvedWorkload\n", true)
	if InspectHelmReleaseReferences(encoded, "app", "team", 2).Complete {
		t.Fatal("unknown workload ignored")
	}
	for _, value := range []string{"!", base64.StdEncoding.EncodeToString([]byte("{}")), strings.Repeat("a", (1<<20)+1)} {
		if InspectHelmReleaseReferences(value, "app", "team", 2).Complete {
			t.Fatal("invalid record became empty success")
		}
	}
	encoded = helmReferenceFixture(t, "", true)
	for _, scope := range []struct {
		name, namespace string
		revision        int
	}{{"other", "team", 2}, {"app", "other", 2}, {"app", "team", 3}} {
		if InspectHelmReleaseReferences(encoded, scope.name, scope.namespace, scope.revision).Complete {
			t.Fatal("foreign release accepted")
		}
	}
}

func TestHelmReferenceDecompressionIsBounded(t *testing.T) {
	encoded := helmReferenceFixture(t, strings.Repeat("x", 9<<20), true)
	if InspectHelmReleaseReferences(encoded, "app", "team", 2).Complete {
		t.Fatal("oversized expanded record accepted")
	}
}

func TestHelmRenderedEmptyDocumentsDoNotHideWorkloadReferences(t *testing.T) {
	manifest := "---\n# disabled optional resource\n---\napiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n      containers:\n      - image: goodrain.me/retained:v2\n---\n# disabled optional hook\n"
	result := InspectHelmReleaseReferences(helmReferenceFixture(t, manifest, true), "app", "team", 2)
	if !result.Complete || len(result.Images) != 2 {
		t.Fatal("empty Helm documents blocked valid workload references", result.Complete, result.Images)
	}
	for _, content := range []string{"---\n# comments only\n---\n", "{}", "---\n{}\n"} {
		if InspectHelmReleaseReferences(helmReferenceFixture(t, content, true), "app", "team", 2).Complete {
			t.Fatalf("content without a Kubernetes object was accepted: %q", content)
		}
	}
}

func TestHelmStandardSupportingResourcesDoNotInventImageReferences(t *testing.T) {
	for _, resource := range []struct{ version, kind string }{
		{"networking.k8s.io/v1", "IngressClass"},
		{"storage.k8s.io/v1", "StorageClass"},
		{"coordination.k8s.io/v1", "Lease"},
		{"apiregistration.k8s.io/v1", "APIService"},
		{"admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration"},
		{"admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration"},
		{"policy/v1beta1", "PodDisruptionBudget"},
		{"autoscaling/v2beta2", "HorizontalPodAutoscaler"},
		{"gateway.networking.k8s.io/v1beta1", "Gateway"},
		{"scheduling.k8s.io/v1", "PriorityClass"},
	} {
		t.Run(resource.kind, func(t *testing.T) {
			manifest := "apiVersion: " + resource.version + "\nkind: " + resource.kind + "\nmetadata:\n  name: supporting-resource\n"
			result := InspectHelmReleaseReferences(helmReferenceFixture(t, manifest, true), "app", "team", 2)
			if !result.Complete || len(result.Images) != 1 || result.Images[0] != "goodrain.me/hooks:v1" {
				t.Fatal("standard supporting resource blocked the retained hook reference", result.Complete, result.Images)
			}
			foreign := strings.Replace(manifest, resource.version, "example.test/v1", 1)
			if InspectHelmReleaseReferences(helmReferenceFixture(t, foreign, true), "app", "team", 2).Complete {
				t.Fatal("foreign resource with a standard kind was ignored")
			}
		})
	}
}

func TestSavedMeshMetadataInjectionDoesNotInventAnImage(t *testing.T) {
	manifest := "apiVersion: rainbond.io/v1alpha1\nkind: ServiceMeshClass\ninjectMethods:\n- method: label\n  name: sidecar.istio.io/inject\n  value: 'true'\n  cover: true\n"
	result := InspectHelmReleaseReferences(helmReferenceFixture(t, manifest, true), "app", "team", 2)
	if !result.Complete || len(result.Images) != 1 {
		t.Fatal("metadata-only mesh class blocked known hook references", result.Complete, result.Images)
	}
	for _, changed := range []string{
		strings.Replace(manifest, "method: label", "method: container", 1),
		manifest + "image: goodrain.me/unknown:v1\n",
	} {
		if InspectHelmReleaseReferences(helmReferenceFixture(t, changed, true), "app", "team", 2).Complete {
			t.Fatal("unresolved mesh image injection was ignored")
		}
	}
}

func TestRetainedRainbondChartProtectsDeclaredComponentImages(t *testing.T) {
	manifest := `apiVersion: v1
kind: List
items:
- apiVersion: rainbond.io/v1alpha1
  kind: RainbondCluster
  spec:
    rainbondImageRepository: registry.example/platform
    installVersion: retained
    sentinelImage: registry.example/sentinel:retained
- apiVersion: rainbond.io/v1alpha1
  kind: RbdComponent
  metadata:
    name: rbd-api
  spec:
    image: registry.example/rbd-api:retained
`
	result := InspectHelmReleaseReferences(helmReferenceFixture(t, manifest, true), "app", "team", 2)
	want := []string{"goodrain.me/hooks:v1", "registry.example/rbd-api:retained", "registry.example/sentinel:retained"}
	if !result.Complete || strings.Join(result.Images, "\n") != strings.Join(want, "\n") {
		t.Fatal("retained Rainbond component images were not protected", result.Complete, result.Images)
	}
	for _, invalid := range []string{
		strings.Replace(manifest, "image: registry.example/rbd-api:retained", "image: ''", 1),
		strings.ReplaceAll(manifest, "rainbond.io/v1alpha1", "example.test/v1"),
	} {
		if InspectHelmReleaseReferences(helmReferenceFixture(t, invalid, true), "app", "team", 2).Complete {
			t.Fatal("invalid component reference was accepted")
		}
	}
}
