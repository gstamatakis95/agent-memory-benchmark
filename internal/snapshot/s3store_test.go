package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// testS3Client points an aws-sdk-go-v2 S3 client at an httptest server that
// fakes the handful of S3 calls this package makes (HEAD/GET/PUT), so these
// tests run fast and Docker-free — no MinIO, no network.
func testS3Client(t *testing.T, handler http.HandlerFunc) *s3.Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
		// Keep the wire format a plain, unchunked body so the fake server
		// can read it directly (default checksum behavior can otherwise
		// switch PutObject to streaming/chunked encoding).
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
}

func TestS3StorePublishRefusesExistingArtifact(t *testing.T) {
	const bucket = "bkt"
	version := int64(7)
	key := ArtifactKey(version)
	putCalled := false

	client := testS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/"+bucket+"/"+key:
			w.Header().Set("Content-Length", "123")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			putCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})
	st := &S3Store{Client: client, Bucket: bucket}

	tarPath := filepath.Join(t.TempDir(), "snap.tar.zst")
	if err := os.WriteFile(tarPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Different size than the remote object: a real collision, refused.
	_, err := st.publish(context.Background(), version, tarPath)
	if err == nil {
		t.Fatal("expected publish to refuse an existing artifact of a different size")
	}
	if putCalled {
		t.Fatal("publish must not upload when the artifact already exists")
	}

	// Same size: a retried Publish of this very build; idempotent success.
	if err := os.WriteFile(tarPath, bytes.Repeat([]byte("x"), 123), 0o644); err != nil {
		t.Fatal(err)
	}
	gotKey, err := st.publish(context.Background(), version, tarPath)
	if err != nil {
		t.Fatalf("publish retry with identical size must succeed: %v", err)
	}
	if gotKey != key || putCalled {
		t.Fatalf("retry: key=%q putCalled=%v", gotKey, putCalled)
	}
}

func TestS3StorePublishUploadsWhenAbsent(t *testing.T) {
	const bucket = "bkt"
	version := int64(9)
	key := ArtifactKey(version)
	const content = "hello snapshot"

	var putBody []byte
	var gotContentType string
	client := testS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read put body: %v", err)
			}
			putBody = b
			gotContentType = r.Header.Get("Content-Type")
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})
	st := &S3Store{Client: client, Bucket: bucket}

	tarPath := filepath.Join(t.TempDir(), "snap.tar.zst")
	if err := os.WriteFile(tarPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	gotKey, err := st.publish(context.Background(), version, tarPath)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if gotKey != key {
		t.Fatalf("key = %q, want %q", gotKey, key)
	}
	if string(putBody) != content {
		t.Fatalf("uploaded body = %q, want %q", putBody, content)
	}
	if gotContentType != "application/zstd" {
		t.Fatalf("content type = %q, want application/zstd", gotContentType)
	}
}

func TestS3StoreCurrentNotFound(t *testing.T) {
	client := testS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	st := &S3Store{Client: client, Bucket: "bkt"}

	p, ok, err := st.current(context.Background())
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false (nothing published); got %+v", p)
	}
}

func TestS3StoreCurrentParsesPointer(t *testing.T) {
	want := Pointer{Version: 3, Key: "snapshots/snapshot-v3.tar.zst"}
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}

	client := testS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	st := &S3Store{Client: client, Bucket: "bkt"}

	got, ok, err := st.current(context.Background())
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if got != want {
		t.Fatalf("current() = %+v, want %+v", got, want)
	}
}

func TestS3StoreFlipPointerUploadsJSON(t *testing.T) {
	var putBody []byte
	var gotPath, gotContentType string
	client := testS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method %s", r.Method)
		}
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		putBody = b
		w.WriteHeader(http.StatusOK)
	})
	st := &S3Store{Client: client, Bucket: "bkt"}

	p := Pointer{Version: 5, Key: ArtifactKey(5)}
	if err := st.flipPointer(context.Background(), p); err != nil {
		t.Fatalf("flipPointer: %v", err)
	}
	if gotPath != "/bkt/"+PointerKey {
		t.Fatalf("path = %q, want %q", gotPath, "/bkt/"+PointerKey)
	}
	if gotContentType != "application/json" {
		t.Fatalf("content type = %q, want application/json", gotContentType)
	}
	var got Pointer
	if err := json.Unmarshal(putBody, &got); err != nil {
		t.Fatalf("unmarshal put body: %v", err)
	}
	if got != p {
		t.Fatalf("uploaded pointer = %+v, want %+v", got, p)
	}
}

// TestS3StoreFetchCacheHit verifies fetch() short-circuits (no S3 call at
// all) when cacheDir/v<version>/manifest.json already reports the requested
// version. This deliberately avoids exercising the download+Unseal path,
// which depends on the artifact side of this package (builder.go/open.go/
// seal.go), owned by another agent.
func TestS3StoreFetchCacheHit(t *testing.T) {
	client := testS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("fetch should not touch S3 on a cache hit; got %s %s", r.Method, r.URL.Path)
	})
	st := &S3Store{Client: client, Bucket: "bkt"}

	cacheDir := t.TempDir()
	version := int64(4)
	target := filepath.Join(cacheDir, "v4")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(Manifest{Version: version, DocCount: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, ManifestFile), manifest, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := st.fetch(context.Background(), Pointer{Version: version, Key: ArtifactKey(version)}, cacheDir)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got != target {
		t.Fatalf("fetch returned %q, want %q", got, target)
	}
}

// TestS3StoreFetchCacheMissWrongVersion verifies a stale directory (present
// but for a different version) is not treated as a cache hit — fetch must
// go on to contact S3 rather than silently serving mismatched content.
func TestS3StoreFetchCacheMissWrongVersion(t *testing.T) {
	called := false
	client := testS3Client(t, func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNotFound) // stand-in failure is fine; we only assert the attempt happened
	})
	st := &S3Store{Client: client, Bucket: "bkt"}

	cacheDir := t.TempDir()
	target := filepath.Join(cacheDir, "v4")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(Manifest{Version: 999}) // wrong version
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, ManifestFile), manifest, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = st.fetch(context.Background(), Pointer{Version: 4, Key: ArtifactKey(4)}, cacheDir)
	if err == nil {
		t.Fatal("expected an error since the fake GetObject 404s")
	}
	if !called {
		t.Fatal("fetch should have gone to S3 since the cached manifest version did not match")
	}
}
