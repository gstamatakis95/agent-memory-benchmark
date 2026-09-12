package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// isNotFoundErr reports whether err is S3's "object does not exist" outcome,
// in any of the shapes the SDK can produce: a typed *types.NotFound (the
// HeadObject 404, which carries no body to parse a code from) or
// *types.NoSuchKey (GetObject's typical 404 body), or a generic
// smithy.APIError whose code is "NotFound"/"NoSuchKey" (a 404 with an empty
// or non-XML body gets its code synthesized from the HTTP status text).
func isNotFoundErr(err error) bool {
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return true
		}
	}
	return false
}

// publish uploads the sealed tarball to ArtifactKey(version). Artifacts are
// content-addressed and never overwritten: a HeadObject first checks the key
// is absent (docs/07-snapshot-serving.md section 3).
func (st *S3Store) publish(ctx context.Context, version int64, tarPath string) (string, error) {
	key := ArtifactKey(version)

	f, err := os.Open(tarPath)
	if err != nil {
		return "", fmt.Errorf("snapshot: open tarball %s: %w", tarPath, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("snapshot: stat tarball %s: %w", tarPath, err)
	}

	head, err := st.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(st.Bucket),
		Key:    aws.String(key),
	})
	switch {
	case err == nil:
		// Versions come from a Postgres sequence and are never reused, so an
		// object already at this key can only be an earlier attempt of THIS
		// build (the Publish activity retried after the upload completed but
		// before the ledger insert / pointer flip). Same size => same bytes:
		// treat the retry as done. Any other size is a real collision.
		if head.ContentLength != nil && *head.ContentLength == fi.Size() {
			return key, nil
		}
		return "", fmt.Errorf("snapshot: artifact already exists with a different size, never overwritten: %s", key)
	case isNotFoundErr(err):
		// Absent, as expected; proceed to upload.
	default:
		return "", fmt.Errorf("snapshot: head %s: %w", key, err)
	}

	_, err = st.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(st.Bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(fi.Size()),
		ContentType:   aws.String("application/zstd"),
	})
	if err != nil {
		return "", fmt.Errorf("snapshot: put %s: %w", key, err)
	}
	return key, nil
}

// flipPointer writes current.json. Callers MUST only call it after Publish
// returns successfully for p.Key (the atomic cutover).
func (st *S3Store) flipPointer(ctx context.Context, p Pointer) error {
	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("snapshot: marshal pointer: %w", err)
	}
	_, err = st.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(st.Bucket),
		Key:           aws.String(PointerKey),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("snapshot: flip pointer: %w", err)
	}
	return nil
}

// current reads current.json; ok is false when no snapshot was ever
// published.
func (st *S3Store) current(ctx context.Context) (Pointer, bool, error) {
	out, err := st.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(st.Bucket),
		Key:    aws.String(PointerKey),
	})
	if err != nil {
		if isNotFoundErr(err) {
			return Pointer{}, false, nil
		}
		return Pointer{}, false, fmt.Errorf("snapshot: get %s: %w", PointerKey, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return Pointer{}, false, fmt.Errorf("snapshot: read %s: %w", PointerKey, err)
	}
	var p Pointer
	if err := json.Unmarshal(data, &p); err != nil {
		return Pointer{}, false, fmt.Errorf("snapshot: parse %s: %w", PointerKey, err)
	}
	return p, true, nil
}

// readManifestVersion reads a manifest.json at path and returns its Version;
// any error (missing file, bad JSON) is reported to the caller, who treats
// it as "no usable cache".
func readManifestVersion(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, err
	}
	return m.Version, nil
}

// fetch downloads the artifact at p.Key into cacheDir/v<version>/ (creating
// it) and extracts it, returning the extracted directory. It is cached by
// the immutable version key: if the directory already holds a manifest
// whose Version matches, nothing is downloaded. A partial download or
// extraction never leaves a directory that looks complete: work happens in
// temporary siblings under cacheDir, renamed into place only on success.
func (st *S3Store) fetch(ctx context.Context, p Pointer, cacheDir string) (string, error) {
	target := filepath.Join(cacheDir, fmt.Sprintf("v%d", p.Version))
	if v, err := readManifestVersion(filepath.Join(target, ManifestFile)); err == nil && v == p.Version {
		return target, nil // cache hit
	}

	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", fmt.Errorf("snapshot: mkdir cache dir %s: %w", cacheDir, err)
	}

	tmpTarPath, err := downloadToTemp(ctx, st.Client, st.Bucket, p, cacheDir)
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpTarPath)

	tmpDir, err := os.MkdirTemp(cacheDir, fmt.Sprintf(".tmp-v%d-*", p.Version))
	if err != nil {
		return "", fmt.Errorf("snapshot: mkdir temp extract dir: %w", err)
	}
	if err := Unseal(tmpTarPath, tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("snapshot: unseal %s: %w", p.Key, err)
	}

	if err := os.Rename(tmpDir, target); err != nil {
		// Another fetch for the same immutable version may have raced us
		// into place; if target is now a valid v<version> snapshot, use it
		// and discard our own extraction. Otherwise the rename failure is
		// real and must be surfaced.
		if v, mErr := readManifestVersion(filepath.Join(target, ManifestFile)); mErr == nil && v == p.Version {
			os.RemoveAll(tmpDir)
			return target, nil
		}
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("snapshot: rename extracted snapshot into place: %w", err)
	}
	return target, nil
}

// downloadToTemp streams the artifact at p.Key into a temporary file under
// cacheDir and returns its path. The caller owns removing it.
func downloadToTemp(ctx context.Context, client *s3.Client, bucket string, p Pointer, cacheDir string) (string, error) {
	tmp, err := os.CreateTemp(cacheDir, fmt.Sprintf(".tmp-v%d-*.tar.zst", p.Version))
	if err != nil {
		return "", fmt.Errorf("snapshot: create temp tarball: %w", err)
	}
	tmpPath := tmp.Name()

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(p.Key),
	})
	if err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("snapshot: get artifact %s: %w", p.Key, err)
	}

	_, copyErr := io.Copy(tmp, out.Body)
	out.Body.Close()
	closeErr := tmp.Close()
	if copyErr != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("snapshot: download artifact %s: %w", p.Key, copyErr)
	}
	if closeErr != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("snapshot: write temp tarball: %w", closeErr)
	}
	return tmpPath, nil
}
