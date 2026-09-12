package snapshot

// Seal/Unseal turn a finished snapshot directory into (and back out of) the
// single tar.zst artifact that gets published to S3 (package doc comment;
// docs/01-retrieval.md section 4.7). Entries are written in WalkDir's
// already-lexical order with a fixed mtime/mode so sealing the same
// directory twice byte-for-byte reproduces the same archive — that's what
// lets a caller diff two builds of the same version for reproducibility.

import (
	"archive/tar"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// fixedModTime is stamped on every tar entry instead of the real mtime, and
// fixedFileMode/fixedDirMode replace the real (umask-dependent) mode bits,
// so two builds of byte-identical content produce byte-identical archives.
var fixedModTime = time.Unix(0, 0)

const (
	fixedFileMode = 0o644
	fixedDirMode  = 0o755
)

// seal is Seal's implementation (see api.go for the contract).
func seal(dir, tarPath string) error {
	out, err := os.Create(tarPath)
	if err != nil {
		return err
	}
	defer out.Close()

	zw, err := zstd.NewWriter(out)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zw)

	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir {
			return nil // don't write a "." entry for the root itself
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)

		if d.IsDir() {
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeDir,
				Name:     name + "/",
				Mode:     fixedDirMode,
				ModTime:  fixedModTime,
			})
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("snapshot: seal: %s is not a regular file or directory", path)
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Size:     info.Size(),
			Mode:     fixedFileMode,
			ModTime:  fixedModTime,
		}); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return err
	}

	if err := tw.Close(); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return out.Close()
}

// unseal is Unseal's implementation (see api.go for the contract).
func unseal(tarPath, dir string) error {
	if err := os.MkdirAll(dir, fixedDirMode); err != nil {
		return err
	}

	in, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer in.Close()

	zr, err := zstd.NewReader(in)
	if err != nil {
		return err
	}
	defer zr.Close()

	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		target, err := safeJoin(dir, hdr.Name)
		if err != nil {
			return err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, fixedDirMode); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), fixedDirMode); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fixedFileMode)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(f, tr)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("snapshot: unseal: unsupported tar entry type %v for %q", hdr.Typeflag, hdr.Name)
		}
	}
}

// safeJoin resolves name (a tar entry path, always forward-slashed) against
// dir and rejects anything that would escape it — an absolute path or a
// ".." component, whether literal or produced by cleaning.
func safeJoin(dir, name string) (string, error) {
	if strings.Contains(name, "\x00") {
		return "", fmt.Errorf("snapshot: unseal: invalid entry name %q", name)
	}
	cleaned := filepath.Clean(filepath.FromSlash(name))
	if filepath.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("snapshot: unseal: entry %q escapes the target directory", name)
	}
	return filepath.Join(dir, cleaned), nil
}
