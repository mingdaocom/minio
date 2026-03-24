// Copyright (c) 2015-2026 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package logger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestWriterCloseFlushesWithoutRotate(t *testing.T) {
	var seq atomic.Int32
	dir := t.TempDir()
	writer, err := NewDir(Options{
		Directory:       dir,
		MaximumFileSize: 1024,
		FileNameFunc: func() string {
			return fmt.Sprintf("minio-%d.log", seq.Add(1))
		},
	})
	if err != nil {
		t.Fatalf("NewDir failed: %v", err)
	}

	payload := []byte("hello logrotate\n")
	if _, err = writer.Write(payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err = writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err = writer.Write([]byte("after close")); err == nil {
		t.Fatal("expected write after close to fail")
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 log file after close, got %d", len(files))
	}

	data, err := os.ReadFile(filepath.Join(dir, files[0].Name()))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("expected flushed payload %q, got %q", payload, data)
	}
}

func TestWriterCloseFlushesAcrossRotations(t *testing.T) {
	var seq atomic.Int32
	dir := t.TempDir()
	writer, err := NewDir(Options{
		Directory:       dir,
		MaximumFileSize: 16,
		FileNameFunc: func() string {
			return fmt.Sprintf("minio-%03d.log", seq.Add(1))
		},
	})
	if err != nil {
		t.Fatalf("NewDir failed: %v", err)
	}

	payload := []byte("0123456789abcdef0123456789abcdeftail")
	if _, err = writer.Write(payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err = writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(files) < 2 {
		t.Fatalf("expected multiple rotated log files, got %d", len(files))
	}

	var combined bytes.Buffer
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			t.Fatalf("Info failed for %s: %v", file.Name(), err)
		}
		if info.Size() == 0 {
			t.Fatalf("unexpected empty rotated log file: %s", file.Name())
		}

		data, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			t.Fatalf("ReadFile failed for %s: %v", file.Name(), err)
		}
		combined.Write(data)
	}

	if !bytes.Equal(combined.Bytes(), payload) {
		t.Fatalf("expected combined payload %q, got %q", payload, combined.Bytes())
	}
}
