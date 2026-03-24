// Copyright (c) 2015-2021 MinIO, Inc.
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

package cmd

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/grid"
	xnet "github.com/minio/pkg/v3/net"
)

// Storage REST server, storageRESTReceiver and StorageRESTClient are
// inter-dependent, below test functions are sufficient to test all of them.
func testStorageAPIDiskInfo(t *testing.T, storage StorageAPI) {
	testCases := []struct {
		expectErr bool
	}{
		{true},
	}

	for i, testCase := range testCases {
		_, err := storage.DiskInfo(t.Context(), DiskInfoOptions{Metrics: true})
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
		if err != errUnformattedDisk {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, errUnformattedDisk, err)
		}
	}
}

func testStorageAPIStatInfoFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", pathJoin("myobject", xlStorageFormatFile), []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		objectName string
		expectErr  bool
	}{
		{"foo", "myobject", false},
		// file not found error.
		{"foo", "yourobject", true},
	}

	for i, testCase := range testCases {
		_, err := storage.StatInfoFile(t.Context(), testCase.volumeName, testCase.objectName+"/"+xlStorageFormatFile, false)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v, err: %v", i+1, expectErr, testCase.expectErr, err)
		}
	}
}

func testStorageAPIListDir(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "path/to/myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		prefix         string
		expectedResult []string
		expectErr      bool
	}{
		{"foo", "path", []string{"to/"}, false},
		// prefix not found error.
		{"foo", "nodir", nil, true},
	}

	for i, testCase := range testCases {
		result, err := storage.ListDir(t.Context(), "", testCase.volumeName, testCase.prefix, -1)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testStorageAPIReadAll(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		objectName     string
		expectedResult []byte
		expectErr      bool
	}{
		{"foo", "myobject", []byte("foo"), false},
		// file not found error.
		{"foo", "yourobject", nil, true},
	}

	for i, testCase := range testCases {
		result, err := storage.ReadAll(t.Context(), testCase.volumeName, testCase.objectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func testStorageAPIReadFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		objectName     string
		offset         int64
		expectedResult []byte
		expectErr      bool
	}{
		{"foo", "myobject", 0, []byte("foo"), false},
		{"foo", "myobject", 1, []byte("oo"), false},
		// file not found error.
		{"foo", "yourobject", 0, nil, true},
	}

	result := make([]byte, 100)
	for i, testCase := range testCases {
		result = result[testCase.offset:3]
		_, err := storage.ReadFile(t.Context(), testCase.volumeName, testCase.objectName, testCase.offset, result, nil)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func testStorageAPIAppendFile(t *testing.T, storage StorageAPI) {
	testData := []byte("foo")
	testCases := []struct {
		volumeName      string
		objectName      string
		data            []byte
		expectErr       bool
		ignoreIfWindows bool
	}{
		{"foo", "myobject", testData, false, false},
		{"foo", "myobject-0byte", []byte{}, false, false},
		// volume not found error.
		{"foo-bar", "myobject", testData, true, false},
		// Test some weird characters over the wire.
		{"foo", "newline\n", testData, false, true},
		{"foo", "newline\t", testData, false, true},
		{"foo", "newline \n", testData, false, true},
		{"foo", "newline$$$\n", testData, false, true},
		{"foo", "newline%%%\n", testData, false, true},
		{"foo", "newline \t % $ & * ^ # @ \n", testData, false, true},
		{"foo", "\n\tnewline \t % $ & * ^ # @ \n", testData, false, true},
	}

	for i, testCase := range testCases {
		if testCase.ignoreIfWindows && runtime.GOOS == "windows" {
			continue
		}
		err := storage.AppendFile(t.Context(), testCase.volumeName, testCase.objectName, testCase.data)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			data, err := storage.ReadAll(t.Context(), testCase.volumeName, testCase.objectName)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(data, testCase.data) {
				t.Fatalf("case %v: expected %v, got %v", i+1, testCase.data, data)
			}
		}
	}
}

func testStorageAPIDeleteFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName string
		objectName string
		expectErr  bool
	}{
		{"foo", "myobject", false},
		// file not found not returned
		{"foo", "myobject", false},
		// file not found not returned
		{"foo", "yourobject", false},
	}

	for i, testCase := range testCases {
		err := storage.Delete(t.Context(), testCase.volumeName, testCase.objectName, DeleteOptions{
			Recursive: false,
			Immediate: false,
		})
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testStorageAPIRenameFile(t *testing.T, storage StorageAPI) {
	err := storage.AppendFile(t.Context(), "foo", "myobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = storage.AppendFile(t.Context(), "foo", "otherobject", []byte("foo"))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		volumeName     string
		objectName     string
		destVolumeName string
		destObjectName string
		expectErr      bool
	}{
		{"foo", "myobject", "foo", "yourobject", false},
		{"foo", "yourobject", "bar", "myobject", false},
		// overwrite.
		{"foo", "otherobject", "bar", "myobject", false},
	}

	for i, testCase := range testCases {
		err := storage.RenameFile(t.Context(), testCase.volumeName, testCase.objectName, testCase.destVolumeName, testCase.destObjectName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestKeepHTTPResponseAliveRoundTripSuccess(t *testing.T) {
	rec := httptest.NewRecorder()
	done := keepHTTPResponseAlive(rec)
	done(nil)

	payload := []byte("response payload")
	if _, err := rec.Write(payload); err != nil {
		t.Fatalf("unable to write payload: %v", err)
	}

	reader, err := waitForHTTPResponse(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("waitForHTTPResponse failed: %v", err)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unable to read payload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("expected payload %q, got %q", payload, got)
	}
}

func TestKeepHTTPResponseAliveRoundTripError(t *testing.T) {
	rec := httptest.NewRecorder()
	done := keepHTTPResponseAlive(rec)
	wantErr := errors.New("response failed")
	done(wantErr)

	if _, err := waitForHTTPResponse(bytes.NewReader(rec.Body.Bytes())); err == nil || err.Error() != wantErr.Error() {
		t.Fatalf("expected error %q, got %v", wantErr, err)
	}
}

func TestKeepHTTPReqResponseAliveBodyReadAndCloseRelease(t *testing.T) {
	testCases := []struct {
		name    string
		release func(io.ReadCloser) error
	}{
		{
			name: "read",
			release: func(body io.ReadCloser) error {
				_, err := io.ReadAll(body)
				return err
			},
		},
		{
			name: "close",
			release: func(body io.ReadCloser) error {
				return body.Close()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://example.com", strings.NewReader("request body"))
			rec := httptest.NewRecorder()
			done, body := keepHTTPReqResponseAlive(rec, req)

			if err := tc.release(body); err != nil {
				t.Fatalf("unable to release request body: %v", err)
			}

			done(nil)

			if got := rec.Body.Bytes(); !bytes.Equal(got, []byte{0}) {
				t.Fatalf("expected response sentinel 0, got %v", got)
			}
		})
	}
}

func TestStreamHTTPResponseRoundTrip(t *testing.T) {
	rec := httptest.NewRecorder()
	resp := streamHTTPResponse(rec)

	blocks := [][]byte{[]byte("first block"), []byte("second block")}
	for i, block := range blocks {
		n, err := resp.Write(block)
		if err != nil {
			t.Fatalf("write block %d failed: %v", i+1, err)
		}
		if n != len(block) {
			t.Fatalf("write block %d: expected %d bytes written, got %d", i+1, len(block), n)
		}
	}
	resp.CloseWithError(nil)

	var got bytes.Buffer
	if err := waitForHTTPStream(io.NopCloser(bytes.NewReader(rec.Body.Bytes())), &got); err != nil {
		t.Fatalf("waitForHTTPStream failed: %v", err)
	}

	want := append(append([]byte{}, blocks[0]...), blocks[1]...)
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("expected payload %q, got %q", want, got.Bytes())
	}
}

func TestStreamHTTPResponseRoundTripError(t *testing.T) {
	rec := httptest.NewRecorder()
	resp := streamHTTPResponse(rec)
	wantErr := errors.New("stream failed")
	resp.CloseWithError(wantErr)

	var got bytes.Buffer
	if err := waitForHTTPStream(io.NopCloser(bytes.NewReader(rec.Body.Bytes())), &got); err == nil || err.Error() != wantErr.Error() {
		t.Fatalf("expected error %q, got %v", wantErr, err)
	}
}

func newStorageRESTHTTPServerClient(t testing.TB) *storageRESTClient {
	// Grid with 2 hosts
	tg, err := grid.SetupTestGrid(2)
	if err != nil {
		t.Fatalf("SetupTestGrid: %v", err)
	}
	t.Cleanup(tg.Cleanup)
	prevHost, prevPort := globalMinioHost, globalMinioPort
	defer func() {
		globalMinioHost, globalMinioPort = prevHost, prevPort
	}()
	// tg[0] = local, tg[1] = remote

	// Remote URL
	url, err := xnet.ParseHTTPURL(tg.Servers[1].URL)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	url.Path = t.TempDir()

	globalMinioHost, globalMinioPort = mustSplitHostPort(url.Host)
	globalNodeAuthToken, _ = authenticateNode(globalActiveCred.AccessKey, globalActiveCred.SecretKey)

	endpoint, err := NewEndpoint(url.String())
	if err != nil {
		t.Fatalf("NewEndpoint failed %v", endpoint)
	}

	if err = endpoint.UpdateIsLocal(); err != nil {
		t.Fatalf("UpdateIsLocal failed %v", err)
	}

	endpoint.PoolIdx = 0
	endpoint.SetIdx = 0
	endpoint.DiskIdx = 0

	poolEps := []PoolEndpoints{{
		Endpoints: Endpoints{endpoint},
	}}
	poolEps[0].SetCount = 1
	poolEps[0].DrivesPerSet = 1

	// Register handlers on newly created servers
	registerStorageRESTHandlers(tg.Mux[0], poolEps, tg.Managers[0])
	registerStorageRESTHandlers(tg.Mux[1], poolEps, tg.Managers[1])

	storage := globalLocalSetDrives[0][0][0]
	if err = storage.MakeVol(t.Context(), "foo"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if err = storage.MakeVol(t.Context(), "bar"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	restClient, err := newStorageRESTClient(endpoint, false, tg.Managers[0])
	if err != nil {
		t.Fatal(err)
	}

	for {
		_, err := restClient.DiskInfo(t.Context(), DiskInfoOptions{})
		if err == nil || errors.Is(err, errUnformattedDisk) {
			break
		}
		time.Sleep(time.Duration(rand.Float64() * float64(100*time.Millisecond)))
	}

	return restClient
}

func TestStorageRESTClientDiskInfo(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIDiskInfo(t, restClient)
}

func TestStorageRESTClientStatInfoFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIStatInfoFile(t, restClient)
}

func TestStorageRESTClientListDir(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIListDir(t, restClient)
}

func TestStorageRESTClientReadAll(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIReadAll(t, restClient)
}

func TestStorageRESTClientReadFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIReadFile(t, restClient)
}

func TestStorageRESTClientAppendFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIAppendFile(t, restClient)
}

func TestStorageRESTClientDeleteFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIDeleteFile(t, restClient)
}

func TestStorageRESTClientRenameFile(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)

	testStorageAPIRenameFile(t, restClient)
}

func TestStorageRESTClientDeleteVersionPassesDeleteOptions(t *testing.T) {
	restClient := newStorageRESTHTTPServerClient(t)
	storage := globalLocalSetDrives[0][0][0]
	if storage == nil {
		t.Fatal("expected local storage to be initialized")
	}

	ctx := t.Context()
	volume := "foo"
	object := "delete-version-with-options"
	versionID := uuid.New().String()
	fi := FileInfo{
		Name: object, Volume: volume, VersionID: versionID, ModTime: UTCNow(), DataDir: uuid.New().String(), Size: 10000,
		Erasure: ErasureInfo{
			Algorithm:    erasureAlgorithm,
			DataBlocks:   4,
			ParityBlocks: 4,
			BlockSize:    blockSizeV2,
			Index:        1,
			Distribution: []int{0, 1, 2, 3, 4, 5, 6, 7},
		},
	}
	if err := storage.WriteMetadata(ctx, "", volume, object, fi); err != nil {
		t.Fatalf("Unable to create metadata, %s", err)
	}

	backupBuf, err := storage.ReadAll(ctx, volume, pathJoin(object, xlStorageFormatFile))
	if err != nil {
		t.Fatalf("Unable to read metadata, %s", err)
	}

	oldDataDir := uuid.New().String()
	if err := storage.WriteAll(ctx, volume, pathJoin(object, oldDataDir, xlStorageFormatFileBackup), backupBuf); err != nil {
		t.Fatalf("Unable to create backup metadata, %s", err)
	}

	if err := restClient.DeleteVersion(ctx, volume, object, FileInfo{Name: object, Volume: volume, VersionID: versionID}, false, DeleteOptions{
		UndoWrite:  true,
		OldDataDir: oldDataDir,
	}); err != nil {
		t.Fatalf("DeleteVersion failed, %s", err)
	}

	restoredBuf, err := storage.ReadAll(ctx, volume, pathJoin(object, xlStorageFormatFile))
	if err != nil {
		t.Fatalf("Unable to read restored metadata, %s", err)
	}
	if !bytes.Equal(restoredBuf, backupBuf) {
		t.Fatal("expected restored metadata to match backup after undo write")
	}
	if _, err := storage.ReadAll(ctx, volume, pathJoin(object, oldDataDir, xlStorageFormatFileBackup)); err != errFileNotFound {
		t.Fatalf("expected backup metadata to be consumed by undo write, got %v", err)
	}
}
