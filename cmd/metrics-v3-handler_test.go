// Copyright (c) 2015-2025 MinIO, Inc.
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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/minio/mux"
)

func TestNormalizeMetricsPath(t *testing.T) {
	path, buckets := normalizeMetricsPath("/bucket/api/test-bucket")
	if path != "/bucket/api" {
		t.Fatalf("expected bucket metrics path to be normalized, got %q", path)
	}
	if len(buckets) != 1 || buckets[0] != "test-bucket" {
		t.Fatalf("expected bucket name to be extracted, got %#v", buckets)
	}

	path, buckets = normalizeMetricsPath("/api")
	if path != "/api" {
		t.Fatalf("expected non-bucket path to stay unchanged, got %q", path)
	}
	if len(buckets) != 0 {
		t.Fatalf("expected no buckets for non-bucket path, got %#v", buckets)
	}
}

func TestMetricsV3ListMetricsTextAndJSON(t *testing.T) {
	h := newMetricsV3Server(NoAuthMiddleware)
	handler := h.listMetrics("/api")
	if handler == nil {
		t.Fatal("expected listMetrics handler for /api")
	}

	t.Run("text", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if got := rec.Header().Get("Content-Type"); got != "text/plain" {
			t.Fatalf("expected text/plain content type, got %q", got)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "| Name | Type | Help | Labels |") {
			t.Fatalf("expected markdown table output, got %q", body)
		}
		if !strings.Contains(body, "minio_api_requests_total") {
			t.Fatalf("expected api request metric in text output, got %q", body)
		}
	})

	t.Run("json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if got := rec.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("expected application/json content type, got %q", got)
		}

		var metrics []metricDisplay
		if err := json.Unmarshal(rec.Body.Bytes(), &metrics); err != nil {
			t.Fatalf("unable to decode json response: %v", err)
		}
		if len(metrics) == 0 {
			t.Fatal("expected at least one metric in json output")
		}
		found := false
		for _, metric := range metrics {
			if metric.Name == "minio_api_requests_total" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected api request metric in json output, got %#v", metrics)
		}
	})
}

func TestMetricsV3HandleBucketMetricsRequireBucket(t *testing.T) {
	h := newMetricsV3Server(NoAuthMiddleware)
	handler := h.handle("/bucket/api", false, nil)
	if handler == nil {
		t.Fatal("expected handler for bucket metrics path")
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for bucket metrics without bucket name, got %d", rec.Code)
	}
}

func TestMetricsV3ServeHTTPBucketListingNormalizesPath(t *testing.T) {
	h := newMetricsV3Server(NoAuthMiddleware)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/minio/metrics/v3/bucket/api/test-bucket?list=1", nil)
	if err := req.ParseForm(); err != nil {
		t.Fatalf("ParseForm failed: %v", err)
	}
	req = mux.SetURLVars(req, map[string]string{
		"pathComps": "/bucket/api/test-bucket",
	})
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for normalized bucket listing path, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "minio_bucket_api_total") {
		t.Fatalf("expected bucket metric listing, got %q", rec.Body.String())
	}
}
