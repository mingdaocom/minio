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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
)

// TestGetMissingSiteNames
func TestGetMissingSiteNames(t *testing.T) {
	testCases := []struct {
		currSites []madmin.PeerInfo
		oldDepIDs set.StringSet
		newDepIDs set.StringSet
		expNames  []string
	}{
		// Test1: missing some sites in replicated setup
		{
			[]madmin.PeerInfo{
				{Endpoint: "minio1:9000", Name: "minio1", DeploymentID: "dep1"},
				{Endpoint: "minio2:9000", Name: "minio2", DeploymentID: "dep2"},
				{Endpoint: "minio3:9000", Name: "minio3", DeploymentID: "dep3"},
			},
			set.CreateStringSet("dep1", "dep2", "dep3"),
			set.CreateStringSet("dep1"),
			[]string{"minio2", "minio3"},
		},
		// Test2: new site added that is not in replicated setup
		{
			[]madmin.PeerInfo{{Endpoint: "minio1:9000", Name: "minio1", DeploymentID: "dep1"}, {Endpoint: "minio2:9000", Name: "minio2", DeploymentID: "dep2"}, {Endpoint: "minio3:9000", Name: "minio3", DeploymentID: "dep3"}},
			set.CreateStringSet("dep1", "dep2", "dep3"),
			set.CreateStringSet("dep1", "dep2", "dep3", "dep4"),
			[]string{},
		},
		// Test3: not currently under site replication.
		{
			[]madmin.PeerInfo{},
			set.CreateStringSet(),
			set.CreateStringSet("dep1", "dep2", "dep3", "dep4"),
			[]string{},
		},
	}

	for i, tc := range testCases {
		names := getMissingSiteNames(tc.oldDepIDs, tc.newDepIDs, tc.currSites)
		if len(names) != len(tc.expNames) {
			t.Errorf("Test %d: Expected `%v`, got `%v`", i+1, tc.expNames, names)
		}
	}
}

func TestGetSRAddOptionsParsesQuery(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://example.com", strings.NewReader("replicateILMExpiry=true"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := parseForm(req); err != nil {
		t.Fatalf("parseForm failed: %v", err)
	}

	opts := getSRAddOptions(req)
	if !opts.ReplicateILMExpiry {
		t.Fatalf("expected ReplicateILMExpiry to be true")
	}
}

func TestGetSREditOptionsParsesQuery(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://example.com", strings.NewReader("disableILMExpiryReplication=true&enableILMExpiryReplication=true"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := parseForm(req); err != nil {
		t.Fatalf("parseForm failed: %v", err)
	}

	opts := getSREditOptions(req)
	if !opts.DisableILMExpiryReplication || !opts.EnableILMExpiryReplication {
		t.Fatalf("expected both edit options to be true, got %#v", opts)
	}
}

func TestGetSRBucketMakeOptionsParsesQuery(t *testing.T) {
	createdAt := UTCNow().Format(time.RFC3339Nano)
	req := httptest.NewRequest(http.MethodPut, "http://example.com", strings.NewReader("createdAt="+createdAt+"&lockEnabled=true&versioningEnabled=true&forceCreate=true"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := parseForm(req); err != nil {
		t.Fatalf("parseForm failed: %v", err)
	}

	opts := getSRBucketMakeOptions(req)
	if !opts.LockEnabled || !opts.VersioningEnabled || !opts.ForceCreate {
		t.Fatalf("expected boolean options to be true, got %#v", opts)
	}
	if opts.CreatedAt == timeSentinel {
		t.Fatalf("expected createdAt to be parsed")
	}
}

func TestGetSiteReplicationNetPerfDurationParsesQuery(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://example.com", strings.NewReader("duration=11s"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := parseForm(req); err != nil {
		t.Fatalf("parseForm failed: %v", err)
	}

	duration := getSiteReplicationNetPerfDuration(req)
	if duration != 11*time.Second {
		t.Fatalf("expected duration 11s, got %v", duration)
	}
}

func TestBuildSRJoinPeersReplicateILMExpiry(t *testing.T) {
	sites := []PeerSiteInfo{
		{
			PeerSite: madmin.PeerSite{
				Endpoint: "http://peer1.example.com",
				Name:     "peer1",
			},
			DeploymentID: "dep1",
		},
		{
			PeerSite: madmin.PeerSite{
				Endpoint: "http://peer2.example.com",
				Name:     "peer2",
			},
			DeploymentID: "dep2",
		},
	}

	testCases := []struct {
		name           string
		existingPeers  map[string]madmin.PeerInfo
		replicateValue bool
		wantValue      bool
	}{
		{
			name: "existing peer enables replication for all new peers",
			existingPeers: map[string]madmin.PeerInfo{
				"dep-old": {ReplicateILMExpiry: true},
			},
			replicateValue: false,
			wantValue:      true,
		},
		{
			name:           "fallback uses requested replication flag",
			existingPeers:  nil,
			replicateValue: false,
			wantValue:      false,
		},
		{
			name:           "requested replication flag is kept when no existing peer forces it",
			existingPeers:  map[string]madmin.PeerInfo{"dep-old": {ReplicateILMExpiry: false}},
			replicateValue: true,
			wantValue:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			peers := buildSRJoinPeers(tc.existingPeers, sites, tc.replicateValue)
			if len(peers) != len(sites) {
				t.Fatalf("expected %d peers, got %d", len(sites), len(peers))
			}
			for _, site := range sites {
				got, ok := peers[site.DeploymentID]
				if !ok {
					t.Fatalf("missing peer %s in join map", site.DeploymentID)
				}
				if got.Endpoint != site.Endpoint || got.Name != site.Name || got.DeploymentID != site.DeploymentID {
					t.Fatalf("peer metadata changed: got %#v want %#v", got, site.PeerSite)
				}
				if got.ReplicateILMExpiry != tc.wantValue {
					t.Fatalf("expected ReplicateILMExpiry=%v for %s, got %v", tc.wantValue, site.DeploymentID, got.ReplicateILMExpiry)
				}
			}
		})
	}
}

func TestMergeSRJoinPeersPreservesExistingReplicationFlag(t *testing.T) {
	existingPeers := map[string]madmin.PeerInfo{
		"dep1": {
			Endpoint:           "http://old-peer1.example.com",
			Name:               "old-peer1",
			DeploymentID:       "dep1",
			ReplicateILMExpiry: true,
		},
	}
	incomingPeers := map[string]madmin.PeerInfo{
		"dep1": {
			Endpoint:           "http://new-peer1.example.com",
			Name:               "new-peer1",
			DeploymentID:       "dep1",
			ReplicateILMExpiry: false,
		},
		"dep2": {
			Endpoint:           "http://peer2.example.com",
			Name:               "peer2",
			DeploymentID:       "dep2",
			ReplicateILMExpiry: false,
		},
	}

	peers := mergeSRJoinPeers(existingPeers, incomingPeers)
	if len(peers) != len(incomingPeers) {
		t.Fatalf("expected %d peers, got %d", len(incomingPeers), len(peers))
	}

	if got := peers["dep1"]; !got.ReplicateILMExpiry {
		t.Fatalf("expected dep1 to keep ReplicateILMExpiry=true, got %#v", got)
	} else if got.Endpoint != incomingPeers["dep1"].Endpoint || got.Name != incomingPeers["dep1"].Name || got.DeploymentID != incomingPeers["dep1"].DeploymentID {
		t.Fatalf("dep1 metadata changed: got %#v want %#v", got, incomingPeers["dep1"])
	}

	if got := peers["dep2"]; got.ReplicateILMExpiry {
		t.Fatalf("expected dep2 ReplicateILMExpiry=false, got %#v", got)
	}
}
