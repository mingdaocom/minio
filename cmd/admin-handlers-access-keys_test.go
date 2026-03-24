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

package cmd

import (
	"testing"

	"github.com/minio/madmin-go/v3"
)

func TestParseAccessKeyBulkListType(t *testing.T) {
	testCases := []struct {
		name                string
		listType            string
		wantSTSKeys         bool
		wantServiceAccounts bool
		wantErr             bool
	}{
		{
			name:                "users only",
			listType:            madmin.AccessKeyListUsersOnly,
			wantSTSKeys:         false,
			wantServiceAccounts: false,
		},
		{
			name:                "sts only",
			listType:            madmin.AccessKeyListSTSOnly,
			wantSTSKeys:         true,
			wantServiceAccounts: false,
		},
		{
			name:                "service accounts only",
			listType:            madmin.AccessKeyListSvcaccOnly,
			wantSTSKeys:         false,
			wantServiceAccounts: true,
		},
		{
			name:                "all",
			listType:            madmin.AccessKeyListAll,
			wantSTSKeys:         true,
			wantServiceAccounts: true,
		},
		{
			name:     "empty is invalid",
			listType: "",
			wantErr:  true,
		},
		{
			name:     "random is invalid",
			listType: "does-not-exist",
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSTSKeys, gotServiceAccounts, err := parseAccessKeyBulkListType(tc.listType)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.listType)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.listType, err)
			}
			if gotSTSKeys != tc.wantSTSKeys || gotServiceAccounts != tc.wantServiceAccounts {
				t.Fatalf("unexpected parse result for %q: got (%v,%v), want (%v,%v)", tc.listType, gotSTSKeys, gotServiceAccounts, tc.wantSTSKeys, tc.wantServiceAccounts)
			}
		})
	}
}
