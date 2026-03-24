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
	"errors"

	"github.com/minio/madmin-go/v3"
)

// parseAccessKeyBulkListType normalizes the listType accepted by the bulk
// access-key listing handlers.
func parseAccessKeyBulkListType(listType string) (listSTSKeys, listServiceAccounts bool, err error) {
	switch listType {
	case madmin.AccessKeyListUsersOnly:
		return false, false, nil
	case madmin.AccessKeyListSTSOnly:
		return true, false, nil
	case madmin.AccessKeyListSvcaccOnly:
		return false, true, nil
	case madmin.AccessKeyListAll:
		return true, true, nil
	default:
		return false, false, errors.New("invalid list type")
	}
}
