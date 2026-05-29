#!/usr/bin/env bash
set -euo pipefail

# Build MinIO for linux/amd64 -> ./minio
# Build MinIO for linux/arm64 -> ./minio_arm
# Run this script in the MinIO source root (where go.mod is).

cd "$(dirname "$0")"

go mod download

BUILD_VERSION="${MINIO_BUILD_VERSION:-$(date -u '+%Y-%m-%dT%H:%M:%SZ')}"
MINIO_RELEASE="${MINIO_RELEASE:-RELEASE}"
LDFLAGS="$(MINIO_RELEASE="${MINIO_RELEASE}" go run buildscripts/gen-ldflags.go "${BUILD_VERSION}")"

echo "==> Build version: ${BUILD_VERSION}"
echo "==> Release prefix: ${MINIO_RELEASE}"

echo "==> Building linux/amd64 -> ./minio"
env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="${LDFLAGS}" -o minio .

echo "==> Building linux/arm64 -> ./minio_arm"
env CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="${LDFLAGS}" -o minio_arm .

echo "==> Done"
ls -lh ./minio ./minio_arm
file ./minio ./minio_arm || true
