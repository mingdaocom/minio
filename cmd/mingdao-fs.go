package cmd

import (
	"context"
	"fmt"
	"net/http"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/lock"
)

type Mingfs struct {
	Mode int64
	*Single
	*Distributed
	*S3Api
}

type Single struct {
	*FSObjects
}

type Distributed struct {
	*xlSets
}

func (mingfs *Mingfs) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, e error) {
	if mingfs.Mode == 1 {
		return mingfs.Distributed.GetObjectInfo(ctx, bucket, object, opts)
	} else {
		return mingfs.Single.GetObjectInfo(ctx, bucket, object, opts)
	}
}


func (mingfs *Mingfs) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if mingfs.Mode == 1 {
		return mingfs.Distributed.GetObjectNInfo(ctx, bucket, object, rs,h,lockType,opts)
	} else {
		return mingfs.Single.GetObjectNInfo(ctx, bucket, object, rs,h,lockType,opts)
	}
}

func (mingfs *Mingfs) PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	// Lock the object.
	fmt.Println("mode ", mingfs.Mode)
	if mingfs.Mode == 1 {
		return mingfs.Distributed.PutObject(ctx, bucket, object, r, opts)
	} else if mingfs.Mode == 3 {
		return mingfs.S3Api.S3PutObject(ctx, bucket, object, r, opts)
	} else {
		fs := mingfs.Single
		objectLock := fs.nsMutex.NewNSLock(ctx, bucket, object)
		if err := objectLock.GetLock(globalObjectTimeout); err != nil {
			logger.LogIf(ctx, err)
			return objInfo, err
		}
		defer objectLock.Unlock()
		defer deleteCache(bucket, object)
		return mingfs.putObject(ctx, bucket, object, r, opts)
	}
}

func (dis *Distributed) PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {

	return dis.getHashedSet(object).putObjectDis(ctx, bucket, object, r, opts)
}

//Todo 文件存储
func (mingfs *Mingfs) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	var err error
	fs := mingfs.Single
	//data := r.Reader
	fsMeta := newFSMetaV1()
	fsMeta.Meta = map[string]string{}
	// Validate if bucket name is valid and exists.
	if _, err = fs.statBucketDir(ctx, bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}
	var wlk *lock.LockedFile
	if bucket != minioMetaBucket {
		bucketMetaDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix)
		fsMetaPath := pathJoin(bucketMetaDir, bucket, object, fs.metaJSONFile)
		wlk, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		//This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			//Remove meta file when PutObject encounters any error
			if retErr != nil {
				tmpDir := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID)
				fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
			}
		}()
	}

	data := r.Reader

	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}

	buf := make([]byte, int(bufSize))
	tempObj := mustGetUUID()
	fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, tempObj)

	bytesWritten, err := fsCreateFile(ctx, fsTmpObjPath, data, buf, data.Size())

	fsMeta.Meta["etag"] = r.MD5CurrentHexString()


	defer fsRemoveFile(ctx, fsTmpObjPath)

	if bytesWritten < data.Size() {
		fsRemoveFile(ctx, fsTmpObjPath)
		return ObjectInfo{}, IncompleteBody{}
	}

	fsNSObjPath := pathJoin(fs.fsPath, bucket, object)
	if err = fsRenameFile(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
		//记录fs文件是必须的
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}
	fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}




