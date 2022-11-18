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
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mountinfo"
	"github.com/minio/minio/internal/opfs"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/mimedb"
)

// FSObjects - Implements fs object layer.
type OPFSObjects struct {
	GatewayUnsupported

	// Path to be exported over S3 API.
	fsPath string
	// meta json filename, varies by fs / cache backend.
	metaJSONFile string
	// Unique value to be used for all
	// temporary transactions.
	fsUUID string

	// This value shouldn't be touched, once initialized.
	fsFormatRlk *lock.RLockedFile // Is a read lock on `format.json`.

	// FS rw pool.
	rwPool *fsIOPool

	// ListObjects pool management.
	listPool *TreeWalkPool

	diskMount bool

	appendFileMap   map[string]*fsAppendFile
	appendFileMapMu sync.Mutex

	// To manage the appendRoutine go-routines
	nsMutex *nsLockMap
}

// Initializes meta volume on all the fs path.
func initMetaVolumeOPFS(fsPath, fsUUID string) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made less expensive
	// optimizing all other calls. Create minio meta volume,
	// if it doesn't exist yet.
	metaBucketPath := pathJoin(fsPath, minioMetaBucket)

	if err := opfsMkdirAll(metaBucketPath, 0o777); err != nil {
		return err
	}

	metaTmpPath := pathJoin(fsPath, minioMetaTmpBucket, fsUUID)
	if err := opfsMkdirAll(metaTmpPath, 0o777); err != nil {
		return err
	}

	if err := opfsMkdirAll(pathJoin(metaTmpPath, bgAppendsDirName), 0o777); err != nil {
		return err
	}

	if err := opfsMkdirAll(pathJoin(fsPath, dataUsageBucket), 0o777); err != nil {
		return err
	}

	metaMultipartPath := pathJoin(fsPath, minioMetaMultipartBucket)
	return opfsMkdirAll(metaMultipartPath, 0o777)
}

// NewFSObjectLayer - initialize new fs object layer.
// GlobalContext should be root privilege
func NewOPFSObjectLayer(fsPath string) (*OPFSObjects, error) {
	ctx := GlobalContext
	if fsPath == "" {
		return nil, errInvalidArgument
	}

	var err error
	if fsPath, err = getValidPath(fsPath); err != nil {
		if err == errMinDiskSize {
			return nil, config.ErrUnableToWriteInBackend(err).Hint(err.Error())
		}

		// Show a descriptive error with a hint about how to fix it.
		var username string
		if u, err := user.Current(); err == nil {
			username = u.Username
		} else {
			username = "<your-username>"
		}
		hint := fmt.Sprintf("Use 'sudo chown -R %s %s && sudo chmod u+rxw %s' to provide sufficient permissions.", username, fsPath, fsPath)
		return nil, config.ErrUnableToWriteInBackend(err).Hint(hint)
	}

	// Assign a new UUID for FS minio mode. Each server instance
	// gets its own UUID for temporary file transaction.
	fsUUID := mustGetUUID()

	// Initialize opfsc
	err = opfsCinit(fsPath)
	if err != nil {
		return nil, err
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err = initMetaVolumeOPFS(fsPath, fsUUID); err != nil {
		return nil, err
	}

	// Initialize `format.json`, this function also returns.
	rlk, err := initFormatOPFS(ctx, fsPath)
	if err != nil {
		return nil, err
	}

	// Initialize fs objects.
	fs := &OPFSObjects{
		fsPath:       fsPath,
		metaJSONFile: fsMetaJSONFile,
		fsUUID:       fsUUID,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		nsMutex:       newNSLock(false),
		listPool:      NewTreeWalkPool(globalLookupTimeout),
		appendFileMap: make(map[string]*fsAppendFile),
		diskMount:     mountinfo.IsLikelyMountPoint(fsPath),
	}

	// Once the filesystem has initialized hold the read lock for
	// the life time of the server. This is done to ensure that under
	// shared backend mode for FS, remote servers do not migrate
	// or cause changes on backend format.
	fs.fsFormatRlk = rlk

	go fs.cleanupStaleUploads(ctx)
	go intDataUpdateTracker.start(ctx, fsPath)

	// Return successfully initialized object layer.
	return fs, nil
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (ofs *OPFSObjects) NewNSLock(bucket string, objects ...string) RWLocker {
	// lockers are explicitly 'nil' for FS mode since there are only local lockers
	return ofs.nsMutex.NewNSLock(nil, bucket, objects...)
}

// SetDriveCounts no-op
func (ofs *OPFSObjects) SetDriveCounts() []int {
	return nil
}

// Shutdown - should be called when process shuts down.
func (ofs *OPFSObjects) Shutdown(ctx context.Context) error {
	ofs.fsFormatRlk.Close()
	// Cleanup and delete tmp uuid.
	return opfsRemoveAll(ctx, pathJoin(ofs.fsPath, minioMetaTmpBucket, ofs.fsUUID))
}

// BackendInfo - returns backend information
func (ofs *OPFSObjects) BackendInfo() madmin.BackendInfo {
	return madmin.BackendInfo{Type: madmin.FS}
}

// LocalStorageInfo - returns underlying storage statistics.
func (ofs *OPFSObjects) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	return ofs.StorageInfo(ctx)
}

// StorageInfo - returns underlying storage statistics.
func (ofs *OPFSObjects) StorageInfo(ctx context.Context) (StorageInfo, []error) {
	return StorageInfo{}, []error{NotImplemented{}}
}

// NSScanner returns data usage stats of the current FS deployment
func (ofs *OPFSObjects) NSScanner(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo, wantCycle uint32, _ madmin.HealScanMode) error {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return NotImplemented{}
}

// Bucket operations

// getBucketDir - will convert incoming bucket names to
// corresponding valid bucket names on the backend in a platform
// compatible way for all operating systems.
func (ofs *OPFSObjects) getBucketDir(ctx context.Context, bucket string) (string, error) {
	if bucket == "" || bucket == "." || bucket == ".." {
		return "", errVolumeNotFound
	}
	bucketDir := pathJoin(ofs.fsPath, bucket)
	return bucketDir, nil
}

func (ofs *OPFSObjects) statBucketDir(ctx context.Context, bucket string) (os.FileInfo, error) {
	bucketDir, err := ofs.getBucketDir(ctx, bucket)
	if err != nil {
		return nil, err
	}
	st, err := opfsStatVolume(ctx, bucketDir)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// MakeBucketWithLocation - create a new bucket, returns if it already exists.
func (ofs *OPFSObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return NotImplemented{}
	}

	// Verify if bucket is valid.
	if s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	defer NSUpdated(bucket, slashSeparator)

	bucketDir, err := ofs.getBucketDir(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	if err = opfsMkdirWithCred(ctx, bucketDir); err != nil {
		return toObjectErr(err, bucket)
	}
	//do acl here
	//mini windows of create and set acl
	if opts.HaveAcl() {
		if err := ofs.SetAcl(ctx, bucket, "", opts.AclGrant); err != nil {
			logger.LogIf(ctx, fmt.Errorf("set acl failed %v", err))
			return err
		}
	}

	meta := newBucketMetadata(bucket)
	rootCtx := newOpfsRoot(ctx)
	if err := meta.Save(rootCtx, ofs); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	return nil
}

//Get/Set/Delete Policy implement for minio self and process file in openfs with root
//access managememt of policy is processed by minio itself

// GetBucketPolicy - only needed for FS in NAS mode
func (ofs *OPFSObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	meta, err := loadBucketMetadata(ctx, ofs, bucket)
	if err != nil {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}
	if meta.policyConfig == nil {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}
	return meta.policyConfig, nil
}

// SetBucketPolicy - only needed for FS in NAS mode
func (ofs *OPFSObjects) SetBucketPolicy(ctx context.Context, bucket string, p *policy.Policy) error {
	meta, err := loadBucketMetadata(ctx, ofs, bucket)
	if err != nil {
		return err
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	configData, err := json.Marshal(p)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = configData

	return meta.Save(ctx, ofs)
}

// DeleteBucketPolicy - only needed for FS in NAS mode
func (ofs *OPFSObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	meta, err := loadBucketMetadata(ctx, ofs, bucket)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = nil
	return meta.Save(ctx, ofs)
}

// GetBucketInfo - fetch bucket metadata info.
func (ofs *OPFSObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	st, err := ofs.statBucketDir(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}

	createdTime := st.ModTime()

	meta, err := globalBucketMetadataSys.Get(bucket)
	if err == nil {
		createdTime = meta.Created
	}

	return BucketInfo{
		Name:    bucket,
		Created: createdTime,
	}, nil
}

// ListBuckets - list all s3 compatible buckets (directories) at fsPath.
func (ofs *OPFSObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if err := checkPathLength(ofs.fsPath); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}

	setOpfsSessionRoot()
	entries, err := opfsReadDirWithOpts(pathJoin(ofs.fsPath, SlashSeparator), readDirOpts{count: -1, followDirSymlink: true})
	if err != nil {
		logger.LogIf(ctx, errDiskNotFound)
		return nil, toObjectErr(errDiskNotFound)
	}

	bucketInfos := make([]BucketInfo, 0, len(entries))
	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry, false) {
			continue
		}
		var fi os.FileInfo
		fi, err = opfsStatVolume(ctx, pathJoin(ofs.fsPath, entry))
		// There seems like no practical reason to check for errors
		// at this point, if there are indeed errors we can simply
		// just ignore such buckets and list only those which
		// return proper Stat information instead.
		if err != nil {
			// Ignore any errors returned here.
			continue
		}
		created := fi.ModTime()
		meta, err := globalBucketMetadataSys.Get(fi.Name())
		if err == nil {
			created = meta.Created
		}

		bucketInfos = append(bucketInfos, BucketInfo{
			Name:    fi.Name(),
			Created: created,
		})
	}

	// Sort bucket infos by bucket name.
	sort.Slice(bucketInfos, func(i, j int) bool {
		return bucketInfos[i].Name < bucketInfos[j].Name
	})

	// Succes.
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket and all the metadata associated
// with the bucket including pending multipart, object metadata.
func (ofs *OPFSObjects) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	defer NSUpdated(bucket, slashSeparator)

	bucketDir, err := ofs.getBucketDir(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	if !opts.Force {
		// Attempt to delete regular bucket.
		if err = opfsRemoveDir(ctx, bucketDir); err != nil {
			return toObjectErr(err, bucket)
		}
	} else {
		tmpBucketPath := pathJoin(ofs.fsPath, minioMetaTmpBucket, bucket+"."+mustGetUUID())
		if err = opfsRename(bucketDir, tmpBucketPath); err != nil {
			return toObjectErr(err, bucket)
		}

		go func() {
			opfsRemoveAll(ctx, tmpBucketPath) // ignore returned error if any.
		}()
	}

	// Cleanup all the bucket metadata.
	ctx = newOpfsRoot(ctx)
	minioMetadataBucketDir := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket)
	if err = opfsRemoveAll(ctx, minioMetadataBucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	// Delete all bucket metadata.
	deleteBucketMetadata(ctx, ofs, bucket)

	return nil
}

// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (ofs *OPFSObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, err error) {
	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	defer NSUpdated(dstBucket, dstObject)

	if !cpSrcDstSame {
		objectDWLock := ofs.NewNSLock(dstBucket, dstObject)
		lkctx, err := objectDWLock.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return oi, err
		}
		ctx = lkctx.Context()
		defer objectDWLock.Unlock(lkctx.Cancel)
	}

	if _, err := ofs.statBucketDir(ctx, srcBucket); err != nil {
		return oi, toObjectErr(err, srcBucket)
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		setOpfsSessionRoot()
		fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, srcBucket, srcObject, ofs.metaJSONFile)
		wlk, err := ofs.rwPool.Write(fsMetaPath)
		if err != nil {
			wlk, err = ofs.rwPool.OpfsCreate(fsMetaPath)
			if err != nil {
				logger.LogIf(ctx, err)
				return oi, toObjectErr(err, srcBucket, srcObject)
			}
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()

		// Save objects' metadata in `fs.json`.
		fsMeta := newFSMetaV1()
		if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = ofs.defaultFsJSON(srcObject)
		}

		fsMeta.Meta = cloneMSS(srcInfo.UserDefined)
		fsMeta.Meta["etag"] = srcInfo.ETag
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		fsObjectPath := pathJoin(ofs.fsPath, srcBucket, srcObject)

		// Update object modtime
		err = opfsTouch(ctx, fsObjectPath)
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		// Stat the file to get object info
		fi, err := opfsStatFile(ctx, fsObjectPath)
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Return the new object info.
		return fsMeta.ToObjectInfo(srcBucket, srcObject, fi), nil
	}

	if err := checkPutObjectArgs(ctx, dstBucket, dstObject, ofs); err != nil {
		return ObjectInfo{}, err
	}

	objInfo, err := ofs.putObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined, AclGrant: dstOpts.AclGrant})
	if err != nil {
		return oi, toObjectErr(err, dstBucket, dstObject)
	}

	return objInfo, nil
}

// GetObjectNInfo - returns object info and a reader for object
// content.
func (ofs *OPFSObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	if _, err = ofs.statBucketDir(ctx, bucket); err != nil {
		return nil, toObjectErr(err, bucket)
	}

	nsUnlocker := func() {}

	if lockType != noLock {
		// Lock the object before reading.
		lock := ofs.NewNSLock(bucket, object)
		switch lockType {
		case writeLock:
			lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.Unlock(lkctx.Cancel) }
		case readLock:
			lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.RUnlock(lkctx.Cancel) }
		}
	}

	// Otherwise we get the object info
	var objInfo ObjectInfo
	if objInfo, err = ofs.getObjectInfo(ctx, bucket, object); err != nil {
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}
	// For a directory, we need to return a reader that returns no bytes.
	if HasSuffix(object, SlashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts, nsUnlocker)
	}
	// Take a rwPool lock for NFS gateway type deployment
	rwPoolUnlocker := func() {}
	if bucket != minioMetaBucket && lockType != noLock {
		setOpfsSessionRoot()
		fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
		_, err = ofs.rwPool.OpfsOpen(fsMetaPath)
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			nsUnlocker()
			return nil, toObjectErr(err, bucket, object)
		}
		// Need to clean up lock after getObject is
		// completed.
		rwPoolUnlocker = func() {
			setOpfsSessionRoot()
			ofs.rwPool.Close(fsMetaPath)
		}
	}

	objReaderFn, off, length, err := NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		rwPoolUnlocker()
		nsUnlocker()
		return nil, err
	}

	// Read the object, doesn't exist returns an s3 compatible error.
	fsObjPath := pathJoin(ofs.fsPath, bucket, object)
	readCloser, size, err := opfsOpenFile(ctx, fsObjPath, off)
	if err != nil {
		rwPoolUnlocker()
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}

	closeFn := func() {
		readCloser.Close()
	}
	reader := io.LimitReader(readCloser, length)

	// Check if range is valid
	if off > size || off+length > size {
		err = InvalidRange{off, length, size}
		logger.LogIf(ctx, err, logger.Application)
		closeFn()
		rwPoolUnlocker()
		nsUnlocker()
		return nil, err
	}

	return objReaderFn(reader, h, closeFn, rwPoolUnlocker, nsUnlocker)
}

// Create a new fs.json file, if the existing one is corrupt. Should happen very rarely.
func (ofs *OPFSObjects) createFsJSON(object, fsMetaPath string) error {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = map[string]string{
		"etag":         GenETag(),
		"content-type": mimedb.TypeByExtension(path.Ext(object)),
	}
	wlk, werr := ofs.rwPool.OpfsCreate(fsMetaPath)
	if werr == nil {
		_, err := fsMeta.WriteTo(wlk)
		wlk.Close()
		return err
	}
	return werr
}

// Used to return default etag values when a pre-existing object's meta data is queried.
func (ofs *OPFSObjects) defaultFsJSON(object string) fsMetaV1 {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = map[string]string{
		"etag":         defaultEtag,
		"content-type": mimedb.TypeByExtension(path.Ext(object)),
	}
	return fsMeta
}

func (ofs *OPFSObjects) getObjectInfoNoFSLock(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	fsMeta := fsMetaV1{}
	if HasSuffix(object, SlashSeparator) {
		fi, err := opfsStatDir(ctx, pathJoin(ofs.fsPath, bucket, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	if !globalCLIContext.StrictS3Compat {
		// Stat the file to get file size.
		fi, err := opfsStatFile(ctx, pathJoin(ofs.fsPath, bucket, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	rootCtx := newOpfsRoot(ctx)
	fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.

	rc, _, err := opfsOpenFile(rootCtx, fsMetaPath, 0)
	if err == nil {
		fsMetaBuf, rerr := ioutil.ReadAll(rc)
		rc.Close()
		if rerr == nil {
			json := jsoniter.ConfigCompatibleWithStandardLibrary
			if rerr = json.Unmarshal(fsMetaBuf, &fsMeta); rerr != nil {
				// For any error to read fsMeta, set default ETag and proceed.
				fsMeta = ofs.defaultFsJSON(object)
			}
		} else {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = ofs.defaultFsJSON(object)
		}
	}

	// Return a default etag and content-type based on the object's extension.
	if err == errFileNotFound {
		fsMeta = ofs.defaultFsJSON(object)
	}

	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		logger.LogIf(ctx, err)
		return oi, err
	}

	// Stat the file to get file size.
	// process access manage for opfsStatFile at least read privilege and attribute read privilege
	fi, err := opfsStatFile(ctx, pathJoin(ofs.fsPath, bucket, object))
	if err != nil {
		return oi, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (ofs *OPFSObjects) getObjectInfo(ctx context.Context, bucket, object string) (oi ObjectInfo, e error) {
	if strings.HasSuffix(object, SlashSeparator) && !ofs.isObjectDir(bucket, object) {
		return oi, errFileNotFound
	}

	fsMeta := fsMetaV1{}
	if HasSuffix(object, SlashSeparator) {
		fi, err := opfsStatDir(ctx, pathJoin(ofs.fsPath, bucket, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.

	setOpfsSessionRoot()
	rlk, err := ofs.rwPool.OpfsOpen(fsMetaPath)
	if err == nil {
		// Read from fs metadata only if it exists.
		_, rerr := fsMeta.ReadFrom(ctx, rlk.LockedFile)
		ofs.rwPool.Close(fsMetaPath)
		if rerr != nil {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = ofs.defaultFsJSON(object)
		}
	}

	// Return a default etag and content-type based on the object's extension.
	if err == errFileNotFound {
		fsMeta = ofs.defaultFsJSON(object)
	}

	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		logger.LogIf(ctx, err)
		return oi, err
	}

	// Stat the file to get file size.
	// process access manage for opfsStatFile at least read privilege and attribute read privilege
	fi, err := opfsStatFile(ctx, pathJoin(ofs.fsPath, bucket, object))
	if err != nil {
		return oi, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// getObjectInfoWithLock - reads object metadata and replies back ObjectInfo.
func (ofs *OPFSObjects) getObjectInfoWithLock(ctx context.Context, bucket, object string) (oi ObjectInfo, err error) {
	// Lock the object before reading.
	lk := ofs.NewNSLock(bucket, object)
	lkctx, err := lk.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	ctx = lkctx.Context()
	defer lk.RUnlock(lkctx.Cancel)

	if err := checkGetObjArgs(ctx, bucket, object); err != nil {
		return oi, err
	}

	if _, err := ofs.statBucketDir(ctx, bucket); err != nil {
		return oi, err
	}

	if strings.HasSuffix(object, SlashSeparator) && !ofs.isObjectDir(bucket, object) {
		return oi, errFileNotFound
	}

	return ofs.getObjectInfo(ctx, bucket, object)
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (ofs *OPFSObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, e error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	oi, err := ofs.getObjectInfoWithLock(ctx, bucket, object)
	if err == errCorruptedFormat || err == io.EOF {
		lk := ofs.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}
		fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
		setOpfsSessionRoot()
		err = ofs.createFsJSON(object, fsMetaPath)
		lk.Unlock(lkctx.Cancel)
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		oi, err = ofs.getObjectInfoWithLock(ctx, bucket, object)
		return oi, toObjectErr(err, bucket, object)
	}
	return oi, toObjectErr(err, bucket, object)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, writes data directly to configured filesystem path.
// Additionally writes `fs.json` which carries the necessary metadata
// for future object operations.
func (ofs *OPFSObjects) PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.Versioned {
		return objInfo, NotImplemented{}
	}

	if err := checkPutObjectArgs(ctx, bucket, object, ofs); err != nil {
		return ObjectInfo{}, err
	}

	defer NSUpdated(bucket, object)

	// Lock the object.
	lk := ofs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	return ofs.putObject(ctx, bucket, object, r, opts)
}

// putObject - wrapper for PutObject
func (ofs *OPFSObjects) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	meta := cloneMSS(opts.UserDefined)
	var err error

	// Validate if bucket name is valid and exists.
	if _, err = ofs.statBucketDir(ctx, bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	fsMeta := newFSMetaV1()
	fsMeta.Meta = meta

	// This is a special case with size as '0' and object ends
	// with a slash separator, we treat it like a valid operation
	// and return success.
	if isObjectDir(object, data.Size()) {
		// put object in a bucket When the bucket have a same name object with put object
		// the principal have the permission of bucket to put object but maybe don't have the permission delete object
		// So opfs minio first delete exist object with root
		if err = ofs.checkWritePermission(ctx, bucket, object); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		if err = opfsMkdirAllWithCred(ctx, pathJoin(ofs.fsPath, bucket, object), 0o777); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		//set acl for prefix object
		if bucket != minioMetaBucket && opts.HaveAcl() {
			if err := ofs.setObjectAcl(ctx, bucket, object, opts.AclGrant); err != nil {
				return ObjectInfo{}, toObjectErr(err, bucket, object)
			}
		}
		var fi os.FileInfo
		if fi, err = opfsStatDir(ctx, pathJoin(ofs.fsPath, bucket, object)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return ObjectInfo{}, errInvalidArgument
	}

	var wlk *lock.LockedFile
	if bucket != minioMetaBucket {
		bucketMetaDir := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix)
		fsMetaPath := pathJoin(bucketMetaDir, bucket, object, ofs.metaJSONFile)
		setOpfsSessionRoot()
		wlk, err = ofs.rwPool.OpfsWrite(fsMetaPath)
		var freshFile bool
		if err != nil {
			wlk, err = ofs.rwPool.OpfsCreate(fsMetaPath)
			if err != nil {
				logger.LogIf(ctx, err)
				return ObjectInfo{}, toObjectErr(err, bucket, object)
			}
			freshFile = true
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			// Remove meta file when PutObject encounters
			// any error and it is a fresh file.
			//
			// We should preserve the `fs.json` of any
			// existing object
			if retErr != nil && freshFile {
				tmpDir := pathJoin(ofs.fsPath, minioMetaTmpBucket, ofs.fsUUID)
				fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
			}
		}()
	}

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := mustGetUUID()

	fsTmpObjPath := pathJoin(ofs.fsPath, minioMetaTmpBucket, ofs.fsUUID, tempObj)
	bytesWritten, err := opfsCreateFile(ctx, fsTmpObjPath, data, data.Size())

	// Delete the temporary object in the case of a
	// failure. If PutObject succeeds, then there would be
	// nothing to delete.
	defer opfsRemoveFile(ctx, fsTmpObjPath)

	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	fsMeta.Meta["etag"] = r.MD5CurrentHexString()

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
	}

	// Entire object was written to the temp location, now it's safe to rename it to the actual location.
	fsNSObjPath := pathJoin(ofs.fsPath, bucket, object)
	// put object in a bucket When the bucket have a same name object with put object
	// the principal have the permission of bucket to put object but maybe don't have the permission delete object
	// So opfs minio first delete exist object with root
	if err = ofs.checkWritePermission(ctx, bucket, object); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	if err = opfsRenameFileWithCred(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket && opts.HaveAcl() {
		if err := ofs.setObjectAcl(ctx, bucket, object, opts.AclGrant); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
		setOpfsSessionRoot()
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Stat the file to fetch timestamp, size.
	fi, err := opfsStatFile(ctx, pathJoin(ofs.fsPath, bucket, object))
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Success.
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObjects - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (ofs *OPFSObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	for idx, object := range objects {
		if object.VersionID != "" {
			errs[idx] = VersionNotFound{
				Bucket:    bucket,
				Object:    object.ObjectName,
				VersionID: object.VersionID,
			}
			continue
		}
		_, errs[idx] = ofs.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil || isErrObjectNotFound(errs[idx]) {
			dobjects[idx] = DeletedObject{
				ObjectName: object.ObjectName,
			}
			errs[idx] = nil
		}
	}
	return dobjects, errs
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (ofs *OPFSObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return objInfo, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	defer NSUpdated(bucket, object)

	// Acquire a write lock before deleting the object.
	lk := ofs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return objInfo, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	if _, err = ofs.statBucketDir(ctx, bucket); err != nil {
		return objInfo, toObjectErr(err, bucket)
	}

	var rwlk *lock.LockedFile

	minioMetaBucketDir := pathJoin(ofs.fsPath, minioMetaBucket)
	fsMetaPath := pathJoin(minioMetaBucketDir, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
	if bucket != minioMetaBucket {
		setOpfsSessionRoot()
		rwlk, err = ofs.rwPool.OpfsWrite(fsMetaPath)
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			return objInfo, toObjectErr(err, bucket, object)
		}
	}

	// Delete the object.
	if err = opfsDeleteFile(ctx, pathJoin(ofs.fsPath, bucket), pathJoin(ofs.fsPath, bucket, object)); err != nil {
		if rwlk != nil {
			rwlk.Close()
		}
		return objInfo, toObjectErr(err, bucket, object)
	}

	// Close fsMetaPath before deletion
	if rwlk != nil {
		rwlk.Close()
	}

	if bucket != minioMetaBucket {
		// Delete the metadata object.
		rootCtx := newOpfsRoot(ctx)
		err = opfsDeleteFile(rootCtx, minioMetaBucketDir, fsMetaPath)
		if err != nil && err != errFileNotFound {
			return objInfo, toObjectErr(err, bucket, object)
		}
	}
	return ObjectInfo{Bucket: bucket, Name: object}, nil
}

func (ofs *OPFSObjects) isLeafDir(bucket string, leafPath string) bool {
	return ofs.isObjectDir(bucket, leafPath)
}

func (ofs *OPFSObjects) isLeaf(bucket string, leafPath string) bool {
	return !strings.HasSuffix(leafPath, slashSeparator)
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry
// is a leaf or non-leaf entry.
func (ofs *OPFSObjects) listDirFactory() ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string, delayIsLeaf bool) {
		var err error
		setOpfsSessionRoot()
		entries, err = opfsReadDir(pathJoin(ofs.fsPath, bucket, prefixDir))
		if err != nil && err != errFileNotFound {
			logger.LogIf(GlobalContext, err)
			return false, nil, false
		}
		if len(entries) == 0 {
			return true, nil, false
		}
		entries, delayIsLeaf = filterListEntries(bucket, prefixDir, entries, prefixEntry, ofs.isLeaf)
		return false, entries, delayIsLeaf
	}

	// Return list factory instance.
	return listDir
}

// isObjectDir returns true if the specified bucket & prefix exists
// and the prefix represents an empty directory. An S3 empty directory
// is also an empty directory in the FS backend.
func (ofs *OPFSObjects) isObjectDir(bucket, prefix string) bool {
	entries, err := opfsReadDirN(pathJoin(ofs.fsPath, bucket, prefix), 1)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

// ListObjectVersions not implemented for FS mode.
func (ofs *OPFSObjects) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (loi ListObjectVersionsInfo, e error) {
	return loi, NotImplemented{}
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (ofs *OPFSObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	// listObjects may in rare cases not be able to find any valid results.
	// Therefore, it cannot set a NextMarker.
	// In that case we retry the operation, but we add a
	// max limit, so we never end up in an infinite loop.
	tries := 50
	for {
		loi, err = listObjects(ctx, ofs, bucket, prefix, marker, delimiter, maxKeys, ofs.listPool,
			ofs.listDirFactory(), ofs.isLeaf, ofs.isLeafDir, ofs.getObjectInfoNoFSLock, ofs.getObjectInfoNoFSLock)
		if err != nil {
			return loi, err
		}
		if !loi.IsTruncated || loi.NextMarker != "" || tries == 0 {
			return loi, nil
		}
		tries--
	}
}

// Get/Put/DELETE ObjectTags access manage for minio
// GetObjectTags - get object tags from an existing object
func (ofs *OPFSObjects) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	oi, err := ofs.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}

// PutObjectTags - replace or add tags to an existing object
func (ofs *OPFSObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return ObjectInfo{}, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
	fsMeta := fsMetaV1{}
	wlk, err := ofs.rwPool.OpfsWrite(fsMetaPath)
	if err != nil {
		wlk, err = ofs.rwPool.OpfsCreate(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}
	// This close will allow for locks to be synchronized on `fs.json`.
	defer wlk.Close()

	// Read objects' metadata in `fs.json`.
	if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
		// For any error to read fsMeta, set default ETag and proceed.
		fsMeta = ofs.defaultFsJSON(object)
	}

	// clean fsMeta.Meta of tag key, before updating the new tags
	delete(fsMeta.Meta, xhttp.AmzObjectTagging)

	// Do not update for empty tags
	if tags != "" {
		fsMeta.Meta[xhttp.AmzObjectTagging] = tags
	}

	if _, err = fsMeta.WriteTo(wlk); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Stat the file to get file size.
	fi, err := opfsStatFile(ctx, pathJoin(ofs.fsPath, bucket, object))
	if err != nil {
		return ObjectInfo{}, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObjectTags - delete object tags from an existing object
func (ofs *OPFSObjects) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return ofs.PutObjectTags(ctx, bucket, object, "", opts)
}

// HealFormat - no-op for fs, Valid only for Erasure.
func (ofs *OPFSObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealObject - no-op for fs. Valid only for Erasure.
func (ofs *OPFSObjects) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (
	res madmin.HealResultItem, err error,
) {
	return res, NotImplemented{}
}

// HealBucket - no-op for fs, Valid only for Erasure.
func (ofs *OPFSObjects) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem,
	error,
) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func (ofs *OPFSObjects) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	return fsWalk(ctx, ofs, bucket, prefix, ofs.listDirFactory(), ofs.isLeaf, ofs.isLeafDir, results, ofs.getObjectInfoNoFSLock, ofs.getObjectInfoNoFSLock)
}

// HealObjects - no-op for fs. Valid only for Erasure.
func (ofs *OPFSObjects) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) (e error) {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetMetrics - no op
func (ofs *OPFSObjects) GetMetrics(ctx context.Context) (*BackendMetrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &BackendMetrics{}, NotImplemented{}
}

// ListObjectsV2 lists all blobs in bucket filtered by prefix
func (ofs *OPFSObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := ofs.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	listObjectsV2Info := ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}
	return listObjectsV2Info, err
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (ofs *OPFSObjects) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (ofs *OPFSObjects) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (ofs *OPFSObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (ofs *OPFSObjects) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported returns true, object tagging is supported in fs object layer.
func (ofs *OPFSObjects) IsTaggingSupported() bool {
	return true
}

// Health returns health of the object layer
func (ofs *OPFSObjects) Health(ctx context.Context, opts HealthOptions) HealthResult {
	if _, err := opfsStatPath(ofs.fsPath); err != nil {
		return HealthResult{}
	}
	return HealthResult{
		Healthy: newObjectLayerFn() != nil,
	}
}

// ReadHealth returns "read" health of the object layer
func (ofs *OPFSObjects) ReadHealth(ctx context.Context) bool {
	_, err := opfsStatPath(ofs.fsPath)
	return err == nil
}

// TransitionObject - transition object content to target tier.
func (ofs *OPFSObjects) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (ofs *OPFSObjects) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

// GetRawData returns raw file data to the callback.
// Errors are ignored, only errors from the callback are returned.
// For now only direct file paths are supported.
func (ofs *OPFSObjects) GetRawData(ctx context.Context, volume, file string, fn func(r io.Reader, host string, disk string, filename string, size int64, modtime time.Time, isDir bool) error) error {
	f, err := opfsOpenWithCred(ctx, filepath.Join(ofs.fsPath, volume, file))
	if err != nil {
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.IsDir() {
		return nil
	}
	return fn(f, "fs", ofs.fsUUID, file, st.Size(), st.ModTime(), st.IsDir())
}

func checkACLInvalid(grants []grant) error {
	for _, g := range grants {
		if g.Grantee.XMLXSI == EmailType {
			return NotImplemented{}
		}

		if g.Grantee.XMLXSI == AuthType {
			return NotImplemented{}
		}
	}
	return nil
}

func (ofs *OPFSObjects) SetAcl(ctx context.Context, bucket, object string, grants []grant) error {
	if err := checkACLInvalid(grants); err != nil {
		logger.LogIf(ctx, fmt.Errorf("check acl invalid err %v, grants %v", err, grants))
		return err
	}
	var path string
	if object == "" {
		path = filepath.Join(ofs.fsPath, bucket, "/")
	} else {
		path = filepath.Join(ofs.fsPath, bucket, object)
	}

	info, err := opfsStatPath(path)
	if err != nil {
		return err
	}

	isdir := info.Mode().IsDir()
	opfsgrants := make([]opfsAcl, 0, len(grants))
	for _, g := range grants {
		var og opfsAcl
		og.aclbits, og.aclflag = s3mapopfs(g.Permission, isdir)
		og.acltype = g.Grantee.XMLXSI
		switch og.acltype {
		case UserType:
			if g.Grantee.ID == "" {
				return errInvalidArgument
			}
			og.uid, err = globalIAMSys.GetUserIdByCanionialID(g.Grantee.ID)
			if err != nil {
				return err
			}
			if err != nil {
				return err
			}
		case GroupType:
			if g.Grantee.URI == "" {
				return errInvalidArgument
			}
			gname, err := parseURIGroup(g.Grantee.URI)
			if err != nil {
				return err
			}
			og.gid, err = globalIAMSys.GetGroupIdByName(gname)
			if err != nil {
				if !isAllUser(g.Grantee.URI) {
					return err
				}
				og.acltype = EveryType
			}
		case EmailType:
			return NotImplemented{}
		case OwnerType:
			og.uid, _, err = opfsGetUidGid(path)
		case EveryType:
		default:
			return errInvalidArgument
		}
		opfsgrants = append(opfsgrants, og)
	}
	err = opfsSetAclWithCred(ctx, path, opfsgrants)
	if err != nil {
		return err
	}

	return nil
}

func (ofs *OPFSObjects) GetAcl(ctx context.Context, bucket, object string) ([]grant, error) {
	var path string
	if object == "" {
		path = filepath.Join(ofs.fsPath, bucket, "/")
	} else {
		path = filepath.Join(ofs.fsPath, bucket, object)
	}
	info, err := opfsStatPath(path)
	if err != nil {
		return nil, err
	}
	isdir := info.Mode().IsDir()
	opfsgrants, err := opfsGetAclWithCred(ctx, path)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("ofapi get failed %v", err))
		return nil, err
	}
	grants, err := opfsAclToGrants(ctx, opfsgrants, isdir)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("convert to grant failed %v", err))
		return []grant{}, err
	}
	grants, err = checkAndAddOwnerGrant(ctx, path, grants)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("check and add owner grant %v", err))
		return grants, err
	}
	return grants, nil
}

func getOwnerGrant(path string) (grant, error) {
	uid, _, err := opfsGetUidGid(path)
	if err != nil {
		return grant{}, err
	}
	cid, err := globalIAMSys.GetCanionialIdByUid(uid)
	if err != nil {
		if uid != 0 {
			return grant{}, err
		}
		cid = globalMinioDefaultOwnerID
	}
	var g grant
	g.Grantee.XMLNS = AWSNS
	g.Grantee.XMLXSI = UserType
	g.Grantee.Type = UserType
	g.Grantee.ID = cid
	g.Permission = GrantPermFullControl
	return g, nil
}

func checkAndAddOwnerGrant(ctx context.Context, path string, grants []grant) ([]grant, error) {
	ownerGrant, err := getOwnerGrant(path)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get owner path %v failed %v", path, err))
		return grants, err
	}

	for _, g := range grants {
		if g.Grantee.XMLNS == ownerGrant.Grantee.XMLNS &&
			g.Grantee.XMLXSI == ownerGrant.Grantee.XMLXSI &&
			g.Grantee.Type == ownerGrant.Grantee.Type &&
			g.Grantee.ID == ownerGrant.Grantee.ID &&
			g.Permission == ownerGrant.Permission {
			return grants, nil
		}
	}
	grants = append(grants, ownerGrant)
	return grants, nil
}

func opfsAclToGrants(ctx context.Context, opfsgrants []opfsAcl, isdir bool) ([]grant, error) {
	grantsSum := make([]grant, 0, len(opfsgrants))
	for _, og := range opfsgrants {
		grants := make([]grant, 0, permissionMaxLen)
		permissionList := opfsmaps3List(og.aclbits, isdir)
		if len(permissionList) == 0 {
			logger.LogIf(ctx, fmt.Errorf("bits %v can't convert to s3 acl", og.aclbits))
			continue
		}
		for _, p := range permissionList {
			var g grant
			g.Permission = p
			switch og.acltype {
			case OwnerType:
				fallthrough
			case UserType:
				cid, err := globalIAMSys.GetCanionialIdByUid(og.uid)
				if err != nil {
					if og.uid != 0 {
						logger.LogIf(ctx, fmt.Errorf("get cid failed %v, %d", err, og.uid))
						return grantsSum, err
					}
					cid = globalMinioDefaultOwnerID
				}
				g.Grantee.XMLNS = AWSNS
				g.Grantee.XMLXSI = UserType
				g.Grantee.Type = UserType
				g.Grantee.ID = cid
			case GroupType:
				gname, err := globalIAMSys.GetGroupNameByGid(og.gid)
				if err != nil {
					logger.LogIf(ctx, fmt.Errorf("get gname failed %v", err))
					return grantsSum, err
				}
				g.Grantee.XMLNS = AWSNS
				g.Grantee.XMLXSI = GroupType
				g.Grantee.Type = GroupType
				g.Grantee.URI = httpSchemWithSlash + AWSCom + "/" + AWSGroup + "/" + AWSS3 + "/" + gname
			case EveryType:
				g.Grantee.XMLNS = AWSNS
				g.Grantee.XMLXSI = GroupType
				g.Grantee.Type = GroupType
				g.Grantee.URI = httpSchemWithSlash + AWSCom + "/" + AWSGroup + "/" + AWSGlobal + "/" + AWSAllUser
			default:
				logger.LogIf(ctx, fmt.Errorf("unkown type %v", og.acltype))
				return grantsSum, NotImplemented{}
			}
			grants = append(grants, g)
		}
		grantsSum = append(grantsSum, grants...)
	}
	return grantsSum, nil
}

func (ofs *OPFSObjects) getInheritAcl(ctx context.Context, bucket string) ([]grant, error) {
	path := filepath.Join(ofs.fsPath, bucket, "/")
	opfsgrants, err := opfsGetInheritAclFromDir(path)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get bucket Inherit acl from bucket %v failed %v", path, err))
		return []grant{}, err
	}
	grants, err := opfsAclToGrants(ctx, opfsgrants, true)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("convert to grant from opfsAcl %v failed %v", opfsgrants, err))
		return []grant{}, err
	}

	return grants, nil
}

func checkDirHaveWritePermission(ctx context.Context, dirPath string) bool {
	opfsgrants, err := opfsGetInheritAclFromDir(dirPath)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get inheritAcl failed %v", err))
		return false
	}
	uid, gids, err := getOpfsCred(ctx)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get uid and gids failed %v", err))
		return false
	}
	for _, g := range opfsgrants {
		if g.acltype == UserType && uid == g.uid {
			return true
		}
		if g.acltype == GroupType && inGroups(g.gid, gids) {
			return true
		}
	}
	return false
}

func checkDirHaveOwnerPermission(ctx context.Context, dirPath string) bool {
	uid, gids, err := getOpfsCred(ctx)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get uid and gids failed %v", err))
		return false
	}
	//dir owner have permission
	duid, _, err := opfsGetUidGid(dirPath)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get uid and gids from path %v failed %v", dirPath, err))
		return false
	}

	if duid == uid {
		return true
	}
	opfsgrants, err := opfsGetAcl(dirPath)
	for _, g := range opfsgrants {
		if g.aclbits&opfs.AclDirFullControl == opfs.AclDirFullControl {
			if g.acltype == UserType && g.uid == uid {
				return true
			}
			if g.acltype == GroupType && inGroups(g.gid, gids) {
				return true
			}
			if g.acltype == OwnerType && uid == g.uid {
				return true
			}
			if g.acltype == EveryType {
				return true
			}
		}
	}

	return false
}

func (ofs *OPFSObjects) checkWritePermission(ctx context.Context, bucket, object string) error {

	if bucket == minioMetaBucket {
		return nil
	}

	if uid, _, err := getOpfsCred(ctx); uid == 0 && err == nil {
		return nil
	}

	bucketPath := filepath.Join(ofs.fsPath, bucket, "/")
	if !checkDirHaveWritePermission(ctx, bucketPath) &&
		!checkDirHaveOwnerPermission(ctx, bucketPath) {
		return errFileAccessDenied
	}

	// check object's parent have permission
	parentPath := filepath.Dir(filepath.Join(ofs.fsPath, bucket, object))
	if checkDirHaveWritePermission(ctx, parentPath) ||
		checkDirHaveOwnerPermission(ctx, parentPath) {
		return nil
	}
	//Maybe change bucket acl and don't change the object acl for recursion
	//set new Acl with root
	bucketGrants, err := opfsGetInheritAclFromDir(bucketPath)
	if err != nil {
		return err
	}
	dirGrants, err := opfsGetAcl(parentPath)
	if err != nil {
		return err
	}
	opfsgrants := make([]opfsAcl, 0, len(dirGrants)+len(bucketGrants))
	for _, g := range dirGrants {
		if g.aclbits&opfs.AclDirWrite == opfs.AclDirWrite {
			continue
		}
		opfsgrants = append(opfsgrants, g)
	}
	opfsgrants = append(opfsgrants, bucketGrants...)
	if err := opfsSetAclWithRoot(parentPath, opfsgrants); err != nil {
		return err
	}
	return nil
}

func (ofs *OPFSObjects) setObjectAcl(ctx context.Context, bucket, object string, optsGrants []grant) error {
	grants, err := ofs.getInheritAcl(ctx, bucket)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get Inherit acl from bucket %v failed %v", bucket, err))
		return err
	}
	optsGrants = append(optsGrants, grants...)
	if err := ofs.SetAcl(ctx, bucket, object, optsGrants); err != nil {
		logger.LogIf(ctx, fmt.Errorf("set acl failed %v", err))
		return err
	}

	return nil
}
