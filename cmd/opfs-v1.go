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
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mountinfo"
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
func NewOPFSObjectLayer(fsPath string) (ObjectLayer, error) {
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
	di, err := getDiskInfo(ofs.fsPath)
	if err != nil {
		return StorageInfo{}, []error{err}
	}
	storageInfo := StorageInfo{
		Disks: []madmin.Disk{
			{
				State:          madmin.DriveStateOk,
				TotalSpace:     di.Total,
				UsedSpace:      di.Used,
				AvailableSpace: di.Free,
				DrivePath:      ofs.fsPath,
			},
		},
	}
	storageInfo.Backend.Type = madmin.FS
	return storageInfo, nil
}

// NSScanner returns data usage stats of the current FS deployment
func (ofs *OPFSObjects) NSScanner(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo, wantCycle uint32, _ madmin.HealScanMode) error {
	defer close(updates)
	// Load bucket totals
	var totalCache dataUsageCache
	err := totalCache.load(ctx, ofs, dataUsageCacheName)
	if err != nil {
		return err
	}
	totalCache.Info.Name = dataUsageRoot
	buckets, err := ofs.ListBuckets(ctx)
	if err != nil {
		return err
	}
	if len(buckets) == 0 {
		totalCache.keepBuckets(buckets)
		updates <- totalCache.dui(dataUsageRoot, buckets)
		return nil
	}
	for i, b := range buckets {
		if isReservedOrInvalidBucket(b.Name, false) {
			// Delete bucket...
			buckets = append(buckets[:i], buckets[i+1:]...)
		}
	}

	totalCache.Info.BloomFilter = bf.bytes()

	// Clear totals.
	var root dataUsageEntry
	if r := totalCache.root(); r != nil {
		root.Children = r.Children
	}
	totalCache.replace(dataUsageRoot, "", root)

	// Delete all buckets that does not exist anymore.
	totalCache.keepBuckets(buckets)

	for _, b := range buckets {
		// Load bucket cache.
		var bCache dataUsageCache
		err := bCache.load(ctx, ofs, path.Join(b.Name, dataUsageCacheName))
		if err != nil {
			return err
		}
		if bCache.Info.Name == "" {
			bCache.Info.Name = b.Name
		}
		bCache.Info.BloomFilter = totalCache.Info.BloomFilter
		bCache.Info.NextCycle = wantCycle
		upds := make(chan dataUsageEntry, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for update := range upds {
				totalCache.replace(b.Name, dataUsageRoot, update)
				if intDataUpdateTracker.debug {
					logger.Info(color.Green("NSScanner:")+" Got update: %v", len(totalCache.Cache))
				}
				cloned := totalCache.clone()
				updates <- cloned.dui(dataUsageRoot, buckets)
			}
		}()
		bCache.Info.updates = upds
		cache, err := ofs.scanBucket(ctx, b.Name, bCache)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		logger.LogIf(ctx, err)
		cache.Info.BloomFilter = nil
		wg.Wait()

		if cache.root() == nil {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("NSScanner:") + " No root added. Adding empty")
			}
			cache.replace(cache.Info.Name, dataUsageRoot, dataUsageEntry{})
		}
		if cache.Info.LastUpdate.After(bCache.Info.LastUpdate) {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("NSScanner:")+" Saving bucket %q cache with %d entries", b.Name, len(cache.Cache))
			}
			logger.LogIf(ctx, cache.save(ctx, ofs, path.Join(b.Name, dataUsageCacheName)))
		}
		// Merge, save and send update.
		// We do it even if unchanged.
		cl := cache.clone()
		entry := cl.flatten(*cl.root())
		totalCache.replace(cl.Info.Name, dataUsageRoot, entry)
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("NSScanner:")+" Saving totals cache with %d entries", len(totalCache.Cache))
		}
		totalCache.Info.LastUpdate = time.Now()
		logger.LogIf(ctx, totalCache.save(ctx, ofs, dataUsageCacheName))
		cloned := totalCache.clone()
		updates <- cloned.dui(dataUsageRoot, buckets)

	}

	return nil
}

// scanBucket scans a single bucket in FS mode.
// The updated cache for the bucket is returned.
// A partially updated bucket may be returned.
func (ofs *OPFSObjects) scanBucket(ctx context.Context, bucket string, cache dataUsageCache) (dataUsageCache, error) {
	defer close(cache.Info.updates)

	// Get bucket policy
	// Check if the current bucket has a configured lifecycle policy
	lc, err := globalLifecycleSys.Get(bucket)
	if err == nil && lc.HasActiveRules("", true) {
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("scanBucket:") + " lifecycle: Active rules found")
		}
		cache.Info.lifeCycle = lc
	}

	// Load bucket info.
	cache, err = scanDataFolder(ctx, -1, -1, ofs.fsPath, cache, func(item scannerItem) (sizeSummary, error) {
		bucket, object := item.bucket, item.objectPath()
		fsMetaBytes, err := opfsReadFile(pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile))
		if err != nil && !osIsNotExist(err) {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("scanBucket:")+" object return unexpected error: %v/%v: %w", item.bucket, item.objectPath(), err)
			}
			return sizeSummary{}, errSkipFile
		}

		fsMeta := newFSMetaV1()
		metaOk := false
		if len(fsMetaBytes) > 0 {
			json := jsoniter.ConfigCompatibleWithStandardLibrary
			if err = json.Unmarshal(fsMetaBytes, &fsMeta); err == nil {
				metaOk = true
			}
		}
		if !metaOk {
			fsMeta = ofs.defaultFsJSON(object)
		}

		// Stat the file.
		fi, fiErr := opfsStatPath(item.Path)
		if fiErr != nil {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("scanBucket:")+" object path missing: %v: %w", item.Path, fiErr)
			}
			return sizeSummary{}, errSkipFile
		}

		oi := fsMeta.ToObjectInfo(bucket, object, fi)
		atomic.AddUint64(&globalScannerStats.accTotalVersions, 1)
		atomic.AddUint64(&globalScannerStats.accTotalObjects, 1)
		sz := item.applyActions(ctx, ofs, oi, &sizeSummary{})
		if sz >= 0 {
			return sizeSummary{totalSize: sz, versions: 1}, nil
		}

		return sizeSummary{totalSize: fi.Size(), versions: 1}, nil
	}, 0)

	return cache, err
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

	if err = opfsMkdir(ctx, bucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	meta := newBucketMetadata(bucket)
	if err := meta.Save(ctx, ofs); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	return nil
}

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

	entries, err := opfsReadDirWithOpts(ofs.fsPath, readDirOpts{count: -1, followDirSymlink: true})
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

	objInfo, err := ofs.putObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined})
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
		fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
		_, err = ofs.rwPool.OpfsOpen(fsMetaPath)
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			nsUnlocker()
			return nil, toObjectErr(err, bucket, object)
		}
		// Need to clean up lock after getObject is
		// completed.
		rwPoolUnlocker = func() { ofs.rwPool.Close(fsMetaPath) }
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

	fsMetaPath := pathJoin(ofs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, ofs.metaJSONFile)
	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.

	rc, _, err := opfsOpenFile(ctx, fsMetaPath, 0)
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
		if err = opfsMkdirAll(pathJoin(ofs.fsPath, bucket, object), 0o777); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
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
	if err = opfsRenameFile(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
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
		err = opfsDeleteFile(ctx, minioMetaBucketDir, fsMetaPath)
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
	f, err := opfsOpen(filepath.Join(ofs.fsPath, volume, file))
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
