package cmd

//#cgo CFLAGS: -I /usr/include
//#cgo LDFLAGS: -L /usr/lib64 -lofapi
//#include <sys/time.h>
//#include <stdlib.h>
//#include "glusterfs/api/ofapi.h"
//#include <linux/stat.h>
import "C"

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	//"os/exec"
	"io"
	iofs "io/fs"
	"os"
	pathutil "path"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
)

var errOpenFailed = errors.New("Open file failed")
var errCreateFailed = errors.New("Create bucket failed")
var errRemoveFailed = errors.New("Remove bucket failed")
var errGetattrFailed = errors.New("getattr volume failed")
var errIOError = errors.New("IO failed")
var errUtimeFailed = errors.New("utime dir or file failed")
var errTruncateFailed = errors.New("truncate failed")

type opfsCroot struct {
	fs *C.ofapi_fs_t
	//fspath string
	rootpath string
}

var root opfsCroot

/*
//FIXME just for test and demo
func getShare(fspath string) (sharename string) {
        cmd := fmt.Sprintf("df |grep %s|awk '{print $1}'", fspath)
        c := exec.Command("bash", "-c", cmd)
        output, _ := c.CombinedOutput()

        names := strings.Split(string(output), ":")

        return strings.Trim(string(names[1]), "\n")
}
*/
func opfsCinit(fsPath string) (err error) {
	root.fs = C.ofapi_init(C.CString("localhost"), C.int(1306), C.int(1), C.CString("/dev/null"))
	//FIXME only for test and demo
	//root.fspath = fsPath
	//root.rootpath = getShare(fsPath)
	root.rootpath = fsPath
	return nil
}

func opfsCopen(path string) (fd *C.ofapi_fd_t, err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.ofapi_open(root.fs, cpath, &fd)
	if ret != C.int(0) {
		logger.LogIf(nil, errors.New(fmt.Sprintf("open path %s %d", path, int(ret))))
		if ret == C.int(-2) {
			return nil, errFileNotFound
		} else {
			return nil, errOpenFailed
		}
	}
	return fd, nil
}

func opfsMkdir(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	//FIXME only for test and demo
	//dirPath = strings.ReplaceAll(dirPath, root.fspath, root.rootpath)

	parentdir, bucketname := filepath.Split(strings.TrimSuffix(dirPath, SlashSeparator))
	parentfd, err := opfsCopen(parentdir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(parentfd)
	cBucketName := C.CString(bucketname)
	defer C.free(unsafe.Pointer(cBucketName))
	ret := C.ofapi_mkdirat(parentfd, cBucketName, 0777)
	if ret != C.int(0) {
		logger.LogIf(ctx, errors.New(fmt.Sprintf("create dir %s/%s ret %d",
			parentdir, bucketname, int(ret))))
		return errCreateFailed
	}

	return nil
}

func opfsRemoveFile(ctx context.Context, filePath string) (err error) {
	if filePath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(filePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	//filePath = strings.ReplaceAll(filePath, root.fspath, root.rootpath)
	parentdir, filename := filepath.Split(strings.TrimSuffix(filePath, SlashSeparator))
	parentfd, err := opfsCopen(parentdir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(parentfd)
	cFileName := C.CString(filename)
	defer C.free(unsafe.Pointer(cFileName))
	ret := C.ofapi_unlinkat(parentfd, cFileName)
	if ret != C.int(0) && ret != C.int(-2) {
		logger.LogIf(ctx, errors.New(fmt.Sprintf("remove path %s/%s ret %d",
			parentdir, filename, int(ret))))
		return errRemoveFailed
	}

	return err
}

func opfsRemoveDir(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	//FIXME only for test and demo
	//dirPath = strings.ReplaceAll(dirPath, root.fspath, root.rootpath)
	parentdir, bucketname := filepath.Split(dirPath)
	parentfd, err := opfsCopen(parentdir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(parentfd)
	cBucketName := C.CString(bucketname)
	defer C.free(unsafe.Pointer(cBucketName))
	ret := C.ofapi_rmdirat(parentfd, cBucketName)
	if ret != C.int(0) {
		logger.LogIf(ctx, errors.New(fmt.Sprintf("remove dir %s/%s ret %d",
			parentdir, bucketname, int(ret))))
		return errRemoveFailed
	}

	return nil
}

func opfsRename(srcPath, dstPath string) (err error) {
	//FIXME only for test and demo
	//srcPath = strings.ReplaceAll(srcPath, root.fspath, root.rootpath)
	//dstPath = strings.ReplaceAll(dstPath, root.fspath, root.rootpath)
	srcParent, srcBucket := filepath.Split(strings.TrimSuffix(srcPath, SlashSeparator))
	dstParent, dstBucket := filepath.Split(strings.TrimSuffix(dstPath, SlashSeparator))

	srcParentfd, err := opfsCopen(srcParent)
	if err != nil {
		return err
	}
	defer C.ofapi_close(srcParentfd)
	dstParentfd, err := opfsCopen(dstParent)
	if err != nil {
		return err
	}
	defer C.ofapi_close(dstParentfd)

	cSrcBucket := C.CString(srcBucket)
	defer C.free(unsafe.Pointer(cSrcBucket))
	cDstBucket := C.CString(dstBucket)
	defer C.free(unsafe.Pointer(cDstBucket))
	ret := C.ofapi_renameat(srcParentfd, cSrcBucket, dstParentfd, cDstBucket)
	if ret != C.int(0) {
		logger.LogIf(nil, errors.New(fmt.Sprintf("rename %s/%s to %s/%s ret %d",
			srcParent, srcBucket, dstParent, dstBucket, int(ret))))
		return errOpenFailed
	}

	return nil
}

func opfsRemoveAll(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	//remove all things
	dirPath = strings.TrimSuffix(dirPath, SlashSeparator)
	fd, err := opfsCopen(dirPath)
	if err != nil {
		return err
	}
	defer C.ofapi_close(fd)

	var oatt C.struct_oatt
	ret := C.ofapi_getattr(fd, &oatt)
	if ret != C.int(0) {
		return errGetattrFailed
	}
	parentDir, name := filepath.Split(dirPath)
	parentfd, err := opfsCopen(parentDir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(parentfd)
	cfilename := C.CString(name)
	defer C.free(unsafe.Pointer(cfilename))
	if uint32(oatt.oa_mode)&uint32(C.S_IFDIR) != 0 {
		var dent C.struct_dirent
		for {
			ret := C.ofapi_readdirp(fd, &dent, &oatt)
			if ret == C.int(0) {
				break
			}
			if C.GoString(&dent.d_name[0]) == "." ||
				C.GoString(&dent.d_name[0]) == ".." {
				continue
			}
			childPath := fmt.Sprintf("%s/%s", dirPath, C.GoString(&dent.d_name[0]))
			err = opfsRemoveAll(ctx, childPath)
			if err != nil {
				return err
			}
		}

		ret := C.ofapi_rmdirat(parentfd, cfilename)
		if ret != C.int(0) {
			logger.LogIf(nil, errors.New(fmt.Sprintf("rmdir failed %s/%s ret %d",
				parentDir, name, int(ret))))
			return errRemoveFailed
		}
	} else {
		ret := C.ofapi_unlinkat(parentfd, cfilename)
		if ret != C.int(0) {
			logger.LogIf(nil, errors.New(fmt.Sprintf("unlink failed %s/%s ret %d",
				parentDir, name, int(ret))))
			return errRemoveFailed
		}
	}

	return nil
}

func opfsReadDirWithOpts(dirPath string, opts readDirOpts) (entries []string, err error) {
	//FIXME only for test and demo
	//dirPath = strings.ReplaceAll(dirPath, root.fspath, root.rootpath)
	dirPath = strings.TrimSuffix(dirPath, SlashSeparator)
	dirfd, err := opfsCopen(dirPath)
	if err != nil {
		return nil, err
	}
	defer C.ofapi_close(dirfd)

	count := opts.count

	var dent C.struct_dirent
	var oatt C.struct_oatt
	for count != 0 {

		ret := C.ofapi_readdirp(dirfd, &dent, &oatt)
		if ret == C.int(0) {
			break
		}

		if C.GoString(&dent.d_name[0]) == "." ||
			C.GoString(&dent.d_name[0]) == ".." {
			continue
		}
		var nameStr string
		switch int(dent.d_type) {
		case 1: //file regular
			nameStr = C.GoString(&dent.d_name[0])
		case 2:
			nameStr = C.GoString(&dent.d_name[0])
			nameStr += SlashSeparator
		default:
			logger.LogIf(nil, errors.New(fmt.Sprintf("type %d name %s",
				int(dent.d_type), C.GoString(&dent.d_name[0]))))
		}
		count--
		entries = append(entries, nameStr)
	}

	return
}

type opfsInfo struct {
	name string
	stat C.struct_oatt
}

func (ofi opfsInfo) Name() string {
	return ofi.name
}

func (ofi opfsInfo) Size() int64 {
	return int64(ofi.stat.oa_size)
}

func (ofi opfsInfo) Mode() iofs.FileMode {
	var filemode iofs.FileMode
	switch {
	case (ofi.stat.oa_mode & C.S_IFDIR) != 0:
		filemode |= iofs.ModeDir
	case (ofi.stat.oa_mode & C.S_IFCHR) != 0:
		filemode |= iofs.ModeCharDevice
	case (ofi.stat.oa_mode & C.S_IFREG) != 0:
		filemode |= 0
	case (ofi.stat.oa_mode & C.S_IFIFO) != 0:
		filemode |= iofs.ModeNamedPipe
	case (ofi.stat.oa_mode & C.S_IFLNK) != 0:
		filemode |= iofs.ModeSymlink
	case (ofi.stat.oa_mode & C.S_IFSOCK) != 0:
		filemode |= iofs.ModeSocket
	}
	if (ofi.stat.oa_mode & C.S_ISUID) != 0 {
		filemode |= iofs.ModeSetuid
	}
	if (ofi.stat.oa_mode & C.S_ISGID) != 0 {
		filemode |= iofs.ModeSetgid
	}
	if (ofi.stat.oa_mode & C.S_ISVTX) != 0 {
		filemode |= iofs.ModeSticky
	}

	filemode |= iofs.FileMode(ofi.stat.oa_mode & C.ACCESSPERMS)

	return filemode
}

func (ofi opfsInfo) ModTime() time.Time {
	return time.Unix(int64(ofi.stat.oa_mtime), int64(ofi.stat.oa_mtime_nsec))
}

func (ofi opfsInfo) IsDir() bool {
	return (uint32(ofi.stat.oa_mode) & uint32(C.S_IFDIR)) != 0
}

func (ofi opfsInfo) Sys() interface{} {
	return nil
}

func opfsStat(ctx context.Context, statLoc string) (os.FileInfo, error) {
	if statLoc == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return nil, errInvalidArgument
	}

	if err := checkPathLength(statLoc); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	//FIXME only for test and demo
	//statLoc = strings.ReplaceAll(statLoc, root.fspath, root.rootpath)
	return opfsStatPath(statLoc)
}

func opfsStatVolume(ctx context.Context, volume string) (os.FileInfo, error) {
	ofi, err := opfsStat(ctx, volume)

	if err != nil {
		return nil, err
	}

	if !ofi.IsDir() {
		return nil, errVolumeAccessDenied
	}

	return ofi, nil
}

func opfsStatDir(ctx context.Context, statDir string) (os.FileInfo, error) {
	ofi, err := opfsStat(ctx, statDir)
	if err != nil {
		return nil, err
	}
	if !ofi.IsDir() {
		return nil, errFileNotFound
	}

	return ofi, nil
}

func opfsStatFile(ctx context.Context, statFile string) (os.FileInfo, error) {
	ofi, err := opfsStat(ctx, statFile)
	if err != nil {
		return nil, err
	}

	if ofi.IsDir() {
		return nil, errFileNotFound
	}

	return ofi, nil
}

/*
//FIXME only for go test
func opfsIsFile(ctx context.Context, filePath string) bool {
	ofi, err := opfsStat(ctx, filePath)
	if err != nil {
		return false
	}

	return ofi.Mode().IsRegular()
}
*/

func opfsMkdirAll(path string, mode os.FileMode) error {
	//FIXME only for test and demo
	path = strings.Trim(strings.TrimSuffix(path, SlashSeparator), root.rootpath)
	ipaths := strings.Split(path, SlashSeparator)

	var p strings.Builder
	p.WriteString(root.rootpath)
	for _, s := range ipaths {
		var nfd *C.ofapi_fd_t
		cPath := C.CString(fmt.Sprintf("%s/%s", p.String(), s))
		defer C.free(unsafe.Pointer(cPath))
		ret := C.ofapi_open(root.fs, cPath, &nfd)
		if ret != C.int(0) {
			pfd, err := opfsCopen(p.String())
			if err != nil {
				return err
			}
			defer C.ofapi_close(pfd)
			cName := C.CString(s)
			defer C.free(unsafe.Pointer(cName))
			ret = C.ofapi_mkdirat(pfd, cName, 0777)
			if ret != C.int(0) {
				logger.LogIf(nil, errors.New(fmt.Sprintf("mkdir failed %s/%s, ret %d", p.String(), s, int(ret))))
				return errCreateFailed
			}
		} else {
			defer C.ofapi_close(nfd)
		}
		p.WriteString(fmt.Sprintf("/%s", s))
	}

	return nil
}

// Renames source path to destination path, creates all the
// missing parents if they don't exist.
func opfsRenameFile(ctx context.Context, sourcePath, destPath string) error {

	if err := checkPathLength(sourcePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err := checkPathLength(destPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if sourcePath == "" || destPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err := checkPathLength(sourcePath); err != nil {
		return err
	}
	if err := checkPathLength(destPath); err != nil {
		return err
	}

	if err := opfsMkdirAll(pathutil.Dir(destPath), 0777); err != nil {
		return err
	}

	if err := opfsRename(sourcePath, destPath); err != nil {
		return err
	}

	return nil
}

type opfsFile struct {
	fd     *C.ofapi_fd_t
	offset int64
	flags  int
	name   string
	mutex  sync.Mutex
}

var _zero uintptr

func (opf *opfsFile) Write(p []byte) (n int, err error) {
	if opf.flags&syscall.O_ACCMODE != os.O_WRONLY && opf.flags&syscall.O_ACCMODE != os.O_RDWR {
		logger.LogIf(nil, errors.New(fmt.Sprintf("flags=%x", opf.flags)))
		return 0, errFileAccessDenied
	}
	var cbuffer unsafe.Pointer
	if len(p) > 0 {
		cbuffer = unsafe.Pointer(&p[0])
	} else {
		cbuffer = unsafe.Pointer(&_zero)
	}

	opf.mutex.Lock()
	defer opf.mutex.Unlock()
	ret := C.ofapi_write(opf.fd, C.uint64_t(opf.offset), cbuffer, C.uint32_t(len(p)))
	if ret < C.int(0) {
		logger.LogIf(nil, errors.New(fmt.Sprintf("write len %d offset %ld, ret=%d", len(p), opf.offset, int(ret))))
		return 0, errIOError
	}
	opf.offset += int64(ret)
	return int(ret), nil
}

func (opf *opfsFile) Read(p []byte) (n int, err error) {

	if opf.flags&syscall.O_ACCMODE != os.O_RDONLY && opf.flags&syscall.O_ACCMODE != os.O_RDWR {
		logger.LogIf(nil, errors.New(fmt.Sprintf("flags=%x", opf.flags)))
		return 0, errFileAccessDenied
	}

	var cbuffer unsafe.Pointer
	if len(p) > 0 {
		cbuffer = unsafe.Pointer(&p[0])
	} else {
		cbuffer = unsafe.Pointer(&_zero)
	}

	opf.mutex.Lock()
	defer opf.mutex.Unlock()
	ret := C.ofapi_read(opf.fd, C.uint64_t(opf.offset), cbuffer, C.uint32_t(len(p)))
	if ret < C.int(0) {
		logger.LogIf(nil, errors.New(fmt.Sprintf("read len %d offset %ld, ret=%d", len(p), opf.offset, int(ret))))
		return 0, errIOError
	}
	opf.offset += int64(ret)

	if ret == C.int(0) {
		return int(ret), io.EOF
	} else {
		return int(ret), nil
	}
}

func (opf *opfsFile) ReadAt(p []byte, off int64) (n int, err error) {

	if opf.flags&syscall.O_ACCMODE != os.O_RDONLY && opf.flags&syscall.O_ACCMODE != os.O_RDWR {
		logger.LogIf(nil, errors.New(fmt.Sprintf("flags=%x", opf.flags)))
		return 0, errFileAccessDenied
	}

	var cbuffer unsafe.Pointer
	if len(p) > 0 {
		cbuffer = unsafe.Pointer(&p[0])
	} else {
		cbuffer = unsafe.Pointer(&_zero)
	}

	ret := C.ofapi_read(opf.fd, C.uint64_t(off), cbuffer, C.uint32_t(len(p)))
	if ret < C.int(0) {
		logger.LogIf(nil, errors.New(fmt.Sprintf("read len %d offset %ld, ret=%d", len(p), opf.offset, int(ret))))
		return 0, errIOError
	}

	return int(ret), nil
}

func (opf *opfsFile) Seek(offset int64, whence int) (int64, error) {
	opf.mutex.Lock()
	defer opf.mutex.Unlock()
	switch whence {
	case io.SeekStart:
		opf.offset = offset
	case io.SeekCurrent:
		opf.offset += offset
	case io.SeekEnd:
		var oatt C.struct_oatt
		ret := C.ofapi_getattr(opf.fd, &oatt)
		if ret != C.int(0) {
			return 0, errGetattrFailed
		}
		opf.offset = int64(oatt.oa_size) + offset
	}

	off := opf.offset

	return off, nil
}

func (opf *opfsFile) Close() error {
	C.ofapi_close(opf.fd)
	return nil
}

func (opf *opfsFile) Stat() (os.FileInfo, error) {
	var ofi opfsInfo
	ofi.name = opf.name
	ret := C.ofapi_getattr(opf.fd, &ofi.stat)
	if ret != C.int(0) {
		return nil, errGetattrFailed
	}
	return ofi, nil
}

func opfsOpenPath(path string, flag int, perm os.FileMode) (*opfsFile, error) {
	var opf opfsFile
	//path = strings.ReplaceAll(path, root.fspath, root.rootpath)
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.ofapi_open(root.fs, cpath, &opf.fd)
	if ret != C.int(0) {
		if ret == C.int(-2) && ((flag & os.O_CREATE) != 0) {
			dirpath, filename := filepath.Split(strings.TrimSuffix(path, SlashSeparator))
			var pfd *C.ofapi_fd_t
			cDirPath := C.CString(dirpath)
			defer C.free(unsafe.Pointer(cDirPath))
			ret = C.ofapi_open(root.fs, cDirPath, &pfd)
			if ret != C.int(0) {
				logger.LogIf(nil, errors.New(fmt.Sprintf("open parent failed flags=%x, path=%s", flag, path)))
				return nil, errOpenFailed
			}
			defer C.ofapi_close(pfd)
			cFileName := C.CString(filename)
			defer C.free(unsafe.Pointer(cFileName))
			ret = C.ofapi_creatat(pfd, cFileName, C.uint32_t(perm))
			if ret != C.int(0) {
				logger.LogIf(nil, errors.New(fmt.Sprintf("create failed flags=%x, path=%s", flag, path)))
				return nil, errCreateFailed
			}
			ret := C.ofapi_open(root.fs, cpath, &opf.fd)
			if ret != C.int(0) {
				logger.LogIf(nil, errors.New(fmt.Sprintf("seconde open failed flags=%x, path=%s", flag, path)))
				return nil, errOpenFailed
			}
		} else {
			logger.LogIf(nil, errors.New(fmt.Sprintf("first open failed flags=%x, path=%s", flag, path)))
			return nil, errOpenFailed
		}
	}
	opf.flags = flag
	_, opf.name = filepath.Split(strings.TrimSuffix(path, SlashSeparator))
	if flag&os.O_TRUNC != 0 {
		ret := C.ofapi_truncate(opf.fd, C.uint64_t(0))
		if ret != C.int(0) {
			return nil, errTruncateFailed
		}
	}
	if flag&os.O_APPEND != 0 {
		var oatt C.struct_oatt
		ret := C.ofapi_getattr(opf.fd, &oatt)
		if ret != C.int(0) {
			return nil, errGetattrFailed
		}
		opf.mutex.Lock()
		defer opf.mutex.Unlock()
		opf.offset = int64(oatt.oa_size)
	}
	return &opf, nil
}

// Creates a file and copies data from incoming reader.
func opfsCreateFile(ctx context.Context, filePath string, reader io.Reader, fallocSize int64) (int64, error) {
	if filePath == "" || reader == nil {
		logger.LogIf(ctx, errInvalidArgument)
		return 0, errInvalidArgument
	}

	if err := checkPathLength(filePath); err != nil {
		logger.LogIf(ctx, err)
		return 0, err
	}

	if err := opfsMkdirAll(pathutil.Dir(filePath), 0o777); err != nil {
		return 0, err
	}

	flags := os.O_CREATE | os.O_WRONLY
	if globalFSOSync {
		flags |= os.O_SYNC
	}
	writer, err := lock.OpfsOpen(filePath, flags, 0o666)
	if err != nil {
		return 0, osErrToFileErr(err)
	}
	defer writer.Close()

	bytesWritten, err := xioutil.Copy(writer, reader)
	if err != nil {
		logger.LogIf(ctx, err)
		return 0, err
	}

	return bytesWritten, nil
}

func opfsOpen(filePath string) (*opfsFile, error) {
	var opf opfsFile
	//filePath = strings.ReplaceAll(filePath, root.fspath, root.rootpath)
	fd, err := opfsCopen(strings.TrimSuffix(filePath, SlashSeparator))
	if err != nil {
		return nil, err
	}
	opf.fd = fd
	opf.flags = os.O_RDONLY
	_, opf.name = filepath.Split(filePath)

	return &opf, nil
}

func opfsStatPath(filePath string) (os.FileInfo, error) {
	var ofi opfsInfo
	//filePath = strings.ReplaceAll(filePath, root.fspath, root.rootpath)
	fd, err := opfsCopen(strings.TrimSuffix(filePath, SlashSeparator))
	if err != nil {
		return nil, err
	}
	defer C.ofapi_close(fd)
	ret := C.ofapi_getattr(fd, &ofi.stat)
	if ret != C.int(0) {
		return nil, errGetattrFailed
	}
	_, ofi.name = filepath.Split(strings.TrimSuffix(filePath, SlashSeparator))

	return ofi, nil
}

func opfsReadDirN(dirPath string, count int) (entries []string, err error) {
	return opfsReadDirWithOpts(dirPath, readDirOpts{count: count})
}

func opfsReadDir(dirPath string) (entries []string, err error) {
	return opfsReadDirWithOpts(dirPath, readDirOpts{count: -1})
}

func opfsTouch(ctx context.Context, statLoc string) error {
	if statLoc == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}
	if err := checkPathLength(statLoc); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	now := time.Now()

	statLoc = strings.TrimSuffix(statLoc, SlashSeparator)
	//statLoc = strings.ReplaceAll(strings.TrimSuffix(statLoc, SlashSeparator), root.fspath, root.rootpath)
	var fd *C.ofapi_fd_t
	var uatime C.struct_timeval
	var umtime C.struct_timeval
	uatime.tv_sec = C.long(now.Unix())
	umtime.tv_sec = C.long(now.Unix())
	fd, err := opfsCopen(statLoc)
	if err != nil {
		return err
	}
	defer C.ofapi_close(fd)
	ret := C.ofapi_utime(fd, &uatime, &umtime)
	if ret != C.int(0) {
		return errUtimeFailed
	}

	return nil
}

func opfsReadFile(name string) ([]byte, error) {

	opf, err := opfsOpen(name)
	if err != nil {
		return nil, err
	}

	defer opf.Close()

	ofi, err := opf.Stat()
	if err != nil {
		return nil, err
	}

	p := make([]byte, ofi.Size())

	_, err = io.ReadFull(opf, p)

	return p, err
}
func opfsOpenFile(ctx context.Context, readPath string, offset int64) (io.ReadCloser, int64, error) {
	if readPath == "" || offset < 0 {
		logger.LogIf(ctx, errInvalidArgument)
		return nil, 0, errInvalidArgument
	}
	if err := checkPathLength(readPath); err != nil {
		logger.LogIf(ctx, err)
		return nil, 0, err
	}

	fr, err := opfsOpen(readPath)
	if err != nil {
		return nil, 0, osErrToFileErr(err)
	}

	// Stat to get the size of the file at path.
	st, err := fr.Stat()
	if err != nil {
		fr.Close()
		err = osErrToFileErr(err)
		if err != errFileNotFound {
			logger.LogIf(ctx, err)
		}
		return nil, 0, err
	}

	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		fr.Close()
		return nil, 0, errIsNotRegular
	}

	// Seek to the requested offset.
	if offset > 0 {
		_, err = fr.Seek(offset, io.SeekStart)
		if err != nil {
			fr.Close()
			logger.LogIf(ctx, err)
			return nil, 0, err
		}
	}

	// Success.
	return fr, st.Size(), nil
}

func opfsAppendFile(dst string, src string, osync bool) error {
	flags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	if osync {
		flags |= os.O_SYNC
	}
	appendFile, err := opfsOpenPath(dst, flags, 0o666)
	if err != nil {
		return err
	}
	defer appendFile.Close()

	srcFile, err := opfsOpen(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	_, err = io.Copy(appendFile, srcFile)
	return err
}

func opfsWriteFile(name string, data []byte, perm os.FileMode) error {
	f, err := opfsOpenPath(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

func opfsRemovePath(path string) (err error) {
	path = strings.TrimSuffix(path, SlashSeparator)
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var dirfd *C.ofapi_fd_t
	ret := C.ofapi_open(root.fs, cPath, &dirfd)
	if ret != C.int(0) {
		if ret == C.int(-2) {
			return nil
		}
		message := fmt.Sprintf("open dir path %s ret %d", path, int(ret))
		logger.LogIf(nil, errors.New(message))
		return osErrToFileErr(errOpenFailed)
	}
	defer C.ofapi_close(dirfd)
	var oatt C.struct_oatt
	ret = C.ofapi_getattr(dirfd, &oatt)
	if ret != C.int(0) {
		return errGetattrFailed
	}

	if uint32(oatt.oa_mode)&uint32(C.S_IFDIR) == 0 {
		parentDir, name := filepath.Split(path)
		cParentDir := C.CString(parentDir)
		defer C.free(unsafe.Pointer(cParentDir))
		var parentfd *C.ofapi_fd_t

		ret = C.ofapi_open(root.fs, cParentDir, &parentfd)
		if ret != C.int(0) {
			message := fmt.Sprintf("open dir path %s ret %d", path, int(ret))
			logger.LogIf(nil, errors.New(message))
			return osErrToFileErr(errOpenFailed)
		}
		defer C.ofapi_close(parentfd)
		cfilename := C.CString(name)
		defer C.free(unsafe.Pointer(cfilename))
		ret := C.ofapi_unlinkat(parentfd, cfilename)
		if ret != C.int(0) {
			message := fmt.Sprintf("unlink failed %s/%s ret=%d", parentDir, name, int(ret))
			logger.LogIf(nil, errors.New(message))
			return osErrToFileErr(errRemoveFailed)
		}

		return nil
	} else {
		parentDir, name := filepath.Split(path)
		cParentDir := C.CString(parentDir)
		defer C.free(unsafe.Pointer(cParentDir))
		var parentfd *C.ofapi_fd_t

		ret = C.ofapi_open(root.fs, cParentDir, &parentfd)
		if ret != C.int(0) {
			message := fmt.Sprintf("open dir path %s ret %d", path, int(ret))
			logger.LogIf(nil, errors.New(message))
			return osErrToFileErr(errOpenFailed)
		}
		defer C.ofapi_close(parentfd)
		cfilename := C.CString(name)
		defer C.free(unsafe.Pointer(cfilename))
		ret := C.ofapi_rmdirat(parentfd, cfilename)
		if ret != C.int(0) {
			message := fmt.Sprintf("unlink failed %s/%s ret=%d", parentDir, name, int(ret))
			logger.LogIf(nil, errors.New(message))
			return osErrToFileErr(errRemoveFailed)
		}

		return nil
	}
}

func opfsRemoveAllPath(dirPath string) (err error) {
	//remove all things
	dirPath = strings.TrimSuffix(dirPath, SlashSeparator)
	cdirPath := C.CString(dirPath)
	defer C.free(unsafe.Pointer(cdirPath))
	var dirfd *C.ofapi_fd_t
	ret := C.ofapi_open(root.fs, cdirPath, &dirfd)
	if ret != C.int(0) {
		message := fmt.Sprintf("open dir path %s ret %d", dirPath, int(ret))
		logger.LogIf(nil, errors.New(message))
		return osErrToFileErr(errOpenFailed)
	}
	defer C.ofapi_close(dirfd)
	var oatt C.struct_oatt
	ret = C.ofapi_getattr(dirfd, &oatt)
	if ret != C.int(0) {
		return errGetattrFailed
	}

	if uint32(oatt.oa_mode)&uint32(C.S_IFDIR) == 0 {
		parentDir, name := filepath.Split(dirPath)
		cParentDir := C.CString(parentDir)
		defer C.free(unsafe.Pointer(cParentDir))
		var parentfd *C.ofapi_fd_t

		ret = C.ofapi_open(root.fs, cParentDir, &parentfd)
		if ret != C.int(0) {
			message := fmt.Sprintf("open dir path %s ret %d", dirPath, int(ret))
			logger.LogIf(nil, errors.New(message))
			return osErrToFileErr(errOpenFailed)
		}
		defer C.ofapi_close(parentfd)
		cfilename := C.CString(name)
		defer C.free(unsafe.Pointer(cfilename))
		ret := C.ofapi_unlinkat(parentfd, cfilename)
		if ret != C.int(0) {
			message := fmt.Sprintf("unlink failed %s/%s ret=%d", parentDir, name, int(ret))
			logger.LogIf(nil, errors.New(message))
			return osErrToFileErr(errRemoveFailed)
		}

		return nil
	}

	var dent C.struct_dirent

	for {
		ret := C.ofapi_readdirp(dirfd, &dent, &oatt)
		if ret == C.int(0) {
			break
		}
		if C.GoString(&dent.d_name[0]) == "." ||
			C.GoString(&dent.d_name[0]) == ".." {
			continue
		}
		if uint32(oatt.oa_mode)&uint32(C.S_IFDIR) != 0 {
			childPath := fmt.Sprintf("%s/%s", dirPath, C.GoString(&dent.d_name[0]))
			err = opfsRemoveAllPath(childPath)
			if err != nil {
				return err
			}
		}
		parentDir, name := filepath.Split(dirPath)
		cParentDir := C.CString(parentDir)
		defer C.free(unsafe.Pointer(cParentDir))
		var parentfd *C.ofapi_fd_t
		ret = C.ofapi_open(root.fs, cParentDir, &parentfd)
		if ret != C.int(0) {
			message := fmt.Sprintf("open dir path %s ret %d", dirPath, int(ret))
			logger.LogIf(nil, errors.New(message))
			return osErrToFileErr(errOpenFailed)
		}
		defer C.ofapi_close(parentfd)
		cfilename := C.CString(name)
		defer C.free(unsafe.Pointer(cfilename))
		if uint32(oatt.oa_mode)&uint32(C.S_IFDIR) != 0 {
			ret := C.ofapi_rmdirat(parentfd, cfilename)
			if ret != C.int(0) {
				message := fmt.Sprintf("rmdir failed %s/%s ret=%d", parentDir, name, int(ret))
				logger.LogIf(nil, errors.New(message))
				return osErrToFileErr(errRemoveFailed)
			}
		} else {
			ret := C.ofapi_unlinkat(parentfd, cfilename)
			if ret != C.int(0) {
				message := fmt.Sprintf("unlink failed %s/%s ret=%d", parentDir, name, int(ret))
				logger.LogIf(nil, errors.New(message))
				return osErrToFileErr(errRemoveFailed)
			}
		}
	}

	return nil
}
func opfsdeleteFile(basePath, deletePath string, recursive bool) error {
	if basePath == "" || deletePath == "" {
		return nil
	}
	basePath = pathutil.Clean(basePath)
	deletePath = pathutil.Clean(deletePath)
	if !strings.HasPrefix(deletePath, basePath) || deletePath == basePath {
		return nil
	}

	var err error
	if recursive {
		err = opfsRemoveAllPath(deletePath)
	} else {
		err = opfsRemovePath(deletePath)
	}
	if err != nil {
		return err
	}

	deletePath = pathutil.Dir(deletePath)

	// Delete parent directory obviously not recursively. Errors for
	// parent directories shouldn't trickle down.
	opfsdeleteFile(basePath, deletePath, false)

	return nil
}

func opfsDeleteFile(ctx context.Context, basePath, deletePath string) error {
	if err := checkPathLength(basePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err := checkPathLength(deletePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err := opfsdeleteFile(basePath, deletePath, false); err != nil {
		if err != errFileNotFound {
			logger.LogIf(ctx, err)
		}
		return err
	}
	return nil
}

func opfsRemoveMeta(ctx context.Context, basePath, deletePath, tmpDir string) error {
	// Special case for windows please read through.
	if runtime.GOOS == globalWindowsOSName {
		// Ordinarily windows does not permit deletion or renaming of files still
		// in use, but if all open handles to that file were opened with FILE_SHARE_DELETE
		// then it can permit renames and deletions of open files.
		//
		// There are however some gotchas with this, and it is worth listing them here.
		// Firstly, Windows never allows you to really delete an open file, rather it is
		// flagged as delete pending and its entry in its directory remains visible
		// (though no new file handles may be opened to it) and when the very last
		// open handle to the file in the system is closed, only then is it truly
		// deleted. Well, actually only sort of truly deleted, because Windows only
		// appears to remove the file entry from the directory, but in fact that
		// entry is merely hidden and actually still exists and attempting to create
		// a file with the same name will return an access denied error. How long it
		// silently exists for depends on a range of factors, but put it this way:
		// if your code loops creating and deleting the same file name as you might
		// when operating a lock file, you're going to see lots of random spurious
		// access denied errors and truly dismal lock file performance compared to POSIX.
		//
		// We work-around these un-POSIX file semantics by taking a dual step to
		// deleting files. Firstly, it renames the file to tmp location into multipartTmpBucket
		// We always open files with FILE_SHARE_DELETE permission enabled, with that
		// flag Windows permits renaming and deletion, and because the name was changed
		// to a very random name somewhere not in its origin directory before deletion,
		// you don't see those unexpected random errors when creating files with the
		// same name as a recently deleted file as you do anywhere else on Windows.
		// Because the file is probably not in its original containing directory any more,
		// deletions of that directory will not fail with "directory not empty" as they
		// otherwise normally would either.

		tmpPath := pathJoin(tmpDir, mustGetUUID())

		opfsRenameFile(ctx, deletePath, tmpPath)

		// Proceed to deleting the directory if empty
		opfsDeleteFile(ctx, basePath, pathutil.Dir(deletePath))

		// Finally delete the renamed file.
		return opfsDeleteFile(ctx, tmpDir, tmpPath)
	}
	return opfsDeleteFile(ctx, basePath, deletePath)
}
