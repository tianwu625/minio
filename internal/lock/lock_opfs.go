//go:build !windows && !plan9 && !solaris
// +build !windows,!plan9,!solaris

package lock

//#cgo CFLAGS: -I /usr/include
//#cgo LDFLAGS: -L /usr/lib64 -lofapi
//#include <sys/time.h>
//#include <stdlib.h>
//#include "glusterfs/api/ofapi.h"
//#include <linux/stat.h>
import "C"

import (
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"syscall"
	"time"
	"unsafe"
	//"strings"
	"path/filepath"
	//"os/exec"
	"sync"

	"github.com/minio/minio/internal/logger"
)

var LerrOpenFailed = errors.New("Open file failed")
var LerrCreateFailed = errors.New("Create bucket failed")
var LerrRemoveFailed = errors.New("Remove bucket failed")
var LerrGetattrFailed = errors.New("getattr volume failed")
var LerrIOError = errors.New("IO failed")
var LerrUtimeFailed = errors.New("utime dir or file failed")
var LerrFileAccessDenied = errors.New("permission deny")
var LerrTruncateFailed = errors.New("truncate failed")
var LerrIsDir = errors.New("file is dir")

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

func (ofi opfsInfo) IsDir() bool {
	return (uint32(ofi.stat.oa_mode) & uint32(C.S_IFDIR)) != 0
}

func (ofi opfsInfo) Sys() interface{} {
	return nil
}

func (ofi opfsInfo) ModTime() time.Time {
	return time.Unix(int64(ofi.stat.oa_mtime), int64(ofi.stat.oa_mtime_nsec))
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
		return 0, os.ErrPermission
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
		return 0, os.ErrInvalid
	}
	opf.offset += int64(ret)
	return int(ret), nil
}

func (opf *opfsFile) Read(p []byte) (n int, err error) {

	//logger.Info("inlock opf %p read file %s len %d fd %p", opf, opf.name, len(p), unsafe.Pointer(opf.fd))
	if opf.flags&syscall.O_ACCMODE != os.O_RDONLY && opf.flags&syscall.O_ACCMODE != os.O_RDWR {
		logger.LogIf(nil, errors.New(fmt.Sprintf("flags=%x", opf.flags)))
		return 0, os.ErrPermission
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
		return 0, os.ErrInvalid
	}
	opf.offset += int64(ret)

	if int(ret) == 0 {
		return int(ret), io.EOF
	} else {
		return int(ret), nil
	}
}

func (opf *opfsFile) ReadAt(p []byte, off int64) (n int, err error) {

	//logger.Info("in lock opf %p read file %s len %d fd %p", opf, opf.name, len(p), unsafe.Pointer(opf.fd))
	if opf.flags&syscall.O_ACCMODE != os.O_RDONLY && opf.flags&syscall.O_ACCMODE != os.O_RDWR {
		logger.LogIf(nil, errors.New(fmt.Sprintf("flags=%x", opf.flags)))
		return 0, os.ErrPermission
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
		return 0, os.ErrInvalid
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
			return 0, LerrGetattrFailed
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
		return nil, os.ErrInvalid
	}
	return ofi, nil
}

func (opf *opfsFile) Truncate(size int64) error {
	ret := C.ofapi_truncate(opf.fd, C.uint64_t(size))
	if ret != C.int(0) {
		return os.ErrInvalid
	}
	return nil
}

func (opf *opfsFile) Name() string {
	return opf.name
}

func lockedOpfsOpenFile(path string, flag int, perm os.FileMode, lockType int) (*LockedFile, error) {
	f, err := OpfsOpen(path, flag, perm)
	if err != nil {
		return nil, err
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if st.IsDir() {
		f.Close()
		return nil, err
	}

	return &LockedFile{File: f}, nil
}

func TryLockedOpfsOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpfsOpenFile(path, flag, perm, syscall.LOCK_NB)
}

func LockedOpfsOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpfsOpenFile(path, flag, perm, 0)
}

type opfsCroot struct {
	fs       *C.ofapi_fs_t
	fspath   string
	rootpath string
}

var root opfsCroot

//FIXME just for test and demo
/*
func getShare(fspath string) (sharename string) {
        cmd := fmt.Sprintf("df |grep %s|awk '{print $1}'", fspath)
        c := exec.Command("bash", "-c", cmd)
        output, _ := c.CombinedOutput()

        names := strings.Split(string(output), ":")

        return strings.Trim(string(names[1]), "\n")
}
*/
func OpfsCinit(fsPath string) (err error) {
	root.fs = C.ofapi_init(C.CString("localhost"), C.int(1306), C.int(1), C.CString("/dev/null"))
	//FIXME only for test and demo
	//root.fspath = fsPath
	//root.rootpath = getShare(fsPath)
	return nil
}

func OpfsOpen(path string, flag int, perm os.FileMode) (*opfsFile, error) {
	var opf opfsFile
	//path = strings.ReplaceAll(path, root.fspath, root.rootpath)
	ret := C.ofapi_open(root.fs, C.CString(path), &opf.fd)
	if ret != C.int(0) {
		if ret == C.int(-2) && ((flag & os.O_CREATE) != 0) {
			dirpath, filename := filepath.Split(path)
			var pfd *C.ofapi_fd_t
			ret = C.ofapi_open(root.fs, C.CString(dirpath), &pfd)
			if ret != C.int(0) {
				logger.LogIf(nil, errors.New(fmt.Sprintf("open parent failed flags=%x, path=%s", flag, path)))
				return nil, os.ErrInvalid
			}
			defer C.ofapi_close(pfd)
			ret = C.ofapi_creatat(pfd, C.CString(filename), C.uint32_t(perm))
			if ret != C.int(0) {
				logger.LogIf(nil, errors.New(fmt.Sprintf("create failed flags=%x, path=%s", flag, path)))
				return nil, os.ErrInvalid
			}
			ret := C.ofapi_open(root.fs, C.CString(path), &opf.fd)
			if ret != C.int(0) {
				logger.LogIf(nil, errors.New(fmt.Sprintf("seconde open failed flags=%x, path=%s", flag, path)))
				return nil, os.ErrInvalid
			}
		} else {
			if ret == C.int(-2) {
				return nil, os.ErrNotExist
			}
			return nil, os.ErrInvalid
		}
	}
	opf.flags = flag
	_, opf.name = filepath.Split(path)
	if flag&os.O_TRUNC != 0 {
		ret := C.ofapi_truncate(opf.fd, C.uint64_t(0))
		if ret != C.int(0) {
			return nil, LerrTruncateFailed
		}
	}
	if flag&os.O_APPEND != 0 {
		var oatt C.struct_oatt
		ret := C.ofapi_getattr(opf.fd, &oatt)
		if ret != C.int(0) {
			return nil, LerrGetattrFailed
		}
		opf.mutex.Lock()
		defer opf.mutex.Unlock()
		opf.offset = int64(oatt.oa_size)
	}
	return &opf, nil
}
