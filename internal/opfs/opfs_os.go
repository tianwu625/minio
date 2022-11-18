package opfs

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
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/minio/minio/internal/logger"
)

type OpfsInfo struct {
	name string
	stat C.struct_oatt
}

func (ofi OpfsInfo) Name() string {
	return ofi.name
}

func (ofi OpfsInfo) Size() int64 {
	return int64(ofi.stat.oa_size)
}

func (ofi OpfsInfo) Mode() iofs.FileMode {
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

func (ofi OpfsInfo) IsDir() bool {
	return (uint32(ofi.stat.oa_mode) & uint32(C.S_IFDIR)) != 0
}

func (ofi OpfsInfo) Sys() interface{} {
	return nil
}

func (ofi OpfsInfo) ModTime() time.Time {
	return time.Unix(int64(ofi.stat.oa_mtime), int64(ofi.stat.oa_mtime_nsec))
}

type OpfsFile struct {
	fd     *C.ofapi_fd_t
	offset int64
	flags  int
	name   string
	mutex  sync.Mutex
}

var _zero uintptr

func (opf *OpfsFile) Write(p []byte) (n int, err error) {
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

func (opf *OpfsFile) Read(p []byte) (n int, err error) {

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

func (opf *OpfsFile) ReadAt(p []byte, off int64) (n int, err error) {

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

func (opf *OpfsFile) Seek(offset int64, whence int) (int64, error) {
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
			return 0, syscall.Errno(int(-ret))
		}
		opf.offset = int64(oatt.oa_size) + offset
	}
	off := opf.offset

	return off, nil
}

func (opf *OpfsFile) Close() error {
	C.ofapi_close(opf.fd)
	return nil
}

func (opf *OpfsFile) Stat() (os.FileInfo, error) {
	var ofi OpfsInfo
	ofi.name = opf.name
	ret := C.ofapi_getattr(opf.fd, &ofi.stat)
	if ret != cok {
		return nil, os.ErrInvalid
	}
	return ofi, nil
}

func (opf *OpfsFile) Truncate(size int64) error {
	ret := C.ofapi_truncate(opf.fd, C.uint64_t(size))
	if ret != C.int(0) {
		return os.ErrInvalid
	}
	return nil
}

func (opf *OpfsFile) Name() string {
	return opf.name
}

func (opf *OpfsFile) Readdir(n int) (entries []os.FileInfo, err error) {
	count := 0
	err = nil
	for {
		var dent C.struct_dirent
		var oatt C.struct_oatt
		ret := C.ofapi_readdirp(opf.fd, &dent, &oatt)
		if ret == cok {
			err = io.EOF
		}
		if ret < cok {
			err = opfsErr(ret)
			break
		}
		dname := C.GoString(&dent.d_name[0])
		if dname == "." || dname == ".." {
			continue
		}
		entries = append(entries, OpfsInfo{
			name: dname,
			stat: oatt,
		})
		count++
		if n > 0 && count == n {
			break
		}
		if err == io.EOF {
			break
		}
	}

	return
}

func (opf *OpfsFile) Readdirnames(n int) (names []string, err error) {
	count := 0
	err = nil
	for {
		var dent C.struct_dirent
		var oatt C.struct_oatt
		ret := C.ofapi_readdirp(opf.fd, &dent, &oatt)
		if ret == cok {
			err = io.EOF
			break
		}
		if ret < cok {
			err = opfsErr(ret)
			break
		}
		dname := C.GoString(&dent.d_name[0])
		if dname == "." || dname == ".." {
			continue
		}
		switch int(dent.d_type) {
		case 2:
			dname += SlashSeparator
		}
		names = append(names, dname)
		count++
		if n > 0 && count == n {
			break
		}
	}

	return
}
