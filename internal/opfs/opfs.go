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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/minio/minio/internal/logger"
)

type opfsCroot struct {
	mutex    sync.Mutex
	init     bool
	fs       *C.ofapi_fs_t
	rootpath string
}

var root opfsCroot

func opfsErr(ret C.int) error {
	if ret < C.int(0) {
		ret = -ret
	}

	return syscall.Errno(int(ret))
}

func Init(fsPath string) (err error) {
	root.mutex.Lock()
	defer root.mutex.Unlock()
	if root.init {
		return nil
	}
	root.init = true
	root.fs = C.ofapi_init(C.CString("localhost"), C.int(1306), C.int(1), C.CString("/dev/null"))
	root.rootpath = fsPath
	return nil
}

const cok = C.int(0)

func open(path string) (fd *C.ofapi_fd_t, err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.ofapi_open(root.fs, cpath, &fd)
	if ret != cok {
		return nil, opfsErr(ret)
	}
	return fd, nil
}

func createFile(path string, perm os.FileMode) error {
	dirpath, filename := filepath.Split(path)
	pfd, err := open(dirpath)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("createFile open path %s fail err %v", dirpath, err))
		return err
	}
	defer C.ofapi_close(pfd)
	cfilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cfilename))
	ret := C.ofapi_creatat(pfd, cfilename, C.uint32_t(perm))
	if ret != cok {
		logger.LogIf(nil, fmt.Errorf("createFile dirpath %s filename %s fail err %v", dirpath, filename, err))
		return opfsErr(ret)
	}
	return nil
}

func OpenWithCreate(path string, flag int, perm os.FileMode) (*OpfsFile, error) {
	var opf OpfsFile
	var fd *C.ofapi_fd_t
	fd, err := open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && (flag&os.O_CREATE != 0) {
			err := createFile(path, perm)
			if err != nil {
				return nil, err
			}
			fd, err = open(path)
			if err != nil {
				logger.LogIf(nil, fmt.Errorf("seconde open failed flags=%x, perm=%v, path=%s", flag, perm, path))
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	opf.fd = fd
	opf.flags = flag
	_, opf.name = filepath.Split(path)
	if flag&os.O_TRUNC != 0 {
		ret := C.ofapi_truncate(opf.fd, C.uint64_t(0))
		if ret != cok {
			return nil, opfsErr(ret)
		}
	}
	if flag&os.O_APPEND != 0 {
		var oatt C.struct_oatt
		ret := C.ofapi_getattr(opf.fd, &oatt)
		if ret != cok {
			return nil, opfsErr(ret)
		}
		opf.offset = int64(oatt.oa_size)
	}
	return &opf, nil
}

func toCUint32Array(s []int) (*C.uint32_t, C.uint32_t) {
	sHdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	cs := (*C.uint32_t)(unsafe.Pointer(uintptr(sHdr.Data)))
	cl := (C.uint32_t)(sHdr.Len)
	return cs, cl
}

func SetCred(uid int, gids []int) error {
	C.ofapi_setcred(C.uint32_t(uid), C.uint32_t(gids[0]))
	return nil
}

const (
	SlashSeparator = "/"
	defaultPerm    = 0777
)

func MakeDir(path string) error {
	dir, name := filepath.Split(strings.TrimSuffix(path, SlashSeparator))
	pfd, err := open(dir)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("MakeDir open dir %v fail err %v", dir, err))
		return err
	}
	defer C.ofapi_close(pfd)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	ret := C.ofapi_mkdirat(pfd, cname, defaultPerm)
	if ret != cok {
		if !errors.Is(opfsErr(ret), os.ErrExist) {
			logger.LogIf(nil, fmt.Errorf("MakeDir dir %v name %v fail err %v", dir, name, opfsErr(ret)))
		}
		return opfsErr(ret)
	}
	return nil
}

func RemoveFile(path string) error {
	dir, name := filepath.Split(strings.TrimSuffix(path, SlashSeparator))
	pfd, err := open(dir)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("RemoveFile open dir %v fail err %v", dir, err))
		return err
	}
	defer C.ofapi_close(pfd)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	ret := C.ofapi_unlinkat(pfd, cname)
	if ret != cok && !errors.Is(opfsErr(ret), os.ErrNotExist) {
		logger.LogIf(nil, fmt.Errorf("RemoveFile dir %v name %v fail err %v", dir, name, opfsErr(ret)))
		return opfsErr(ret)
	}
	return nil
}

func RemoveDir(path string) error {
	dir, name := filepath.Split(strings.TrimSuffix(path, SlashSeparator))
	pfd, err := open(dir)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("RemoveDir open dir %v fail err %v", dir, err))
		return err
	}
	defer C.ofapi_close(pfd)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	ret := C.ofapi_rmdirat(pfd, cname)
	if ret != cok {
		if !errors.Is(opfsErr(ret), syscall.ENOTEMPTY) {
			logger.LogIf(nil, fmt.Errorf("RemoveDir dir %v name %v fail err %v", dir, name, opfsErr(ret)))
		}
		return opfsErr(ret)
	}
	return nil
}

func Rename(src, dst string) error {
	sdir, sname := filepath.Split(strings.TrimSuffix(src, SlashSeparator))
	ddir, dname := filepath.Split(strings.TrimSuffix(dst, SlashSeparator))

	psfd, err := open(sdir)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("Rename open src dir %v fail err %v", sdir, err))
		return err
	}
	defer C.ofapi_close(psfd)
	pdfd, err := open(ddir)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("Rename open dst dir %v fail err %v", ddir, err))
		return err
	}
	defer C.ofapi_close(pdfd)

	csname := C.CString(sname)
	defer C.free(unsafe.Pointer(csname))
	cdname := C.CString(dname)
	defer C.free(unsafe.Pointer(cdname))
	ret := C.ofapi_renameat(psfd, csname, pdfd, cdname)
	if ret != cok {
		logger.LogIf(nil, fmt.Errorf("Rename sdir %v sname %v ddir %v dname %v fail err %v",
			sdir, sname, ddir, dname, opfsErr(ret)))
		return opfsErr(ret)
	}

	return nil
}

func ReadDir(path string) (name string, err error) {
	path = strings.TrimSuffix(path, SlashSeparator)
	fd, err := open(path)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("ReadDir open dir %v fail err %v", path, err))
		return "", err
	}
	defer C.ofapi_close(fd)

	var dent C.struct_dirent
	var oatt C.struct_oatt
	for {
		ret := C.ofapi_readdirp(fd, &dent, &oatt)
		if ret == cok {
			return "", nil
		}

		dname := C.GoString(&dent.d_name[0])

		if dname == "." || dname == ".." {
			continue
		}

		switch int(dent.d_type) {
		case 1: //file regular
		case 2:
			dname += SlashSeparator
			return dname, nil
		default:
			logger.LogIf(nil, fmt.Errorf("ReadDir type %v name %v", int(dent.d_type), dname))
			return dname, opfsErr(C.int(95))
		}
	}

	return "", nil
}

func Stat(path string) (os.FileInfo, error) {
	var ofi OpfsInfo

	fd, err := open(strings.TrimSuffix(path, SlashSeparator))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.LogIf(nil, fmt.Errorf("Stat open %v fail err %v", path, err))
		}
		return nil, err
	}
	defer C.ofapi_close(fd)

	ret := C.ofapi_getattr(fd, &ofi.stat)
	if ret != cok {
		return nil, opfsErr(ret)
	}

	ofi.name = filepath.Base(strings.TrimSuffix(path, SlashSeparator))

	return ofi, nil
}

func Open(path string) (*OpfsFile, error) {
	var opf OpfsFile

	fd, err := open(strings.TrimSuffix(path, SlashSeparator))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.LogIf(nil, fmt.Errorf("Open open %v fail err %v", path, err))
		}
		return nil, err
	}

	opf.fd = fd
	opf.flags = os.O_RDONLY
	opf.name = filepath.Base(path)

	return &opf, nil
}

func Utime(path string, t time.Time) error {
	var uatime C.struct_timeval
	var umtime C.struct_timeval

	uatime.tv_sec = C.long(t.Unix())
	umtime.tv_sec = C.long(t.Unix())

	fd, err := open(strings.TrimSuffix(path, SlashSeparator))
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("Utime open %v fail err %v", path, err))
		return err
	}
	defer C.ofapi_close(fd)
	ret := C.ofapi_utime(fd, &uatime, &umtime)
	if ret != cok {
		logger.LogIf(nil, fmt.Errorf("Utime %v fail err %v", path, opfsErr(ret)))
		return opfsErr(ret)
	}

	return nil
}

const (
	AclDirRead = int(C.OFAPI_ACE_MASK_DIR_LIST | C.OFAPI_ACE_MASK_XATTR_READ |
		C.OFAPI_ACE_MASK_ATTR_READ)
	AclFileRead = int(C.OFAPI_ACE_MASK_FILE_DATA_READ | C.OFAPI_ACE_MASK_XATTR_READ |
		C.OFAPI_ACE_MASK_ATTR_READ)
	AclDirWrite = int(C.OFAPI_ACE_MASK_DIR_ADD_FILE | C.OFAPI_ACE_MASK_DIR_ADD_DIR |
		C.OFAPI_ACE_MASK_XATTR_WRITE | C.OFAPI_ACE_MASK_ATTR_WRITE |
		C.OFAPI_ACE_MASK_DELETE_CHILD | C.OFAPI_ACE_MASK_DELETE)
	AclFileWrite = int(C.OFAPI_ACE_MASK_FILE_DATA_WRITE | C.OFAPI_ACE_MASK_XATTR_WRITE |
		C.OFAPI_ACE_MASK_ATTR_WRITE | C.OFAPI_ACE_MASK_FILE_DATA_APPEND |
		C.OFAPI_ACE_MASK_DELETE)
	AclRead           = int(C.OFAPI_ACE_MASK_ACL_READ)
	AclWrite          = int(C.OFAPI_ACE_MASK_ACL_WRITE)
	AclDirFullControl = AclDirRead | AclDirWrite | AclWrite | AclRead |
		int(C.OFAPI_ACE_MASK_CHANGE_OWNER|C.OFAPI_ACE_MASK_SYNC|
			C.OFAPI_ACE_MASK_EXECUTE)
	AclFileFullControl = AclFileRead | AclFileWrite | AclWrite | AclRead |
		int(C.OFAPI_ACE_MASK_CHANGE_OWNER|C.OFAPI_ACE_MASK_SYNC|
			C.OFAPI_ACE_MASK_EXECUTE)
	AclFlagDefault = int(C.OFAPI_ACE_FLAG_NO_PROPAGET)
	AclFlagInherit = int(C.OFAPI_ACE_FLAG_FILE_INHERIT |
		C.OFAPI_ACE_FLAG_DIR_INHERIT)
)

//for acl operate
type AclGrant struct {
	uid     int
	gid     int
	acltype string
	aclbits int
	aclflag int
}

func toCPoint(as []C.struct_oace) *C.struct_oace {
	asHdr := (*reflect.SliceHeader)(unsafe.Pointer(&as))
	return (*C.struct_oace)(unsafe.Pointer(uintptr(asHdr.Data)))
}

const (
	//grantee type for user
	UserType = "CanonicalUser"
	//grantee type for group
	GroupType = "Group"
	//grantee type for owner canned acl
	OwnerType = "Owner"
	//grantee type for every one canned acl
	EveryType = "EveryOne"
)

func SetAcl(path string, grants []AclGrant) error {
	oa := make([]C.struct_oace, 0, len(grants))
	for _, og := range grants {
		var a C.struct_oace
		a.oe_type = C.OFAPI_ACE_TYPE_ALLOWED
		switch og.acltype {
		case UserType:
			a.oe_credtype = C.OFAPI_ACE_CRED_UID
			a.oe_cred = C.uint32_t(og.uid)
		case GroupType:
			a.oe_credtype = C.OFAPI_ACE_CRED_GID
			a.oe_cred = C.uint32_t(og.gid)
		case OwnerType:
			a.oe_credtype = C.OFAPI_ACE_CRED_OWNER
			a.oe_cred = C.uint32_t(og.uid)
		case EveryType:
			a.oe_credtype = C.OFAPI_ACE_CRED_EVERYONE
		default:
			return syscall.Errno(95)
		}
		a.oe_mask = C.uint32_t(og.aclbits)
		a.oe_flag = C.uint32_t(og.aclflag)
		oa = append(oa, a)
	}
	var aid C.uint64_t
	ret := C.ofapi_acladd(root.fs, toCPoint(oa), C.uint32_t(len(oa)), &aid)
	if ret != cok {
		logger.LogIf(nil, fmt.Errorf("SetAcl acladd fail err %v", opfsErr(ret)))
		return opfsErr(ret)
	}
	fd, err := open(path)
	defer C.ofapi_close(fd)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("SetAcl open %v fail err %v", path, err))
		return err
	}
	ret = C.ofapi_setattr(fd, C.uint32_t(0), C.uint32_t(0), C.uint32_t(0), aid)
	if ret != cok {
		logger.LogIf(nil, fmt.Errorf("SetAcl setattr %v fail err %v", path, err))
		return opfsErr(ret)
	}
	return nil
}

func (csoa *C.struct_oace) toSlice(slen int) []C.struct_oace {
	var as []C.struct_oace
	asHdr := (*reflect.SliceHeader)(unsafe.Pointer(&as))
	asHdr.Data = uintptr(unsafe.Pointer(csoa))
	asHdr.Len = slen
	asHdr.Cap = slen
	return as
}

func GetAcl(path string) ([]AclGrant, error) {
	fd, err := open(path)
	if err != nil {
		return []AclGrant{}, err
	}

	var oace *C.struct_oace
	var acecnt C.uint32_t
	ret := C.ofapi_aclqry(fd, &oace, &acecnt)
	if ret != cok {
		logger.LogIf(nil, fmt.Errorf("GetAcl aclqry %v fail %v", path, opfsErr(ret)))
		return []AclGrant{}, opfsErr(ret)
	}

	oa := oace.toSlice(int(acecnt))
	opfsgrants := make([]AclGrant, 0, int(acecnt))
	for i := 0; i < int(acecnt); i++ {
		if oa[i].oe_type == C.OFAPI_ACE_TYPE_DENIED {
			continue
		}
		var og AclGrant
		switch oa[i].oe_credtype {
		case C.OFAPI_ACE_CRED_UID:
			og.acltype = UserType
			og.uid = int(oa[i].oe_cred)
		case C.OFAPI_ACE_CRED_GID:
			og.acltype = GroupType
			og.gid = int(oa[i].oe_cred)
		case C.OFAPI_ACE_CRED_OWNER:
			og.acltype = OwnerType
			og.uid = int(oa[i].oe_cred)
		case C.OFAPI_ACE_CRED_EVERYONE:
			og.acltype = EveryType
		default:
			og.acltype = ""
		}
		if og.acltype == "" {
			continue
		}
		og.aclbits = int(oa[i].oe_mask)
		og.aclflag = int(oa[i].oe_flag)
		opfsgrants = append(opfsgrants, og)
	}
	return opfsgrants, nil
}

func GetOwner(path string) (uid, gid int, err error) {
	fd, err := open(path)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("GetOwner open %v fail err %v", path, err))
		return 0, 0, err
	}
	defer C.ofapi_close(fd)

	var oatt C.struct_oatt

	ret := C.ofapi_getattr(fd, &oatt)
	if ret != cok {
		return 0, 0, opfsErr(ret)
	}
	return int(oatt.oa_uid), int(oatt.oa_gid), nil
}
func MakeDirAll(path string, mode os.FileMode) error {
	path = strings.TrimPrefix(strings.TrimSuffix(path, SlashSeparator), root.rootpath)
	path = strings.TrimPrefix(path, SlashSeparator)
	ipaths := strings.Split(path, SlashSeparator)
	var p strings.Builder
	p.WriteString(root.rootpath)
	for _, s := range ipaths {
		dirPath := p.String() + SlashSeparator + s
		err := MakeDir(dirPath)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		p.WriteString(fmt.Sprintf("/%s", s))
	}

	return nil

}
