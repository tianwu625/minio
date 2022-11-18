package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	pathutil "path"
	"reflect"
	"runtime"
	"strings"
	"time"
	"unsafe"

	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/opfs"
)

func opfsMkdirWithCred(ctx context.Context, dirPath string) (err error) {
	if err := setUserCred(ctx); err != nil {
		return err
	}
	return opfsMkdir(ctx, dirPath)
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

	if err = opfs.MakeDir(dirPath); err != nil {
		switch {
		case osIsExist(err):
			return errVolumeExists
		case osIsPermission(err):
			logger.LogIf(ctx, errDiskAccessDenied)
			return errDiskAccessDenied
		case isSysErrNotDir(err):
			logger.LogIf(ctx, errDiskAccessDenied)
			return errDiskAccessDenied
		default:
			logger.LogIf(ctx, err)
			return err
		}
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

	if err := setUserCred(ctx); err != nil {
		return err
	}

	if err := opfs.RemoveFile(filePath); err != nil {
		return osErrToFileErr(err)
	}

	return nil
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

	if err := setUserCred(ctx); err != nil {
		return err
	}

	if err := opfs.RemoveDir(dirPath); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrNotEmpty(err) {
			return errVolumeNotEmpty
		}
		return err
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

	if err := setUserCred(ctx); err != nil {
		return err
	}
	return opfsRemoveAllPath(dirPath)
}

func opfsReadDirWithOpts(dirPath string, opts readDirOpts) (entries []string, err error) {
	count := opts.count

	opf, err := opfs.Open(dirPath)
	if err != nil {
		return entries, osErrToFileErr(err)
	}
	defer opf.Close()

	entries, err = opf.Readdirnames(count)
	if err != nil && err != io.EOF {
		return nil, osErrToFileErr(err)
	}

	return entries, nil
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
	if err := setUserCred(ctx); err != nil {
		return nil, err
	}
	ofi, err := opfs.Stat(statLoc)
	if err != nil {
		return nil, err
	}

	return ofi, nil
}

func opfsStatVolume(ctx context.Context, volume string) (os.FileInfo, error) {
	ofi, err := opfsStat(ctx, volume)

	if err != nil {
		if osIsNotExist(err) {
			return nil, errVolumeNotFound
		} else if osIsPermission(err) {
			return nil, errVolumeAccessDenied
		}
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
		err = osErrToFileErr(err)
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
		err = osErrToFileErr(err)
		return nil, err
	}

	if ofi.IsDir() {
		return nil, errFileNotFound
	}

	return ofi, nil
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

	if err := opfs.Rename(sourcePath, destPath); err != nil {
		return err
	}

	return nil
}

func opfsRenameFileWithCred(ctx context.Context, sourcePath, destPath string) error {
	if err := setUserCred(ctx); err != nil {
		logger.LogIf(ctx, fmt.Errorf("set user cred failed %v", err))
		return err
	}
	return opfsRenameFile(ctx, sourcePath, destPath)
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

	if err := setUserCred(ctx); err != nil {
		return 0, err
	}

	if err := opfsMkdirAll(pathutil.Dir(filePath), 0o777); err != nil {
		switch {
		case osIsPermission(err):
			return 0, errFileAccessDenied
		case osIsExist(err):
			return 0, errFileAccessDenied
		case isSysErrIO(err):
			return 0, errFaultyDisk
		case isSysErrInvalidArg(err):
			return 0, errUnsupportedDisk
		case isSysErrNoSpace(err):
			return 0, errDiskFull
		}
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

	if err := setUserCred(ctx); err != nil {
		return err
	}

	now := time.Now()
	if err := opfs.Utime(statLoc, now); err != nil {
		return err
	}

	return nil
}

func opfsReadFile(name string) ([]byte, error) {
	opf, err := opfs.Open(name)
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

	if err := setUserCred(ctx); err != nil {
		logger.LogIf(ctx, err)
		return nil, 0, err
	}

	fr, err := opfs.Open(readPath)
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
	appendFile, err := opfs.OpenWithCreate(dst, flags, 0o666)
	if err != nil {
		return err
	}
	defer appendFile.Close()

	srcFile, err := opfs.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	_, err = io.Copy(appendFile, srcFile)
	return err
}

func opfsWriteFile(name string, data []byte, perm os.FileMode) error {
	f, err := opfs.OpenWithCreate(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
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
	opf, err := opfs.Open(path)
	if err != nil {
		if osIsNotExist(err) {
			return nil
		}
		return err
	}
	defer opf.Close()

	ofi, err := opf.Stat()
	if err != nil {
		if osIsNotExist(err) {
			return nil
		}
		return err
	}

	if ofi.IsDir() {
		err := opfs.RemoveDir(path)
		if err != nil {
			if osIsNotExist(err) {
				return nil
			} else if isSysErrNotEmpty(err) {
				return nil
			}
			return err
		}
	} else {
		err := opfs.RemoveFile(path)
		if err != nil {
			return osErrToFileErr(err)
		}
	}

	return nil
}

func opfsRemoveAllPath(dirPath string) (err error) {
	opf, err := opfs.Open(dirPath)
	if err != nil {
		if osIsNotExist(err) {
			return nil
		}
		return err
	}
	defer opf.Close()

	ofi, err := opf.Stat()
	if err != nil {
		if osIsNotExist(err) {
			return nil
		}
		return err
	}

	if ofi.IsDir() {
		names, err := opf.Readdirnames(-1)
		if err != nil && err != io.EOF {
			if osIsNotExist(err) {
				return nil
			}
			return err
		}
		for _, name := range names {
			newPath := dirPath + SlashSeparator + strings.TrimSuffix(name, SlashSeparator)
			err := opfsRemoveAllPath(newPath)
			if err != nil {
				return err
			}
		}
		err = opfs.RemoveDir(dirPath)
		if err != nil {
			if osIsNotExist(err) {
				return nil
			} else if isSysErrNotEmpty(err) {
				return errVolumeNotEmpty
			}
			return err
		}
	} else {
		err := opfs.RemoveFile(dirPath)
		if err != nil {
			return osErrToFileErr(err)
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

	if err := setUserCred(ctx); err != nil {
		return err
	}

	if err := opfsdeleteFile(basePath, deletePath, false); err != nil {
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

const (
	opfsCredKey = "opfscred"
	//s3 acl Max Length is 5, include READ,WRITE,ACLREAD,ACLWRITE,FULLCONTROL
	permissionMaxLen = 5
)

var (
	errNoCred = errors.New("no cred info")
)

func getOpfsCred(ctx context.Context) (uid int, gids []int, err error) {
	claims, ok := ctx.Value(opfsCredKey).(map[string]interface{})
	if !ok {
		logger.LogIf(ctx, errNoCred)
		return uid, gids, errNoCred
	}
	if uid, ok := claims[UserID].(int); !ok {
		return uid, gids, errInvalidArgument
	}
	if gid, ok := claims[GroupID].(int); !ok {
		return uid, gids, errInvalidArgument
	} else {
		gids = append(gids, gid)
	}
	if sgids, ok := claims[GroupIDs].([]int); ok {
		gids = append(gids, sgids...)
	}
	return uid, gids, nil
}

func setUserCred(ctx context.Context) error {
	uid, gids, err := getOpfsCred(ctx)
	if err != nil {
		return err
	}
	if err := opfs.SetCred(uid, gids); err != nil {
		return osErrToFileErr(err)
	}
	return nil
}

var (
	rootUid  = 0
	rootGids = []int{0}
)

func setOpfsSessionRoot() {
	opfs.SetCred(rootUid, rootGids)
	return
}

func opfsMkdirAllWithCred(ctx context.Context, path string, mode os.FileMode) error {
	if err := setUserCred(ctx); err != nil {
		return err
	}
	if err := opfsMkdirAll(path, mode); err != nil {
		return err
	}
	return nil
}

func opfsOpenWithCred(ctx context.Context, path string) (*opfs.OpfsFile, error) {
	if err := setUserCred(ctx); err != nil {
		return nil, err
	}
	opf, err := opfs.Open(path)
	if err != nil {
		return opf, err
	}
	return opf, err
}

func s3mapopfs(perm string, isDir bool) (int, int) {
	var aclbits, aclflag int
	aclflag = opfs.AclFlagDefault
	switch perm {
	case GrantPermRead:
		if isDir {
			aclbits = opfs.AclDirRead
		} else {
			aclbits = opfs.AclFileRead
		}
	case GrantPermWrite:
		if isDir {
			aclbits = opfs.AclDirWrite
			aclflag = opfs.AclFlagInherit
		} else {
			aclbits = opfs.AclFileWrite
		}
	case GrantPermReadAcp:
		aclbits = opfs.AclRead
	case GrantPermWriteAcp:
		aclbits = opfs.AclWrite
	case GrantPermFullControl:
		if isDir {
			aclbits = opfs.AclDirFullControl
		} else {
			aclbits = opfs.AclFileFullControl
		}
	default:
	}

	return aclbits, aclflag
}

func opfsmaps3List(opfsacl int, isDir bool) []string {
	permissionList := make([]string, 0, permissionMaxLen)
	if isDir {
		if opfsacl&opfs.AclDirFullControl == opfs.AclDirFullControl {
			permissionList = append(permissionList, GrantPermFullControl)
			return permissionList
		}
		if opfsacl&opfs.AclDirRead == opfs.AclDirRead {
			permissionList = append(permissionList, GrantPermRead)
		}
		if opfsacl&opfs.AclDirWrite == opfs.AclDirWrite {
			permissionList = append(permissionList, GrantPermWrite)
		}
	} else {
		if opfsacl&opfs.AclFileFullControl == opfs.AclFileFullControl {
			permissionList = append(permissionList, GrantPermFullControl)
			return permissionList
		}
		if opfsacl&opfs.AclFileRead == opfs.AclFileRead {
			permissionList = append(permissionList, GrantPermRead)
		}
		// object no write permission, have write permission only from parent dir inherit
		// so opfs minio not show this permission to get Acl
	}
	if opfsacl&opfs.AclRead == opfs.AclRead {
		permissionList = append(permissionList, GrantPermReadAcp)
	}
	if opfsacl&opfs.AclWrite == opfs.AclWrite {
		permissionList = append(permissionList, GrantPermWriteAcp)
	}

	return permissionList
}

//for acl operate
type opfsAcl struct {
	uid     int
	gid     int
	acltype string
	aclbits int
	aclflag int
}

func opfsSetAcl(path string, grants []opfsAcl) error {
	var acls []opfs.AclGrant
	aclsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&acls))
	grantsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&grants))
	aclsHdr.Data = grantsHdr.Data
	aclsHdr.Len = grantsHdr.Len
	aclsHdr.Cap = grantsHdr.Cap
	if err := opfs.SetAcl(path, acls); err != nil {
		return err
	}

	return nil
}

func opfsSetAclWithRoot(path string, grants []opfsAcl) error {
	setOpfsSessionRoot()
	return opfsSetAcl(path, grants)
}

func opfsSetAclWithCred(ctx context.Context, path string, grants []opfsAcl) error {
	if err := setUserCred(ctx); err != nil {
		return err
	}
	return opfsSetAcl(path, grants)
}

func opfsGetAcl(path string) ([]opfsAcl, error) {
	acls, err := opfs.GetAcl(path)
	if err != nil {
		return []opfsAcl{}, err
	}
	var grants []opfsAcl
	aclsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&acls))
	grantsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&grants))
	grantsHdr.Data = aclsHdr.Data
	grantsHdr.Len = aclsHdr.Len
	grantsHdr.Cap = aclsHdr.Cap

	return grants, nil
}

func opfsGetAclWithCred(ctx context.Context, path string) ([]opfsAcl, error) {
	if err := setUserCred(ctx); err != nil {
		return nil, err
	}
	opfsgrants, err := opfsGetAcl(path)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("get acl from path %v failed %v", path, err))
		return opfsgrants, err
	}

	return opfsgrants, err
}

func opfsGetUidGid(path string) (int, int, error) {
	setOpfsSessionRoot()
	uid, gid, err := opfs.GetOwner(path)
	if err != nil {
		return 0, 0, err
	}
	return uid, gid, nil
}

func opfsGetInheritAclFromDir(path string) ([]opfsAcl, error) {
	setOpfsSessionRoot()
	opfsgrants, err := opfsGetAcl(path)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("get acl from path %v failed %v", path, err))
		return opfsgrants, err
	}
	inheritGrants := make([]opfsAcl, 0, len(opfsgrants))
	for _, g := range opfsgrants {
		if g.aclflag == opfs.AclFlagInherit &&
			g.aclbits&opfs.AclDirWrite == opfs.AclDirWrite {
			inheritGrants = append(inheritGrants, g)
		}
	}

	return inheritGrants, nil
}

func inGroups(gid int, gids []int) bool {
	for _, id := range gids {
		if gid == id {
			return true
		}
	}

	return false
}

func opfsMkdirAll(path string, perm os.FileMode) error {
	return opfs.MakeDirAll(path, perm)
}

func opfsRename(src, dst string) error {
	return opfs.Rename(src, dst)
}

func opfsCinit(path string) error {
	return opfs.Init(path)
}

func opfsStatPath(filePath string) (os.FileInfo, error) {
	return opfs.Stat(filePath)
}
