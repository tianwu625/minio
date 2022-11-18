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
	"os"
	"syscall"

	"github.com/minio/minio/internal/opfs"
)

func lockedOpfsOpenFile(path string, flag int, perm os.FileMode, lockType int) (*LockedFile, error) {
	f, err := opfs.OpenWithCreate(path, flag, perm)
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

func OpfsCinit(fsPath string) (err error) {
	return opfs.Init(fsPath)
}

func OpfsOpen(path string, flag int, perm os.FileMode) (*opfs.OpfsFile, error) {
	return opfs.OpenWithCreate(path, flag, perm)
}
