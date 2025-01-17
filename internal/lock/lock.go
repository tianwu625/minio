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

// Package lock - implements filesystem locking wrappers around an
// open file descriptor.
package lock

import (
	"errors"
	"io"
	"os"
	"sync"
)

// ErrAlreadyLocked is returned if the underlying fd is already locked.
var ErrAlreadyLocked = errors.New("file already locked")

// RLockedFile represents a read locked file, implements a special
// closer which only closes the associated *os.File when the ref count.
// has reached zero, i.e when all the readers have given up their locks.
type RLockedFile struct {
	*LockedFile
	mutex sync.Mutex
	refs  int // Holds read lock refs.
}

// IsClosed - Check if the rlocked file is already closed.
func (r *RLockedFile) IsClosed() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.refs == 0
}

// IncLockRef - is used by called to indicate lock refs.
func (r *RLockedFile) IncLockRef() {
	r.mutex.Lock()
	r.refs++
	r.mutex.Unlock()
}

// Close - this closer implements a special closer
// closes the underlying fd only when the refs
// reach zero.
func (r *RLockedFile) Close() (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.refs == 0 {
		return os.ErrInvalid
	}

	r.refs--
	if r.refs == 0 {
		err = r.File.Close()
	}

	return err
}

// Provides a new initialized read locked struct from *os.File
func newRLockedFile(lkFile *LockedFile) (*RLockedFile, error) {
	if lkFile == nil {
		return nil, os.ErrInvalid
	}

	return &RLockedFile{
		LockedFile: lkFile,
		refs:       1,
	}, nil
}

// RLockedOpenFile - returns a wrapped read locked file, if the file
// doesn't exist at path returns an error.
func RLockedOpenFile(path string, lockedOpenFile func(path string, flag int, perm os.FileMode) (*LockedFile, error)) (*RLockedFile, error) {
	lkFile, err := lockedOpenFile(path, os.O_RDONLY, 0o666)
	if err != nil {
		return nil, err
	}

	return newRLockedFile(lkFile)
}

type LockedFileInterface interface {
	io.ReaderAt
	io.Reader
	io.Writer
	io.Seeker
	Stat() (os.FileInfo, error)
	Truncate(int64) error
	io.Closer
	Name() string
}

// LockedFile represents a locked file
type LockedFile struct {
	File LockedFileInterface
}

func (l *LockedFile) Read(p []byte) (n int, err error) {
	return l.File.Read(p)
}

func (l *LockedFile) Seek(offset int64, whence int) (int64, error) {
	return l.File.Seek(offset, whence)
}

func (l *LockedFile) Close() error {
	return l.File.Close()
}

func (l *LockedFile) ReadAt(p []byte, off int64) (n int, err error) {
	return l.File.ReadAt(p, off)
}

func (l *LockedFile) Write(p []byte) (n int, err error) {
	return l.File.Write(p)
}

func (l *LockedFile) Stat() (os.FileInfo, error) {
	return l.File.Stat()
}

func (l *LockedFile) Truncate(size int64) error {
	return l.File.Truncate(size)
}

func (l *LockedFile) Name() string {
	return l.File.Name()
}
