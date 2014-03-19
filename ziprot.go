package ziprot

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

type ZipRot struct {
	base     string
	_file    unsafe.Pointer
	size     int64
	maxFiles int64
	maxSize  int64
	lock     *sync.Mutex
}

func New(base string) (self *ZipRot, err error) {
	self = &ZipRot{
		base: base,
		lock: &sync.Mutex{},
	}
	if err = self.open(); err != nil {
		return
	}
	if err = self.maybeRotate(); err != nil {
		return
	}
	return
}

func (self *ZipRot) open() (err error) {
	file, err := os.OpenFile(self.base, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		return
	}
	stat, err := file.Stat()
	if err != nil {
		return
	}
	atomic.StorePointer(&self._file, unsafe.Pointer(file))
	atomic.StoreInt64(&self.size, stat.Size())
	return
}

func (self *ZipRot) file() *os.File {
	return (*os.File)(atomic.LoadPointer(&self._file))
}

func (self *ZipRot) MaxFiles(n int64) *ZipRot {
	atomic.StoreInt64(&self.maxFiles, n)
	return self
}

func (self *ZipRot) MaxSize(n int64) *ZipRot {
	atomic.StoreInt64(&self.maxSize, n)
	return self
}

func (self *ZipRot) Write(p []byte) (n int, err error) {
	n, err = self.file().Write(p)
	if err != nil {
		return
	}
	atomic.AddInt64(&self.size, int64(n))
	if err = self.maybeRotate(); err != nil {
		return
	}
	return
}

func (self *ZipRot) Close() error {
	return self.file().Close()
}

func (self *ZipRot) maybeRotate() (err error) {
	if atomic.LoadInt64(&self.size) > atomic.LoadInt64(&self.maxSize) {
		return self.rotate()
	}
	return
}

func (self *ZipRot) freeName(n int) (err error) {
	name := fmt.Sprintf("%v.%v", self.base, n)
	_, err = os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	if int64(n) >= atomic.LoadInt64(&self.maxFiles) {
		return os.Remove(name)
	}
	if err = self.freeName(n + 1); err != nil {
		return
	}
	return os.Rename(name, fmt.Sprintf("%v.%v", self.base, n+1))
}

func (self *ZipRot) rotate() (err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	if err = self.freeName(1); err != nil {
		return
	}
	if err = os.Rename(self.base, fmt.Sprintf("%v.1", self.base)); err != nil {
		return
	}
	oldFile := self.file()
	if err = self.open(); err != nil {
		return
	}
	return oldFile.Sync()
}
