package ziprot

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type ZipWriter struct {
	closed     int64
	file       *os.File
	zipFile    *os.File
	zipWriter  *gzip.Writer
	zipChannel chan []byte
	zipDone    chan struct{}
}

func NewZipWriter(base string) (self *ZipWriter, err error) {
	self = &ZipWriter{
		zipChannel: make(chan []byte),
		zipDone:    make(chan struct{}, 128),
	}
	if self.file, err = os.Create(base); err != nil {
		return
	}
	if self.zipFile, err = os.Create(fmt.Sprintf("%v.gz", base)); err != nil {
		return
	}
	self.zipWriter = gzip.NewWriter(self.zipFile)
	go self.zip()
	return
}

func (self *ZipWriter) zip() {
	for b := range self.zipChannel {
		self.zipWriter.Write(b)
	}
	close(self.zipDone)
}

func (self *ZipWriter) Size() (result int64, err error) {
	stat, err := self.file.Stat()
	if err != nil {
		return
	}
	result = stat.Size()
	return
}

func (self *ZipWriter) Closed() bool {
	return atomic.LoadInt64(&self.closed) == 1
}

func (self *ZipWriter) Close() (err error) {
	if atomic.CompareAndSwapInt64(&self.closed, 0, 1) {
		close(self.zipChannel)
		<-self.zipDone
		if err = self.zipWriter.Close(); err != nil {
			return
		}
		if err = self.zipFile.Close(); err != nil {
			return
		}
		if err = self.file.Close(); err != nil {
			return
		}
	}
	return
}

func (self *ZipWriter) Write(p []byte) (n int, err error) {
	if atomic.LoadInt64(&self.closed) == 1 {
		err = fmt.Errorf("Writer closed")
		return
	}
	if n, err = self.file.Write(p); err != nil {
		return
	}
	self.zipChannel <- p
	return
}

func (self *ZipWriter) Sync() (err error) {
	return self.file.Sync()
}

type ZipRot struct {
	base       string
	_zipWriter unsafe.Pointer
	maxFiles   int64
	maxSize    int64
	rotators   int64
	nonblock   int64
}

func New(base string) (self *ZipRot, err error) {
	self = &ZipRot{
		base: base,
	}
	_, err = os.Stat(self.base)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if err = self.gzip(self.base, fmt.Sprintf("%v.gz", self.base)); err != nil {
			return
		}
	}
	if err = self.rotate(nil); err != nil {
		return
	}
	return
}

func (self *ZipRot) gzip(src, dst string) (err error) {
	reader, err := os.Open(src)
	if err != nil {
		return
	}
	defer reader.Close()
	writer, err := os.Create(dst)
	if err != nil {
		return
	}
	defer writer.Close()
	zipWriter := gzip.NewWriter(writer)
	defer zipWriter.Close()
	if _, err = io.Copy(zipWriter, reader); err != nil {
		return
	}
	return
}

func (self *ZipRot) freeName(n int) (err error) {
	name := fmt.Sprintf("%v.gz.%v", self.base, n)
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
	return os.Rename(name, fmt.Sprintf("%v.gz.%v", self.base, n+1))
}

func (self *ZipRot) rotate(oldZipWriter *ZipWriter) (err error) {
	if atomic.CompareAndSwapInt64(&self.rotators, 0, 1) {
		defer atomic.StoreInt64(&self.rotators, 0)
		if err = self.freeName(1); err != nil {
			err = fmt.Errorf("Trying to free %v.gz.1: %v", self.base, err)
			return
		}
		if err = os.Rename(fmt.Sprintf("%v.gz", self.base), fmt.Sprintf("%v.gz.1", self.base)); err != nil {
			if os.IsNotExist(err) {
				err = nil
			} else {
				err = fmt.Errorf("Trying to rename %v.gz to %v.gz.1: %v", self.base, self.base, err)
				return
			}
		}
		var newZipWriter *ZipWriter
		newZipWriter, err = NewZipWriter(self.base)
		if err != nil {
			return
		}
		runtime.SetFinalizer(newZipWriter, func(f *ZipWriter) {
			f.Close()
		})
		atomic.StorePointer(&self._zipWriter, unsafe.Pointer(newZipWriter))
		if oldZipWriter != nil {
			if err = oldZipWriter.Close(); err != nil {
				err = fmt.Errorf("Trying to sync old file: %v", err)
				return
			}
		}
	}
	return
}

func (self *ZipRot) zipWriter() *ZipWriter {
	return (*ZipWriter)(atomic.LoadPointer(&self._zipWriter))
}

func (self *ZipRot) MaxFiles(n int64) *ZipRot {
	atomic.StoreInt64(&self.maxFiles, n)
	return self
}

func (self *ZipRot) MaxSize(n int64) *ZipRot {
	atomic.StoreInt64(&self.maxSize, n)
	return self
}

func (self *ZipRot) Block(b bool) *ZipRot {
	if b {
		atomic.StoreInt64(&self.nonblock, 0)
	} else {
		atomic.StoreInt64(&self.nonblock, 1)
	}
	return self
}

func (self *ZipRot) Write(p []byte) (n int, err error) {
	var zipWriter *ZipWriter
	for {
		zipWriter = self.zipWriter()
		if n, err = zipWriter.Write(p); err == nil {
			break
		} else if !zipWriter.Closed() {
			return
		}
	}
	size, err := zipWriter.Size()
	if err != nil {
		return
	}
	if size > atomic.LoadInt64(&self.maxSize) {
		if atomic.LoadInt64(&self.nonblock) == 0 {
			if err = self.rotate(zipWriter); err != nil {
				return
			}
		} else {
			go func() {
				if err := self.rotate(zipWriter); err != nil {
					log.Printf("While trying to rotate: %v", err)
				}
			}()
		}
	}
	return
}

func (self *ZipRot) Close() error {
	return self.zipWriter().Close()
}
