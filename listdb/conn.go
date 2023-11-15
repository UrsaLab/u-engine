package listdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"

	"github.com/UrsaLab/u-engine/listdb/internal/filelock"
)

const stepCacheOffset = 32 * 1024

type Conn struct {
	file   *os.File
	locked bool

	closed bool

	isEnd bool
	cur   cursor
	cache []cursor
}

type cursor struct {
	Index, Pos int64
}

func Open(path string) (*Conn, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	if err = filelock.Lock(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	runtime.SetFinalizer(file, func(f *os.File) {
		panic(fmt.Sprintf("file %q became unreachable without a call to Close", f.Name()))
	})

	return &Conn{
		file:   file,
		locked: true,
		cache:  []cursor{{0, 0}},
	}, nil
}

func OpenReadOnly(path string) (*Conn, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &Conn{
		file:   file,
		locked: false,
		cache:  []cursor{{0, 0}},
	}, nil
}

func (c *Conn) Read() ([]byte, error) {
	elem, err := c.readElem()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if err = c.fileSeek(c.cur.Pos); err != nil {
				return nil, err
			}
			if c.locked {
				if err = c.file.Truncate(c.cur.Pos); err == nil {
					err = io.EOF
				}
			}
		}
		if err == io.EOF {
			c.isEnd = true
		}
		return nil, err
	}
	return elem, nil
}

func (c *Conn) Append(elem []byte) error {
	if elem == nil {
		panic("cannot append nil")
	}
	if !c.isEnd {
		if err := c.seekEnd(); err != nil {
			return err
		}
	}
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(len(elem)))
	frame := append(buf[:n], elem...)
	if _, err := bytes.NewReader(frame).WriteTo(c.file); err != nil {
		return err
	}
	c.moveCursor(int64(len(frame)))
	return nil
}

func (c *Conn) Commit() error {
	return c.file.Sync()
}

func (c *Conn) Truncate(size int64) error {
	if _, err := c.seek(size, io.SeekStart); err != nil {
		return err
	}
	return c.file.Truncate(c.cur.Pos)
}

func (c *Conn) Close() error {
	if c.closed {
		return fs.ErrClosed
	}
	c.closed = true

	var err error
	if c.locked {
		runtime.SetFinalizer(c.file, nil)
		err = filelock.Unlock(c.file)
	}

	if closeErr := c.file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (c *Conn) seek(offset int64, whence int) (int64, error) {
	var index int64
	switch whence {
	case io.SeekStart:
		index = offset
	case io.SeekCurrent:
		index = c.cur.Index + offset
	case io.SeekEnd:
		if err := c.seekEnd(); err != nil {
			return -1, err
		}
		index = c.cur.Index + offset
	default:
		panic("invalid whence")
	}

	if index == c.cur.Index {
		return index, nil
	}
	if index < 0 {
		return -1, errors.New("seek before start")
	}

	var ref *cursor
	var cacheEnd int
	for left, right := 0, len(c.cache); ; {
		mid := (left + right) / 2
		if cacheMid := &c.cache[mid]; cacheMid.Index > index {
			right = mid
		} else if left != mid {
			left = mid
		} else {
			ref = cacheMid
			cacheEnd = mid + 1
			break
		}
	}

	if index < c.cur.Index || ref.Index > c.cur.Index {
		if err := c.fileSeek(ref.Pos); err != nil {
			return -1, err
		}
		c.cur = *ref
	}
	c.cache = c.cache[:cacheEnd]

	c.isEnd = false
	for c.cur.Index != index {
		if _, err := c.Read(); err != nil {
			if err == io.EOF {
				err = errors.New("seek after end")
			}
			return -1, err
		}
	}
	return index, nil
}

func (c *Conn) seekEnd() error {
	cacheEnd := &c.cache[len(c.cache)-1]
	if err := c.fileSeek(cacheEnd.Pos); err != nil {
		return err
	}
	c.cur = *cacheEnd

	for {
		if _, err := c.Read(); err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
	}
}

func (c *Conn) fileSeek(offset int64) error {
	if sought, err := c.file.Seek(offset, io.SeekStart); err != nil {
		return err
	} else if sought != offset {
		panic("sought offset mismatch")
	}
	return nil
}

func (c *Conn) readElem() ([]byte, error) {
	b := make([]byte, binary.MaxVarintLen64)
	nb, err := c.file.Read(b)
	if err != nil {
		if err != io.EOF || nb == 0 {
			return nil, err
		}
	}

	size, n := binary.Varint(b[:nb])
	if n == 0 {
		return nil, io.ErrUnexpectedEOF
	} else if n < 0 {
		return nil, errors.New("malformed data")
	}

	if _, err = c.file.Seek(int64(n-nb), io.SeekCurrent); err != nil {
		return nil, err
	}

	elem := make([]byte, size)
	if _, err = io.ReadFull(c.file, elem); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	c.moveCursor(size + int64(n))
	return elem, nil
}

func (c *Conn) moveCursor(offset int64) {
	c.cur.Index++
	c.cur.Pos += offset
	if c.cur.Pos-c.cache[len(c.cache)-1].Pos >= stepCacheOffset {
		c.cache = append(c.cache, c.cur)
	}
}
