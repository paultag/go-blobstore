package blobstore

import (
	"fmt"
	"hash"
	"io"
	"os"
	"path"
)

type Writer struct {
	path   string
	writer io.WriteCloser
	target io.Writer
	hash   hash.Hash
}

// io.WriteCloser interface {{{

func (n Writer) Write(b []byte) (int, error) {
	return n.target.Write(b)
}

func (n Writer) Close() error {
	return n.writer.Close()
}

// }}}

// Commit {{{

func (s Store) Commit(w Writer) (*Object, error) {
	err := w.writer.Close()
	if err != nil {
		return nil, err
	}
	oid := fmt.Sprintf("%x", w.hash.Sum(nil))
	obj := Object{id: oid}
	objPath := s.objToPath(obj)
	if err := os.MkdirAll(path.Dir(objPath), 0755); err != nil {
		return nil, err
	}
	err = os.Rename(w.path, objPath)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// }}}

// vim: foldmethod=marker
