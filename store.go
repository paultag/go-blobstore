package blobstore

import (
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"crypto/sha256"
)

func mkdirP(p string) error {
	return os.MkdirAll(path.Dir(p), 0700)
}

type hashFunc func() hash.Hash

type Store struct {
	root      string
	blobRoot  string
	stageRoot string
	tempRoot  string

	objectIDHasher hashFunc
}

func (s Store) Exists(o Object) bool {
	_, err := os.Stat(s.objToPath(o))
	return !os.IsNotExist(err)
}

func (s Store) Link(o Object, path string) error {
	if !s.Exists(o) {
		return fmt.Errorf("No commited blob: '%s'", o.Id())
	}
	storePath := s.objToPath(o)
	stagePath := s.qualifyStagePath(path)

	if err := mkdirP(stagePath); err != nil {
		return err
	}

	_, err := os.Stat(stagePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		if err := os.Remove(stagePath); err != nil {
			return err
		}
	}

	return os.Symlink(storePath, stagePath)
}

func (s Store) Load(hash string) (*Object, error) {
	o := Object{id: hash}
	if s.Exists(o) {
		return &o, nil
	}
	return nil, fmt.Errorf("No such object: '%s'", hash)
}

func (s Store) qualifyBlobPath(p string) string {
	return path.Join(s.root, s.blobRoot, p)
}

func (s Store) qualifyStagePath(p string) string {
	return path.Join(s.root, s.stageRoot, p)
}

func (s Store) objToPath(o Object) string {
	id := o.Id()
	return s.qualifyBlobPath(path.Join(id[0:1], id[1:2], id[2:6], id))
}

func Load(path string) (*Store, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	return &Store{
		root:           absPath,
		blobRoot:       ".blobs",
		tempRoot:       ".blobs/new",
		stageRoot:      "",
		objectIDHasher: sha256.New,
	}, nil
}

func (s Store) Create() (*Writer, error) {
	dir := path.Join(s.root, s.tempRoot)

	if err := mkdirP(path.Join(dir, "foo")); err != nil {
		return nil, err
	}

	fd, err := ioutil.TempFile(dir, "blob")
	if err != nil {
		return nil, err
	}
	hashWriter := s.objectIDHasher()

	return &Writer{
		path:   fd.Name(),
		writer: fd,
		target: io.MultiWriter(fd, hashWriter),
		hash:   hashWriter,
	}, nil
}

type Writer struct {
	path   string
	writer io.WriteCloser
	target io.Writer
	hash   hash.Hash
}

func (n Writer) Close() error {
	return n.writer.Close()
}

func (n Writer) Write(b []byte) (int, error) {
	return n.target.Write(b)
}

func (s Store) Commit(w Writer) (*Object, error) {
	err := w.writer.Close()
	if err != nil {
		return nil, err
	}
	oid := fmt.Sprintf("%x", w.hash.Sum(nil))
	obj := Object{id: oid}
	objPath := s.objToPath(obj)
	if err := mkdirP(objPath); err != nil {
		return nil, err
	}
	err = os.Rename(w.path, objPath)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

type Object struct {
	id string
}

func (o Object) Id() string {
	return o.id
}
