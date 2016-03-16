package blobstore

import (
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"crypto/sha256"
)

type GarbageCollector interface {
	Find(s Store) ([]Object, error)
}

type DumbGarbageCollector struct{}

func (d DumbGarbageCollector) Find(s Store) ([]Object, error) {
	linked, err := s.Linked()
	if err != nil {
		return nil, err
	}
	list, err := s.List()
	if err != nil {
		return nil, err
	}

	ret := []Object{}
	for _, node := range list {
		if _, ok := linked[node]; !ok {
			ret = append(ret, node)
		}
	}
	return ret, nil
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

func (s Store) Open(o Object) (io.ReadCloser, error) {
	fd, err := os.Open(s.objToPath(o))
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func (s Store) Link(o Object, targetPath string) error {
	if !s.Exists(o) {
		return fmt.Errorf("No commited blob: '%s'", o.Id())
	}
	storePath := s.objToPath(o)
	stagePath := s.qualifyStagePath(targetPath)

	if err := os.MkdirAll(path.Dir(stagePath), 0755); err != nil {
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
		blobRoot:       ".blobs/store",
		tempRoot:       ".blobs/new",
		stageRoot:      "",
		objectIDHasher: sha256.New,
	}, nil
}

func (s Store) Linked() (map[Object][]string, error) {
	seen := map[Object][]string{}

	blobRoot := path.Clean(path.Join(s.root, s.blobRoot))

	err := filepath.Walk(
		path.Join(s.root, s.stageRoot),

		func(p string, f os.FileInfo, err error) error {
			if f.IsDir() || strings.HasPrefix(path.Clean(p), blobRoot) {
				return nil
			}
			link, err := os.Readlink(p)
			if err != nil {
				/* The only error is of type PathError */
				return nil
			}

			if !strings.HasPrefix(path.Clean(link), blobRoot) {
				return nil
			}
			_, hash := path.Split(link)
			obj := Object{id: hash}

			seen[obj] = append(seen[obj], p)

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return seen, nil
}

func (s Store) List() ([]Object, error) {
	objectList := []Object{}

	err := filepath.Walk(
		path.Join(s.root, s.blobRoot),
		func(p string, f os.FileInfo, err error) error {
			if f.IsDir() {
				return nil
			}
			_, hash := path.Split(p)
			objectList = append(objectList, Object{id: hash})
			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return objectList, nil
}

func (s Store) GC(gc GarbageCollector) error {
	nodes, err := gc.Find(s)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if err := s.Remove(node); err != nil {
			return err
		}
	}
	return nil
}

func (s Store) Remove(o Object) error {
	if !s.Exists(o) {
		return fmt.Errorf("No such object: '%s'", o.Id())
	}

	path := s.objToPath(o)
	return os.Remove(path)
}

func (s Store) Create() (*Writer, error) {
	dir := path.Join(s.root, s.tempRoot)

	if err := os.MkdirAll(dir, 0755); err != nil {
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
	if err := os.MkdirAll(path.Dir(objPath), 0755); err != nil {
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
