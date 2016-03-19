package blobstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"crypto/sha256"
)

// Load {{{

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

// }}}

// Store {{{

type Store struct {
	root      string
	blobRoot  string
	stageRoot string
	tempRoot  string

	objectIDHasher hashFunc
}

// Exists {{{

func (s Store) Exists(o Object) bool {
	_, err := os.Stat(s.objToPath(o))
	return !os.IsNotExist(err)
}

// }}}

// Open {{{

func (s Store) Open(o Object) (io.ReadCloser, error) {
	fd, err := os.Open(s.objToPath(o))
	if err != nil {
		return nil, err
	}
	return fd, nil
}

// }}}

// Link {{{

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

// }}}

// Load {{{

func (s Store) Load(hash string) (*Object, error) {
	o := Object{id: hash}
	if s.Exists(o) {
		return &o, nil
	}
	return nil, fmt.Errorf("No such object: '%s'", hash)
}

// }}}

// Visitor {{{

func (s Store) LinkedVisitor(progn func(Object, string, os.FileInfo) error) error {
	blobRoot := path.Clean(path.Join(s.root, s.blobRoot))
	return filepath.Walk(
		path.Join(s.root, s.stageRoot),
		func(p string, f os.FileInfo, err error) error {
			p = path.Clean(p)

			/* For each file in the stage (but anything that's not in the
			 * blob root), let's read the link. If it's a symlink, call the
			 * visitor, and move on */
			if f.IsDir() || strings.HasPrefix(path.Clean(p), blobRoot) {
				return nil
			}
			link, err := os.Readlink(p)
			if err != nil {
				/* The only error is of type PathError */
				return nil
			}

			if !strings.HasPrefix(path.Clean(link), blobRoot) {
				/* If the link is pointing outside the blobRoot, we don't
				 * care to visit it */
				return nil
			}
			_, hash := path.Split(link)
			obj := Object{id: hash}
			return progn(obj, p, f)
		},
	)
}

// }}}

// Linked {{{

func (s Store) Linked() (map[Object][]string, error) {
	seen := map[Object][]string{}
	err := s.LinkedVisitor(func(obj Object, p string, info os.FileInfo) error {
		seen[obj] = append(seen[obj], p)
		return nil
	})
	return seen, err
}

// }}}

// List {{{

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

// }}}

// GC {{{

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

// }}}

// Remove {{{

func (s Store) Remove(o Object) error {
	if !s.Exists(o) {
		return fmt.Errorf("No such object: '%s'", o.Id())
	}

	path := s.objToPath(o)
	return os.Remove(path)
}

// }}}

// Create {{{

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

// }}}

// path helpers {{{

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

// }}}

// }}}

// vim: foldmethod=marker
