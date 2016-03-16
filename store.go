package blobstore

import (
	"fmt"
	"hash"
	"io"
	"os"
	"path"

	"crypto/sha256"
)

type hashFunc func() hash.Hash

type Store struct {
	root      string
	blobRoot  string
	stageRoot string

	objectIDHasher hashFunc
}

func (s Store) Exists(o Object) bool {
	_, err := os.Stat(s.objToPath(o))
	return !os.IsNotExist(err)
}

func (s Store) Load(hash string) (*Object, error) {
	o := Object{id: hash}
	if s.Exists(o) {
		return &o, nil
	}
	return nil, fmt.Errorf("No such object: '%s'", hash)
}

func (s Store) qualifyPath(p string) string {
	return path.Join(s.root, s.blobRoot, p)
}

func (s Store) objToPath(o Object) string {
	id := o.Id()
	return s.qualifyPath(path.Join(id[0:1], id[1:2], id[2:6], id))
}

func Load(path string) Store {
	return Store{
		root:           path,
		blobRoot:       ".blobs",
		stageRoot:      "",
		objectIDHasher: sha256.New,
	}
}

func (s Store) Create() (*Writer, error) {
	// writer is the fd object
	// target is the fd object merged with the hash
	return nil, nil
}

type Writer struct {
	writer io.WriteCloser
	target io.Writer
	hash   hash.Hash
}

func (n Writer) Close() error {
	return n.target.Close()
}

func (n Writer) Write(b []byte) (int, error) {
	return n.target.Write(b)
}

func (n Writer) Commit() (*Object, error) {
	return nil, nil
}

type Object struct {
	id string
}

func (o Object) Id() string {
	return o.id
}
