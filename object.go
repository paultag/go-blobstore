package blobstore

type Object struct {
	id string
}

func (o Object) Id() string {
	return o.id
}

// vim: foldmethod=marker
