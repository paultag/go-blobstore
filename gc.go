package blobstore

type GarbageCollector interface {
	Find(s Store) ([]Object, error)
}

type DumbGarbageCollector struct{}

// Find {{{

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

// }}}

// vim: foldmethod=marker
