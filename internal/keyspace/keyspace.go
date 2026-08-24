package keyspace

import (
	"strconv"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

// Space maps a caller's keys into one namespace.
type Space struct {
	prefix string
}

// New returns the Space for a namespace.
func New(namespace string) Space {
	return Space{prefix: "s:" + strconv.Itoa(len(namespace)) + ":" + namespace + ":"}
}

// Key returns the canonical identity for a caller's key.
func (s Space) Key(userKey string) backend.Key {
	k, err := backend.NewKey(s.prefix + userKey)
	if err != nil {
		// The prefix makes this unreachable; backends still reject the zero key.
		return backend.Key{}
	}
	return k
}
