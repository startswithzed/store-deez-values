package main

import (
	"errors"
	"sync"
)

// add a RW mutex to add thread safety
var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

// creating sentient error
var ErrorNoSuchKey = errors.New("no such key")

func Delete(key string) error {
  // establish a write lock 
	store.Lock()
  // delete key, value from the map
	delete(store.m, key)
  // release the lock
	store.Unlock()

	return nil
}

func Get(key string) (string, error) {
	// establish a read lock
  store.RLock()
  // get key, value from the map
	value, ok := store.m[key]
	// release the lock
  store.RUnlock()

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

func Put(key string, value string) error {
  // establish write lock 
	store.Lock()
  // store key value pair in the map
	store.m[key] = value
  // release the lock
	store.Unlock()

	return nil
}