package main

import (
	"errors"
	"sync"
)

// store is a map which supports concurrency
var store = struct {
  sync.RWMutex
  m map[string]string
} {m: make(map[string]string)}

// creating a sentinel error
var ErrorNoSuchKey = errors.New("No such key")

func Put(key string, value string) error {
  // establish a write lock
  store.Lock()
  // write the value
	store.m[key] = value
	// release the lock
  store.Unlock()
  
  return nil
}

func Get(key string) (string, error) {
  // establish read lock
  store.RLock()
  // read the value
  value, ok := store.m[key]
  // release the read lock
  store.RUnlock()
  
  if !ok {
    return "", ErrorNoSuchKey
  }

  return value, nil
}

func Delete(key string) error {
  store.Lock()
	delete(store.m, key)
	store.Unlock()

  return nil
}