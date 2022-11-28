package main

import "errors"

var store = make(map[string]string)
// creating a sentinel error
var ErrorNoSuchKey = errors.New("No such key")

func Put(key string, value string) error {
  store[key] = value

  return nil
}

func Get(key string) (string, error) {
  value, ok := store[key]

  if !ok {
    return "", ErrorNoSuchKey
  }

  return value, nil
}

func Delete(key string) error {
  delete(store, key)

  return nil
}