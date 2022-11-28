package main

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	const key = "create-key"
	const value = "create-value"

	var val interface{}
	var contains bool

  // reset store to prev state after the test
	defer delete(store, key)

	// check if store doesn't already contain the key
	_, contains = store[key]
	if contains {
		t.Error("Key/Value already exists")
	}

	// test err is nil while putting
	err := Put(key, value)
	if err != nil {
		t.Error(err)
	}

	// check if value is successfully created
	val, contains = store[key]
	if !contains {
		t.Error("Could not store value")
	}

	// check if value matches the key
	if val != value {
		t.Error("val/value mismatch")
	}
}

func TestGet(t *testing.T) {
	const key = "read-key"
	const value = "read-value"

	var val interface{}
	var err error

	defer delete(store, key)

	// Read a value which is not saved to generate error
	val, err = Get(key)
	if err == nil {
		t.Error("An error was expected")
	}
	if !errors.Is(err, ErrorNoSuchKey) {
		t.Error("Unexpected error:", err)
	}

	store[key] = value

	val, err = Get(key)
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	if val != value {
		t.Error("Value mismatch")
	}
}

func TestDelete(t *testing.T) {
	const key = "delete-key"
	const value = "delete-value"

	var contains bool

	defer delete(store, key)

	store[key] = value

	_, contains = store[key]
	if !contains {
		t.Error("key/value doesn't exist")
	}

	Delete(key)

	_, contains = store[key]
	if contains {
		t.Error("Delete failed")
	}
}