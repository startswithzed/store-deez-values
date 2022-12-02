package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var transact *TransactionLogger

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func notAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not Allowed", http.StatusMethodNotAllowed)
}

// handles requests to add key value pairs to the database
// HTTP PUT request handler
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	// get key from path variable 
  vars := mux.Vars(r)
	key := vars["key"]

  // get value from the request body
	value, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

  // return status 500 if can't read value
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

  // add key value pair to the map
	err = Put(key, string(value))
	
  if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

  // write put event to the log
	transact.WritePut(key, string(value))

  // return status 201
	w.WriteHeader(http.StatusCreated)

	log.Printf("PUT key=%s value=%s\n", key, string(value))
}

// handles requests to get key value pairs from the database
// HTTP GET request handler
func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)

  // return status 404 if key not found
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

  // return status 500 if any other error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

  // write value to response
	w.Write([]byte(value))

	log.Printf("GET key=%s\n", key)
}

// handles requests to remove key value pairs from the database
// HTTP DELETE request handler
func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := Delete(key)
	
  if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

  // write del event to the log
	transact.WriteDelete(key)

	log.Printf("DELETE key=%s\n", key)
}

// initializes the transaction log and runs it
func initializeTransactionLog() error {
  var err error

	transact, err = NewTransactionLogger("transactions.log")
  
	if err != nil {
		return fmt.Errorf("failed to create transaction logger: %w", err)
	}

  // read events from the log 
	events, errors := transact.ReadEvents()
	
  count, ok, e := 0, true, Event{}

  // this loop runs until either channels are closed or there is an error
	for ok && err == nil {
		select {
		case err, ok = <-errors:

    // execute event based on event type   
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete: 
				err = Delete(e.Key)
				count++
			case EventPut: 
				err = Put(e.Key, e.Value)
				count++
			}
		}
	}

	log.Printf("%d events replayed\n", count)

  // run the logger
	transact.Run()

	return err
}

func main() {
	// blocks until all data is read.
	err := initializeTransactionLog()
	
  if err != nil {
		panic(err)
	}

	r := mux.NewRouter()

	r.Use(loggingMiddleware)

	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	r.HandleFunc("/v1", notAllowedHandler)
	r.HandleFunc("/v1/{key}", notAllowedHandler)

	log.Fatal(http.ListenAndServe(":8080", r))
}