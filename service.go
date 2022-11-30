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

// add a middleware function to log
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func notAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not Allowed", http.StatusMethodNotAllowed)
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	// get the key from path variable
	vars := mux.Vars(r)
	key := vars["key"]

	// read value from request body
	value, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// add operation to transaction log
	transact.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)

	log.Printf("PUT key=%s value=%s\n", key, string(value))
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)

	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))

	log.Printf("GET key=%s\n", key)
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := Delete(key)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transact.WriteDelete(key)

	log.Printf("DELETE key=%s\n", key)
}

func initializeTransactionLog() error {
	var err error

	transact, err := NewTransactionLogger("/tmp/transactions.log")

	if err != nil {
		return fmt.Errorf("failed to create transaction logger: %w", err)
	}

	events, errors := transact.ReadEvents()

	count, ok, e := 0, true, Event{}

  // bool returned by the channel will be false if the channel has been closed
  // if ok is set false by either channel the loop will terminate
	for ok && err == nil {
		select {
		case err, ok = <-errors:
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

	transact.Run()

	return err
}

func main() {
  // initializes transaction log and reads existing data if any
  // blocks until all data is read
  err := initializeTransactionLog()
	
  if err != nil {
		panic(err)
	}
  
	r := mux.NewRouter()

	// use middleware on the router
	r.Use(loggingMiddleware)

	// add handlers
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	// add handlers for other non-handled routes
	r.HandleFunc("/v1", notAllowedHandler)
	r.HandleFunc("/v1/{key}", notAllowedHandler)

	log.Fatal(http.ListenAndServe(":8080", r))
}
