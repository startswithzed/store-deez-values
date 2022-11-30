package main

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"sync"
)

type EventType byte

// creating enums for event types
const (
  _ = iota
  EventDelete EventType = iota 
	EventPut  
)

type Event struct {
  Sequence  uint64
  EventType EventType
  Key       string
  Value     string
}

type TransactionLogger struct {
  // write only channel for sending events
  events       chan<- Event
  errors       <-chan error
  lastSequence uint64
  file         *os.File
  wg           *sync.WaitGroup
}

func (l *TransactionLogger) WritePut(key, value string) {
  l.wg.Add(1)
  l.events <- Event{EventType: EventPut, Key: key, Value: url.QueryEscape(value)}
}

func (l *TransactionLogger) WriteDelete(key string) {
  l.wg.Add(1)
  l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *TransactionLogger) Err() <-chan error {
  return l.errors
}

func NewTransactionLogger(filename string) (*TransactionLogger, error) {
  var err error
  // initialize the transaction logger with a wait group
  var l TransactionLogger = TransactionLogger{wg: &sync.WaitGroup{}}

  // opens the file by accepting flags
  // O_RDWR: open in read/write mode
  // O_APPEND: any writes to this file will append not overwrite
  // O_CREATE: create the file if it does not exist
  l.file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)

  if err != nil {
    return nil, fmt.Errorf("Cannot open transaction log file: %w", err) 
  }

  return &l, nil
}

// writes new events in the log
func (l *TransactionLogger) Run() {
  events := make(chan Event, 16)
  l.events = events

  errors := make(chan error, 1)
	l.errors = errors

  go func() {
    // loop will block until events channel is closed
    for e := range events {
      l.lastSequence++

      _, err := fmt.Fprintf(
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)

      if err != nil {
				errors <- fmt.Errorf("cannot write to log file: %w", err)
			}

      // wait for this channel to close
      l.wg.Done()
    }
  }()
}

func (l *TransactionLogger) Wait() {
  l.wg.Wait()
}

func (l *TransactionLogger) Close() error {
  l.wg.Wait()

  // close the channel and the file
  if l.events != nil {
    close(l.events)
  }

  return l.file.Close()
}

func (l *TransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
  scanner := bufio.NewScanner(l.file)
  outEvent := make(chan Event)
  outError := make(chan error, 1)

  go func() {
    var e Event

    defer close(outEvent)
    defer close(outError)

    for scanner.Scan() {
      line := scanner.Text()

      fmt.Sscanf(
				line, "%d\t%d\t%s\t%s",
				&e.Sequence, &e.EventType, &e.Key, &e.Value)

      // check if events are in sequence
      if l.lastSequence >= e.Sequence {
        outError <- fmt.Errorf("Transaction numbers out of sequence")
				return
      }

      // unecode the value
      uv, err := url.QueryUnescape(e.Value)

      if err != nil {
				outError <- fmt.Errorf("Value decoding failure: %w", err)
				return
			}

      e.Value = uv
      // update the last sequence of logger
      l.lastSequence = e.Sequence

      outEvent <- e
    }

    if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
  }()

  return outEvent, outError
}