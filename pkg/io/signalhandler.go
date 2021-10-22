package io

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gojek/kat/logger"
)

type SignalHandler struct {
	wg sync.WaitGroup
}

//notifies the context about an external signal that has been notified by cancelling the context.
func (s SignalHandler) SetListener(ctx context.Context, cancelFunc context.CancelFunc, syscallSignal syscall.Signal) {
	cancelChan := make(chan os.Signal, 1)
	s.wg.Add(1)
	// catch SIGETRM
	signal.Notify(cancelChan, syscallSignal)
	go func() {
		select {
		case <-ctx.Done():
		case <-cancelChan:
			logger.Info("Interrupt has been received, will stop reassignment after current batch")
			cancelFunc()

		}
		s.wg.Done()
	}()
}

// ensures all the set up listeners have completed execution.
func (s SignalHandler) Close() {
	s.wg.Wait()
}