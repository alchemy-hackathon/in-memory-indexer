package main

import (
	"fmt"
	"time"
)

type RetryFunc func() error

// Retry will attempt to execute the provided function `fn` up to `maxRetries` times with a delay of `delay` between each attempt.
func Retry(fn RetryFunc, maxRetries int, delay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		if err = fn(); err == nil {
			return nil // Function succeeded, so we return nil (no error)
		}
		if i < maxRetries-1 { // No need to sleep after the last attempt
			time.Sleep(delay)
		}
	}
	return fmt.Errorf("Failed after %d attempts, last error: %s", maxRetries, err)
}
