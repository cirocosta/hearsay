//go:build !unix

package main

import (
	"context"
	"errors"
)

func executeCommand(context.Context, string, string) (commandResult, error) {
	return commandResult{}, errors.New("this research example requires a Unix command runner")
}
