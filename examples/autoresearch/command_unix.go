//go:build unix

package main

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"
)

func executeCommand(ctx context.Context, directory, command string) (commandResult, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir, cmd.Env = directory, commandEnvironment(directory)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var output limitedOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return commandResult{}, err
	}
	err := cmd.Wait()
	// end any background descendants before releasing the shared command lane.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	result := commandResult{
		Exit: cmd.ProcessState.ExitCode(), Output: string(output.data),
		Truncated: output.truncated, Elapsed: time.Since(start).String(),
	}
	if err != nil && !errors.As(err, new(*exec.ExitError)) {
		return result, err
	}
	if ctx.Err() != nil {
		result.Output += "\ncommand deadline/cancellation: " + ctx.Err().Error()
	}
	return result, nil
}
