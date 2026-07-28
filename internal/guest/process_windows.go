//go:build windows

package guest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func prepareCommand(command *exec.Cmd) error {
	command.SysProcAttr = &windows.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
	return nil
}

func ownStartedProcess(command *exec.Cmd) (processOwner, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	information := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	information.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&information)), uint32(unsafe.Sizeof(information))); err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	handle, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(command.Process.Pid))
	if err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	defer windows.CloseHandle(handle)
	if err = windows.AssignProcessToJobObject(job, handle); err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	return &windowsJob{handle: job, process: command.Process}, nil
}

type windowsJob struct {
	handle  windows.Handle
	process *os.Process
}

type jobBasicAccountingInformation struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func (job *windowsJob) Signal(name string) error {
	name = strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(name)), "SIG")
	if name == "INT" {
		return job.process.Signal(os.Interrupt)
	}
	if name == "TERM" || name == "KILL" {
		return job.Kill()
	}
	return fmt.Errorf("unsupported signal %q", name)
}
func (job *windowsJob) Terminate() error { return job.Kill() }
func (job *windowsJob) Kill() error      { return windows.TerminateJobObject(job.handle, 1) }
func (job *windowsJob) ConfirmCleanup(ctx context.Context) (bool, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var information jobBasicAccountingInformation
		err := windows.QueryInformationJobObject(job.handle, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&information)), uint32(unsafe.Sizeof(information)), nil)
		if err != nil {
			if errors.Is(err, windows.ERROR_INVALID_HANDLE) {
				return true, nil
			}
			return false, err
		}
		if information.ActiveProcesses == 0 {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
		}
	}
}
func (job *windowsJob) Close() error { return windows.CloseHandle(job.handle) }

func processExitSignal(*exec.ExitError) string { return "" }
