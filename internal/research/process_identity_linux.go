//go:build linux

package research

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// processStartNSTolerance is the max allowed skew between lifecycle ProcessStartNS
// (wall clock) and the start time reconstructed from /proc (btime + jiffies).
const processStartNSTolerance = 3 * time.Second

const (
	linuxAuxvClockTicks   = uint64(17) // AT_CLKTCK from linux/auxvec.h.
	maximumLinuxAuxvBytes = int64(64 << 10)
)

func processExists(pid int64) (bool, error) {
	_, err := os.Stat(filepath.Join("/proc", strconv.FormatInt(pid, 10)))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func verifyProcessIdentityPlatform(ctx context.Context, pid, parentPID, processStartNS int64) processIdentityStatus {
	if err := ctx.Err(); err != nil {
		return processIdentityStatus{Reason: ReasonHostCaptureFailed}
	}
	procDir := filepath.Join("/proc", strconv.FormatInt(pid, 10))
	if _, err := os.Stat(procDir); err != nil {
		return processIdentityStatus{Alive: false, Verified: true, Reason: "process_exited_before_capture"}
	}
	startTicks, ppid, err := readLinuxProcStatIdentity(pid)
	if err != nil {
		return processIdentityStatus{Alive: true, Verified: false, Reason: ReasonProcessIdentityMismatch}
	}
	if parentPID > 0 && ppid > 0 && ppid != parentPID {
		return processIdentityStatus{Alive: true, Verified: false, Reason: ReasonProcessIdentityMismatch, KernelStartTime: startTicks}
	}
	// Bind lifecycle ProcessStartNS (wall clock) to /proc starttime via btime+jiffies.
	procStartNS, convErr := linuxProcStartUnixNS(startTicks)
	if convErr != nil {
		// Conversion failure: refuse live attach rather than discard ProcessStartNS.
		return processIdentityStatus{Alive: true, Verified: false, Reason: ReasonProcessIdentityMismatch, KernelStartTime: startTicks}
	}
	delta := math.Abs(float64(procStartNS - processStartNS))
	if delta > float64(processStartNSTolerance.Nanoseconds()) {
		return processIdentityStatus{Alive: true, Verified: false, Reason: ReasonProcessIdentityMismatch, KernelStartTime: startTicks}
	}
	return processIdentityStatus{Alive: true, Verified: true, KernelStartTime: startTicks}
}

func readLinuxProcStatIdentity(pid int64) (startTicks string, ppid int64, err error) {
	content, err := os.ReadFile(filepath.Join("/proc", strconv.FormatInt(pid, 10), "stat"))
	if err != nil {
		return "", 0, err
	}
	text := string(content)
	closeParen := strings.LastIndex(text, ")")
	if closeParen < 0 || closeParen+2 >= len(text) {
		return "", 0, os.ErrInvalid
	}
	fields := strings.Fields(text[closeParen+2:])
	// fields[0]=state, [1]=ppid, ... starttime is index 19 (stat field 22).
	if len(fields) < 20 {
		return "", 0, os.ErrInvalid
	}
	ppid, err = strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return "", 0, err
	}
	return fields[19], ppid, nil
}

func linuxProcStartUnixNS(startTicks string) (int64, error) {
	ticks, err := strconv.ParseUint(startTicks, 10, 64)
	if err != nil {
		return 0, err
	}
	btime, err := linuxBootTimeSeconds()
	if err != nil {
		return 0, err
	}
	clk, err := linuxClockTicks()
	if err != nil {
		return 0, err
	}
	elapsedNS, err := linuxTicksToNanoseconds(ticks, clk)
	if err != nil {
		return 0, err
	}
	if btime < 0 || btime > math.MaxInt64/int64(time.Second) {
		return 0, fmt.Errorf("Linux boot time %d is outside the representable Unix nanosecond range", btime)
	}
	bootNS := btime * int64(time.Second)
	if elapsedNS > math.MaxInt64-bootNS {
		return 0, fmt.Errorf("Linux process start time overflows Unix nanoseconds")
	}
	// start_unix_ns = btime*1e9 + ticks * (1e9 / clk_tck)
	return bootNS + elapsedNS, nil
}

func linuxBootTimeSeconds() (int64, error) {
	content, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(content), "\n") {
		if strings.HasPrefix(line, "btime ") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0, os.ErrInvalid
			}
			return strconv.ParseInt(fields[1], 10, 64)
		}
	}
	return 0, os.ErrNotExist
}

// linuxClockTicks reads AT_CLKTCK from the kernel-provided ELF auxiliary
// vector. Go intentionally does not expose sysconf(_SC_CLK_TCK), and assuming
// USER_HZ=100 would turn a platform-dependent value into a false PID identity.
func linuxClockTicks() (int64, error) {
	file, err := os.Open("/proc/self/auxv")
	if err != nil {
		return 0, fmt.Errorf("open /proc/self/auxv: %w", err)
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, maximumLinuxAuxvBytes+1))
	if err != nil {
		return 0, fmt.Errorf("read /proc/self/auxv: %w", err)
	}
	if int64(len(content)) > maximumLinuxAuxvBytes {
		return 0, fmt.Errorf("/proc/self/auxv exceeds the %d-byte safety bound", maximumLinuxAuxvBytes)
	}
	return parseLinuxClockTicksAuxv(content, strconv.IntSize/8, binary.NativeEndian)
}

func parseLinuxClockTicksAuxv(content []byte, wordBytes int, order binary.ByteOrder) (int64, error) {
	if wordBytes != 4 && wordBytes != 8 {
		return 0, fmt.Errorf("unsupported Linux auxiliary-vector word size %d", wordBytes)
	}
	recordBytes := wordBytes * 2
	if len(content) == 0 || len(content)%recordBytes != 0 {
		return 0, fmt.Errorf("Linux auxiliary vector has a truncated record")
	}
	for offset := 0; offset < len(content); offset += recordBytes {
		key := readLinuxAuxvWord(content[offset:offset+wordBytes], wordBytes, order)
		value := readLinuxAuxvWord(content[offset+wordBytes:offset+recordBytes], wordBytes, order)
		if key == 0 { // AT_NULL
			break
		}
		if key != linuxAuxvClockTicks {
			continue
		}
		if value == 0 || value > math.MaxInt64 {
			return 0, fmt.Errorf("Linux AT_CLKTCK value %d is invalid", value)
		}
		return int64(value), nil
	}
	return 0, fmt.Errorf("Linux auxiliary vector does not report AT_CLKTCK")
}

func readLinuxAuxvWord(content []byte, wordBytes int, order binary.ByteOrder) uint64 {
	if wordBytes == 4 {
		return uint64(order.Uint32(content))
	}
	return order.Uint64(content)
}

func linuxTicksToNanoseconds(ticks uint64, clockTicks int64) (int64, error) {
	if clockTicks <= 0 {
		return 0, fmt.Errorf("Linux clock-tick frequency %d is invalid", clockTicks)
	}
	frequency := uint64(clockTicks)
	seconds := ticks / frequency
	remainder := ticks % frequency
	maxSeconds := uint64(math.MaxInt64 / int64(time.Second))
	if seconds > maxSeconds {
		return 0, fmt.Errorf("Linux process start ticks overflow nanoseconds")
	}
	high, low := bits.Mul64(remainder, uint64(time.Second))
	fractionalNS, _ := bits.Div64(high, low, frequency)
	return int64(seconds)*int64(time.Second) + int64(fractionalNS), nil
}
