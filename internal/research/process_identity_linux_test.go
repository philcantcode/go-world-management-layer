//go:build linux

package research

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestParseLinuxClockTicksAuxvSupportsNativeWordWidths(t *testing.T) {
	for _, wordBytes := range []int{4, 8} {
		t.Run(string(rune('0'+wordBytes))+"_byte_words", func(t *testing.T) {
			content := encodeLinuxAuxvForTest(wordBytes, binary.LittleEndian, [][2]uint64{
				{1, 4096}, {linuxAuxvClockTicks, 250}, {0, 0},
			})
			got, err := parseLinuxClockTicksAuxv(content, wordBytes, binary.LittleEndian)
			if err != nil || got != 250 {
				t.Fatalf("clock ticks = %d, %v; want 250", got, err)
			}
		})
	}
}

func TestParseLinuxClockTicksAuxvRejectsMissingInvalidAndTruncatedValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		records [][2]uint64
		trim    int
	}{
		{name: "missing", records: [][2]uint64{{1, 4096}, {0, 0}}},
		{name: "zero", records: [][2]uint64{{linuxAuxvClockTicks, 0}, {0, 0}}},
		{name: "truncated", records: [][2]uint64{{linuxAuxvClockTicks, 100}}, trim: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := encodeLinuxAuxvForTest(8, binary.LittleEndian, test.records)
			content = content[:len(content)-test.trim]
			if got, err := parseLinuxClockTicksAuxv(content, 8, binary.LittleEndian); err == nil || got != 0 {
				t.Fatalf("clock ticks = %d, %v; want a fail-closed error", got, err)
			}
		})
	}
}

func TestLinuxTicksToNanosecondsAvoidsIntermediateOverflow(t *testing.T) {
	const tenYearsSeconds = uint64(10 * 365 * 24 * 60 * 60)
	const frequency = int64(250)
	ticks := tenYearsSeconds*uint64(frequency) + uint64(frequency/2)
	got, err := linuxTicksToNanoseconds(ticks, frequency)
	want := int64(tenYearsSeconds)*1_000_000_000 + 500_000_000
	if err != nil || got != want {
		t.Fatalf("tick duration = %d, %v; want %d", got, err, want)
	}
}

func TestLinuxClockTicksReadsKernelAuxiliaryVector(t *testing.T) {
	clockTicks, err := linuxClockTicks()
	if err != nil || clockTicks <= 0 {
		t.Fatalf("kernel clock ticks = %d, %v", clockTicks, err)
	}
	startTicks, _, err := readLinuxProcStatIdentity(int64(os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	if startNS, err := linuxProcStartUnixNS(startTicks); err != nil || startNS <= 0 {
		t.Fatalf("current process start = %d, %v", startNS, err)
	}
}

func encodeLinuxAuxvForTest(wordBytes int, order binary.ByteOrder, records [][2]uint64) []byte {
	content := make([]byte, len(records)*wordBytes*2)
	for index, record := range records {
		offset := index * wordBytes * 2
		writeLinuxAuxvWordForTest(content[offset:offset+wordBytes], wordBytes, order, record[0])
		writeLinuxAuxvWordForTest(content[offset+wordBytes:offset+wordBytes*2], wordBytes, order, record[1])
	}
	return content
}

func writeLinuxAuxvWordForTest(content []byte, wordBytes int, order binary.ByteOrder, value uint64) {
	if wordBytes == 4 {
		order.PutUint32(content, uint32(value))
		return
	}
	order.PutUint64(content, value)
}
