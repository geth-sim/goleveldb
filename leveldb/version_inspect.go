package leveldb

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/golang/snappy"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// BlockStats: statistics for a single SST.
type BlockStats struct {
	Level         int
	FileNum       int64
	FileSize      int64
	DataBlocks    int
	ItemsPerBlock []int // number of entries in each block

	// Prefix-compression stats for data blocks.
	SharedPrefixSum          uint64 // sum(shared) over all entries
	SharedPrefixCount        int    // total number of entries scanned
	SharedPrefixNonZeroCount int    // number of entries with shared > 0
	MaxSharedPrefix          int    // max(shared)

	// Optional per-block stats for deeper analysis.
	SharedPrefixSumPerBlock   []uint64
	SharedPrefixCountPerBlock []int

	// Histogram of shared prefix lengths.
	// hist[x] = number of entries whose shared prefix length == x
	SharedPrefixHist []int64
}

// InspectAllSSTBlocks: scans all SSTs in the current version and
// - counts data blocks
// - counts items (entries) in each data block
// - collects shared/common key prefix length statistics
// then prints and returns the results.
func (v *version) InspectAllSSTBlocks(ro *opt.ReadOptions, maxTables int, maxBlocksPerTable int) ([]BlockStats, error) {
	_ = ro // reserved for future use

	var out []BlockStats
	seen := 0

	for level, tables := range v.levels {
		for _, t := range tables {
			if maxTables > 0 && seen >= maxTables {
				goto DONE
			}
			seen++

			st, err := v.inspectOneTable(level, t, maxBlocksPerTable)
			if err != nil {
				return out, fmt.Errorf("inspect table L%d #%d: %w", level, t.fd.Num, err)
			}
			out = append(out, st)

			fmt.Printf("[Inspect] L%d file=%d size=%dB dataBlocks=%d\n",
				st.Level, st.FileNum, st.FileSize, st.DataBlocks)

			if len(st.ItemsPerBlock) > 0 {
				minv, maxv, sum := st.ItemsPerBlock[0], st.ItemsPerBlock[0], 0
				for _, x := range st.ItemsPerBlock {
					if x < minv {
						minv = x
					}
					if x > maxv {
						maxv = x
					}
					sum += x
				}
				avg := float64(sum) / float64(len(st.ItemsPerBlock))
				fmt.Printf("          items/block: min=%d max=%d avg=%.2f (countedBlocks=%d)\n",
					minv, maxv, avg, len(st.ItemsPerBlock))
			}

			if st.SharedPrefixCount > 0 {
				avgSharedAll := float64(st.SharedPrefixSum) / float64(st.SharedPrefixCount)
				fmt.Printf("          shared-prefix/all-entry: sum=%d count=%d avg=%.4f\n",
					st.SharedPrefixSum, st.SharedPrefixCount, avgSharedAll)
			}

			if st.SharedPrefixNonZeroCount > 0 {
				avgSharedNZ := float64(st.SharedPrefixSum) / float64(st.SharedPrefixNonZeroCount)
				ratio := 100.0 * float64(st.SharedPrefixNonZeroCount) / float64(st.SharedPrefixCount)
				fmt.Printf("          shared-prefix/shared>0-entry avg=%.4f nonzero-ratio=%.2f%% max=%d\n",
					avgSharedNZ, ratio, st.MaxSharedPrefix)
			} else if st.SharedPrefixCount > 0 {
				fmt.Printf("          shared-prefix/shared>0-entry avg=0.0000 nonzero-ratio=0.00%% max=%d\n",
					st.MaxSharedPrefix)
			}

			// if st.SharedPrefixCount > 0 {
			// 	printSharedPrefixPercentilesFromHist(st.SharedPrefixHist, int64(st.SharedPrefixCount))
			// }
		}
	}

DONE:
	totalTables := len(out)
	totalBlocks := 0
	totalEntries := 0
	minItems := int(^uint(0) >> 1)
	maxItems := 0

	totalSharedPrefixSum := uint64(0)
	totalSharedPrefixCount := 0
	totalSharedPrefixNonZeroCount := 0
	globalMaxSharedPrefix := 0
	var totalSharedPrefixHist []int64

	for _, st := range out {
		for _, x := range st.ItemsPerBlock {
			totalEntries += x
			totalBlocks++
			if x < minItems {
				minItems = x
			}
			if x > maxItems {
				maxItems = x
			}
		}

		totalSharedPrefixSum += st.SharedPrefixSum
		totalSharedPrefixCount += st.SharedPrefixCount
		totalSharedPrefixNonZeroCount += st.SharedPrefixNonZeroCount
		if st.MaxSharedPrefix > globalMaxSharedPrefix {
			globalMaxSharedPrefix = st.MaxSharedPrefix
		}
		totalSharedPrefixHist = mergeInt64Hist(totalSharedPrefixHist, st.SharedPrefixHist)
	}

	if totalBlocks > 0 {
		avg := float64(totalEntries) / float64(totalBlocks)

		fmt.Printf("\n====== FINAL SUMMARY ======\n")
		fmt.Printf("tables=%d\n", totalTables)
		fmt.Printf("blocks=%d\n", totalBlocks)
		fmt.Printf("entries=%d\n", totalEntries)
		fmt.Printf("items/block min=%d max=%d avg=%.2f\n",
			minItems, maxItems, avg)

		if totalSharedPrefixCount > 0 {
			avgSharedAll := float64(totalSharedPrefixSum) / float64(totalSharedPrefixCount)
			fmt.Printf("shared-prefix/all-entry: sum=%d count=%d avg=%.4f\n",
				totalSharedPrefixSum, totalSharedPrefixCount, avgSharedAll)
		}

		if totalSharedPrefixNonZeroCount > 0 {
			avgSharedNonZero := float64(totalSharedPrefixSum) / float64(totalSharedPrefixNonZeroCount)
			fmt.Printf("shared-prefix/shared>0-entry: sum=%d count=%d avg=%.4f\n",
				totalSharedPrefixSum, totalSharedPrefixNonZeroCount, avgSharedNonZero)
		} else {
			fmt.Printf("shared-prefix/shared>0-entry: count=0 avg=0\n")
		}

		if totalSharedPrefixCount > 0 {
			ratio := 100.0 * float64(totalSharedPrefixNonZeroCount) / float64(totalSharedPrefixCount)
			fmt.Printf("shared-prefix nonzero ratio=%.2f%% max=%d\n",
				ratio, globalMaxSharedPrefix)
		}

		hist := make([]int64, maxItems+1)
		for _, st := range out {
			for _, x := range st.ItemsPerBlock {
				hist[x]++
			}
		}

		printItemsPerBlockPercentilesFromHist(hist, int64(totalBlocks))

		if totalSharedPrefixCount > 0 {
			printSharedPrefixPercentilesFromHist(totalSharedPrefixHist, int64(totalSharedPrefixCount))
		}
	}

	return out, nil
}

// InspectOverlappingSSTBlocks: scans only the SSTs overlapping a specific ikey.
// This is safer for brief inspection on the get() hot path.
func (v *version) InspectOverlappingSSTBlocks(aux tFiles, ikey internalKey, ro *opt.ReadOptions, maxTables int, maxBlocksPerTable int) ([]BlockStats, error) {
	_ = ro

	type target struct {
		level int
		t     *tFile
	}
	var targets []target

	v.walkOverlapping(aux, ikey,
		func(level int, t *tFile) bool {
			targets = append(targets, target{level: level, t: t})
			if maxTables > 0 && len(targets) >= maxTables {
				return false
			}
			return true
		},
		nil,
	)

	sort.Slice(targets, func(i, j int) bool {
		if targets[i].level != targets[j].level {
			return targets[i].level < targets[j].level
		}
		return targets[i].t.fd.Num < targets[j].t.fd.Num
	})

	var out []BlockStats
	for _, tg := range targets {
		st, err := v.inspectOneTable(tg.level, tg.t, maxBlocksPerTable)
		if err != nil {
			return out, fmt.Errorf("inspect table L%d #%d: %w", tg.level, tg.t.fd.Num, err)
		}
		out = append(out, st)

		fmt.Printf("[InspectOverlap] L%d file=%d size=%dB dataBlocks=%d\n",
			st.Level, st.FileNum, st.FileSize, st.DataBlocks)

		if st.SharedPrefixCount > 0 {
			avgSharedAll := float64(st.SharedPrefixSum) / float64(st.SharedPrefixCount)
			fmt.Printf("                 shared-prefix/all-entry: sum=%d count=%d avg=%.4f\n",
				st.SharedPrefixSum, st.SharedPrefixCount, avgSharedAll)
		}

		if st.SharedPrefixNonZeroCount > 0 {
			avgSharedNZ := float64(st.SharedPrefixSum) / float64(st.SharedPrefixNonZeroCount)
			ratio := 100.0 * float64(st.SharedPrefixNonZeroCount) / float64(st.SharedPrefixCount)
			fmt.Printf("                 shared-prefix/shared>0-entry avg=%.4f nonzero-ratio=%.2f%% max=%d\n",
				avgSharedNZ, ratio, st.MaxSharedPrefix)
		}

		// if st.SharedPrefixCount > 0 {
		// 	printSharedPrefixPercentilesFromHist(st.SharedPrefixHist, int64(st.SharedPrefixCount))
		// }
	}
	return out, nil
}

// ---- SST parsing (LevelDB table format) -----------------------------------

const sstMagic uint64 = 0xdb4775248b80fb57

const (
	noCompression     = 0
	snappyCompression = 1
)

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

// inspectOneTable: opens a single tFile (SST), parses index block -> data blocks,
// and computes entry counts and shared prefix statistics.
func (v *version) inspectOneTable(level int, t *tFile, maxBlocksPerTable int) (BlockStats, error) {
	f, err := v.s.stor.Open(t.fd)
	if err != nil {
		return BlockStats{}, err
	}
	defer f.Close()

	r, ok := f.(readerAtCloser)
	if !ok {
		return BlockStats{}, fmt.Errorf("storage reader doesn't implement ReaderAt+Closer")
	}

	fileSize := t.size

	idxOff, idxSize, err := readFooterIndexHandle(r, fileSize)
	if err != nil {
		return BlockStats{}, err
	}

	indexBlock, err := readBlock(r, idxOff, idxSize)
	if err != nil {
		return BlockStats{}, err
	}
	dataHandles, err := parseIndexBlockDataHandles(indexBlock)
	if err != nil {
		return BlockStats{}, err
	}

	itemsPerBlock := make([]int, 0, len(dataHandles))
	sharedPrefixSumPerBlock := make([]uint64, 0, len(dataHandles))
	sharedPrefixCountPerBlock := make([]int, 0, len(dataHandles))

	limit := len(dataHandles)
	if maxBlocksPerTable > 0 && limit > maxBlocksPerTable {
		limit = maxBlocksPerTable
	}

	var totalSharedSum uint64
	var totalSharedCount int
	var totalSharedNonZeroCount int
	maxSharedPrefix := 0
	var sharedPrefixHist []int64

	for i := 0; i < limit; i++ {
		h := dataHandles[i]
		dataBlock, err := readBlock(r, h.off, h.size)
		if err != nil {
			return BlockStats{}, err
		}

		n, sharedSum, sharedNonZeroCount, blockMaxShared, blockHist, err := inspectDataBlock(dataBlock)
		if err != nil {
			return BlockStats{}, err
		}

		itemsPerBlock = append(itemsPerBlock, n)
		sharedPrefixSumPerBlock = append(sharedPrefixSumPerBlock, sharedSum)
		sharedPrefixCountPerBlock = append(sharedPrefixCountPerBlock, n)

		totalSharedSum += sharedSum
		totalSharedCount += n
		totalSharedNonZeroCount += sharedNonZeroCount
		if blockMaxShared > maxSharedPrefix {
			maxSharedPrefix = blockMaxShared
		}
		sharedPrefixHist = mergeInt64Hist(sharedPrefixHist, blockHist)
	}

	return BlockStats{
		Level:                     level,
		FileNum:                   t.fd.Num,
		FileSize:                  fileSize,
		DataBlocks:                len(dataHandles),
		ItemsPerBlock:             itemsPerBlock,
		SharedPrefixSum:           totalSharedSum,
		SharedPrefixCount:         totalSharedCount,
		SharedPrefixNonZeroCount:  totalSharedNonZeroCount,
		MaxSharedPrefix:           maxSharedPrefix,
		SharedPrefixSumPerBlock:   sharedPrefixSumPerBlock,
		SharedPrefixCountPerBlock: sharedPrefixCountPerBlock,
		SharedPrefixHist:          sharedPrefixHist,
	}, nil
}

type blockHandle struct {
	off  uint64
	size uint64
}

func readFooterIndexHandle(r io.ReaderAt, fileSize int64) (idxOff uint64, idxSize uint64, err error) {
	// The footer is 48 bytes: [metaindex BH: 20][index BH: 20][magic: 8]
	if fileSize < 48 {
		return 0, 0, fmt.Errorf("file too small for footer: %d", fileSize)
	}
	footer := make([]byte, 48)
	_, err = r.ReadAt(footer, fileSize-48)
	if err != nil {
		return 0, 0, err
	}

	magic := binary.LittleEndian.Uint64(footer[40:])
	if magic != sstMagic {
		return 0, 0, fmt.Errorf("bad sst magic: got=%x want=%x", magic, sstMagic)
	}

	p := 0
	_, _, n1, err := decodeBlockHandle(footer[p:20])
	if err != nil {
		return 0, 0, fmt.Errorf("decode metaindex handle: %w", err)
	}
	p += n1

	off, size, _, err := decodeBlockHandle(footer[p:40])
	if err != nil {
		return 0, 0, fmt.Errorf("decode index handle: %w", err)
	}
	return off, size, nil
}

func readBlock(r io.ReaderAt, off uint64, size uint64) ([]byte, error) {
	// A block is laid out as [data(size)][trailer(5)]:
	// trailer: [compressionType(1)][crc32(4)]
	if size == 0 {
		return nil, fmt.Errorf("block size=0")
	}
	buf := make([]byte, int(size)+5)
	_, err := r.ReadAt(buf, int64(off))
	if err != nil {
		return nil, err
	}

	data := buf[:size]
	ctype := buf[size]

	switch ctype {
	case noCompression:
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	case snappyCompression:
		decoded, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("snappy decode: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unknown compression type: %d", ctype)
	}
}

func decodeBlockHandle(b []byte) (off uint64, size uint64, n int, err error) {
	off, n1 := binary.Uvarint(b)
	if n1 <= 0 {
		return 0, 0, 0, fmt.Errorf("bad varint offset")
	}
	size, n2 := binary.Uvarint(b[n1:])
	if n2 <= 0 {
		return 0, 0, 0, fmt.Errorf("bad varint size")
	}
	return off, size, n1 + n2, nil
}

// parseIndexBlockDataHandles:
// The index block also uses the normal block format (prefix-compressed entries),
// and each entry value contains a data block handle (varint offset+size).
func parseIndexBlockDataHandles(block []byte) ([]blockHandle, error) {
	entriesEnd, err := blockEntriesEnd(block)
	if err != nil {
		return nil, err
	}

	var handles []blockHandle
	pos := 0

	for pos < entriesEnd {
		_, n1 := binary.Uvarint(block[pos:])
		if n1 <= 0 {
			return nil, fmt.Errorf("bad shared varint at %d", pos)
		}
		pos += n1

		nonShared, n2 := binary.Uvarint(block[pos:])
		if n2 <= 0 {
			return nil, fmt.Errorf("bad nonShared varint at %d", pos)
		}
		pos += n2

		valLen, n3 := binary.Uvarint(block[pos:])
		if n3 <= 0 {
			return nil, fmt.Errorf("bad valLen varint at %d", pos)
		}
		pos += n3

		if pos+int(nonShared) > entriesEnd {
			return nil, fmt.Errorf("key overrun at %d", pos)
		}
		pos += int(nonShared)

		if pos+int(valLen) > entriesEnd {
			return nil, fmt.Errorf("value overrun at %d", pos)
		}
		val := block[pos : pos+int(valLen)]
		pos += int(valLen)

		off, size, _, err := decodeBlockHandle(val)
		if err != nil {
			return nil, fmt.Errorf("decode data block handle: %w", err)
		}
		handles = append(handles, blockHandle{off: off, size: size})
	}
	return handles, nil
}

// inspectDataBlock:
// directly scans a data block to compute entry counts and shared prefix statistics.
func inspectDataBlock(block []byte) (count int, sharedSum uint64, sharedNonZeroCount int, maxShared int, hist []int64, err error) {
	entriesEnd, err := blockEntriesEnd(block)
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}

	pos := 0
	cnt := 0
	var sum uint64
	nonZero := 0
	maxv := 0
	localHist := make([]int64, 0, 16)

	for pos < entriesEnd {
		shared, n1 := binary.Uvarint(block[pos:])
		if n1 <= 0 {
			return 0, 0, 0, 0, nil, fmt.Errorf("bad shared varint at %d", pos)
		}
		pos += n1

		nonShared, n2 := binary.Uvarint(block[pos:])
		if n2 <= 0 {
			return 0, 0, 0, 0, nil, fmt.Errorf("bad nonShared varint at %d", pos)
		}
		pos += n2

		valLen, n3 := binary.Uvarint(block[pos:])
		if n3 <= 0 {
			return 0, 0, 0, 0, nil, fmt.Errorf("bad valLen varint at %d", pos)
		}
		pos += n3

		skip := int(nonShared) + int(valLen)
		if pos+skip > entriesEnd {
			return 0, 0, 0, 0, nil, fmt.Errorf("entry overrun at %d", pos)
		}
		pos += skip

		cnt++
		sum += shared
		if shared > 0 {
			nonZero++
		}
		if int(shared) > maxv {
			maxv = int(shared)
		}

		s := int(shared)
		if s >= len(localHist) {
			newHist := make([]int64, s+1)
			copy(newHist, localHist)
			localHist = newHist
		}
		localHist[s]++
	}

	return cnt, sum, nonZero, maxv, localHist, nil
}

// blockEntriesEnd: computes the end offset of the entries area from the restart array at the end of the block.
func blockEntriesEnd(block []byte) (int, error) {
	if len(block) < 4 {
		return 0, fmt.Errorf("block too small")
	}
	nRestarts := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	restartsBytes := nRestarts * 4
	entriesEnd := len(block) - 4 - restartsBytes
	if entriesEnd < 0 || entriesEnd > len(block) {
		return 0, fmt.Errorf("bad restart array: n=%d len=%d", nRestarts, len(block))
	}
	return entriesEnd, nil
}

func mergeInt64Hist(dst []int64, src []int64) []int64 {
	if len(src) == 0 {
		return dst
	}
	if len(dst) < len(src) {
		newDst := make([]int64, len(src))
		copy(newDst, dst)
		dst = newDst
	}
	for i, v := range src {
		dst[i] += v
	}
	return dst
}

func percentileValueSortedAsc(sorted []int, p float64) int {
	// p in [0, 100]
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}

	// Simple nearest-rank style calculation.
	rank := int((p*float64(n) + 99.999999) / 100.0) // effectively ceil
	idx := rank - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

func printItemsPerBlockPercentiles(all []int) {
	if len(all) == 0 {
		return
	}

	sorted := append([]int(nil), all...)
	sort.Ints(sorted)

	fmt.Printf("\nitems/block percentiles (5%% step)\n")
	fmt.Printf("---------------------------------\n")

	for top := 5; top <= 95; top += 5 {
		p := float64(100 - top)
		v := percentileValueSortedAsc(sorted, p)
		fmt.Printf("top %2d%% cutoff (P%02d) = %d\n", top, int(p), v)
	}

	fmt.Printf("\nraw percentiles\n")
	for p := 0; p <= 100; p += 5 {
		v := percentileValueSortedAsc(sorted, float64(p))
		fmt.Printf("P%02d = %d\n", p, v)
	}
}

func printItemsPerBlockPercentilesFromHist(hist []int64, total int64) {
	if total == 0 {
		return
	}

	fmt.Printf("\nitems/block percentiles (5%% step)\n")
	fmt.Printf("---------------------------------\n")

	for top := 5; top <= 95; top += 5 {
		p := 100 - top
		v := percentileValueFromHist(hist, total, float64(p))
		fmt.Printf("top %2d%% cutoff (P%02d) = %d\n", top, p, v)
	}

	fmt.Printf("\nraw percentiles\n")
	for p := 0; p <= 100; p += 5 {
		v := percentileValueFromHist(hist, total, float64(p))
		fmt.Printf("P%02d = %d\n", p, v)
	}
}

func printSharedPrefixPercentilesFromHist(hist []int64, total int64) {
	if total == 0 {
		return
	}

	fmt.Printf("\nshared-prefix length percentiles (5%% step)\n")
	fmt.Printf("------------------------------------------\n")

	for top := 5; top <= 95; top += 5 {
		p := 100 - top
		v := percentileValueFromHist(hist, total, float64(p))
		fmt.Printf("top %2d%% cutoff (P%02d) = %d\n", top, p, v)
	}

	fmt.Printf("\nraw percentiles\n")
	for p := 0; p <= 100; p += 5 {
		v := percentileValueFromHist(hist, total, float64(p))
		fmt.Printf("P%02d = %d\n", p, v)
	}
}

func percentileValueFromHist(hist []int64, total int64, p float64) int {
	if total == 0 {
		return 0
	}
	if p <= 0 {
		for i := 0; i < len(hist); i++ {
			if hist[i] > 0 {
				return i
			}
		}
		return 0
	}
	if p >= 100 {
		for i := len(hist) - 1; i >= 0; i-- {
			if hist[i] > 0 {
				return i
			}
		}
		return 0
	}

	// nearest-rank
	target := int64((p*float64(total) + 99.999999) / 100.0)
	if target < 1 {
		target = 1
	}

	var cum int64
	for i := 0; i < len(hist); i++ {
		cum += hist[i]
		if cum >= target {
			return i
		}
	}
	return len(hist) - 1
}
