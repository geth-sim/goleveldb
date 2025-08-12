package common

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

var (
	MyReadStats = NewReadStats()
)

type ReadStats struct {
	mu sync.Mutex

	ReadRequestCount int64

	// SSTable read counts
	FakeMems   map[string]int64 // e.g., "mem", "imm"
	FakeLevels map[int]int64    // e.g., 0, 1, 2, ...
	RealMems   map[string]int64 // e.g., "mem", "imm"
	RealLevels map[int]int64    // e.g., 0, 1, 2, ...

	FakeLevel0Attempts int64 // Number of Level 0 search requests that ended up all fake (The value of FakeLevels[0] is usually greater than FakeLevel0Attempts, since key overlapping between tables in L0 can cause multiple tables to be searched per request)
	RealLevel0Attempts int64 // Number of Level 0 search requests where the key was actually found (The value of RealLevels[0] is usually greater than RealLevel0Attempts, for the same reason—overlapping tables in L0 can cause multiple tables to be searched per request)
	NotFoundCount      int64 // This doesn't happen for trie nodes, but at the very start of the simulation, there are a few (e.g., 3) unknown read requests that result in NotFound

	// for all data (TODO(jmlee): deprecate this, use CacheHitCounts, CacheMissCounts)
	CacheHitCount  int64
	CacheMissCount int64
	// cache hit/miss counts
	CacheHitCounts  map[string]int64
	CacheMissCounts map[string]int64

	// bloom filter hit/miss counts
	BloomHitCount           int64
	BloomMissCount          int64
	BloomFalsePositiveCount int64
}

func NewReadStats() *ReadStats {
	return &ReadStats{
		FakeMems:        make(map[string]int64),
		FakeLevels:      make(map[int]int64),
		RealMems:        make(map[string]int64),
		RealLevels:      make(map[int]int64),
		CacheHitCounts:  make(map[string]int64),
		CacheMissCounts: make(map[string]int64),
	}
}

func (rs *ReadStats) AddReadRequest() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.ReadRequestCount++
}

func (rs *ReadStats) AddFakeMem(source string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.FakeMems[source]++
}

func (rs *ReadStats) AddFakeLevel(level int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.FakeLevels[level]++
}

func (rs *ReadStats) AddFakeLevel0(cnt int64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.FakeLevels[0] += cnt
}

func (rs *ReadStats) AddFakeLevel0Attempt() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.FakeLevel0Attempts++
}

func (rs *ReadStats) AddRealMem(source string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.RealMems[source]++
}

func (rs *ReadStats) AddRealLevel(level int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.RealLevels[level]++
}

func (rs *ReadStats) AddRealLevel0(cnt int64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.RealLevels[0] += cnt
}

func (rs *ReadStats) AddRealLevel0Attempt() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.RealLevel0Attempts++
}

func (rs *ReadStats) AddNotFound() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.NotFoundCount++
}

func (rs *ReadStats) AddCacheHit() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.CacheHitCount++
}

func (rs *ReadStats) AddCacheMiss() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.CacheMissCount++
}

func (rs *ReadStats) AddSpecificCacheHit(blockKind string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.CacheHitCounts[blockKind]++
}

func (rs *ReadStats) AddSpecificCacheMiss(blockKind string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.CacheMissCounts[blockKind]++
}

func (rs *ReadStats) AddBloomHit() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.BloomHitCount++
}

func (rs *ReadStats) AddBloomMiss() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.BloomMissCount++
}

func (rs *ReadStats) AddBloomFalsePositive() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.BloomFalsePositiveCount++
}

func (rs *ReadStats) PrintStats() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	fmt.Println("========== [ReadStats] ==========")

	fmt.Printf("\nRead Requests: %d\n", rs.ReadRequestCount)

	fmt.Printf("\nNotFound counts: %d\n", rs.NotFoundCount)

	// Memtable & Immtable
	fakeMemSum := int64(0)
	realMemSum := int64(0)
	fmt.Println("\nFake Mem Reads:")
	for _, k := range []string{"mem", "imm"} {
		cnt := rs.FakeMems[k]
		if cnt > 0 {
			fmt.Printf("  %-4s: %d\n", k, cnt)
			fakeMemSum += cnt
		}
	}
	fmt.Printf("  >> Fake Mem Reads Total: %d\n", fakeMemSum)

	fmt.Println("Real Mem Reads:")
	for _, k := range []string{"mem", "imm"} {
		cnt := rs.RealMems[k]
		if cnt > 0 {
			fmt.Printf("  %-4s: %d\n", k, cnt)
			realMemSum += cnt
		}
	}
	fmt.Printf("  >> Real Mem Reads Total: %d\n", realMemSum)

	// sort Levels
	var levels []int
	levelSet := make(map[int]struct{})
	for l := range rs.FakeLevels {
		levelSet[l] = struct{}{}
	}
	for l := range rs.RealLevels {
		levelSet[l] = struct{}{}
	}
	for l := range levelSet {
		levels = append(levels, l)
	}
	sort.Ints(levels)

	fakeLevelSum := int64(0)
	realLevelSum := int64(0)

	fmt.Println("\nFake Level Reads:")
	for _, l := range levels {
		cnt := rs.FakeLevels[l]
		if cnt > 0 {
			if l == 0 {
				fmt.Printf("  L%-2d: %d (FakeLevel0Attempts: %d)\n", l, cnt, rs.FakeLevel0Attempts)
			} else {
				fmt.Printf("  L%-2d: %d\n", l, cnt)
			}
			fakeLevelSum += cnt
		}
	}
	fmt.Printf("  >> Fake Level Reads Total: %d\n", fakeLevelSum)

	fmt.Println("Real Level Reads:")
	for _, l := range levels {
		cnt := rs.RealLevels[l]
		if cnt > 0 {
			if l == 0 {
				fmt.Printf("  L%-2d: %d (RealLevel0Attempts: %d)\n", l, cnt, rs.RealLevel0Attempts)
			} else {
				fmt.Printf("  L%-2d: %d\n", l, cnt)
			}
			realLevelSum += cnt
		}
	}
	fmt.Printf("  >> Real Level Reads Total: %d\n", realLevelSum)

	// ====== cache hit/miss/ratio ======
	fmt.Println("\nCache Stats:")
	fmt.Printf("  Cache Hit  : %d\n", rs.CacheHitCount)
	fmt.Printf("  Cache Miss : %d\n", rs.CacheMissCount)
	totalCacheAccess := rs.CacheHitCount + rs.CacheMissCount
	if totalCacheAccess > 0 {
		hitRate := 100.0 * float64(rs.CacheHitCount) / float64(totalCacheAccess)
		fmt.Printf("  Hit Rate   : %.2f%%\n", hitRate)
	} else {
		fmt.Printf("  Hit Rate   : N/A\n")
	}

	fmt.Println("\nDetailed Cache Stats:")
	// collect and sort blockKind
	var blockKinds []string
	for kind := range rs.CacheHitCounts {
		blockKinds = append(blockKinds, kind)
	}
	for kind := range rs.CacheMissCounts {
		// avoid duplication
		found := false
		for _, k := range blockKinds {
			if k == kind {
				found = true
				break
			}
		}
		if !found {
			blockKinds = append(blockKinds, kind)
		}
	}
	sort.Strings(blockKinds)

	for _, kind := range blockKinds {
		hits := rs.CacheHitCounts[kind]
		misses := rs.CacheMissCounts[kind]
		total := hits + misses
		hitRate := 0.0
		if total > 0 {
			hitRate = float64(hits) / float64(total) * 100.0
		}
		fmt.Printf("  %-12s  Hit: %8d   Miss: %8d   HitRate: %6.2f%%\n", kind, hits, misses, hitRate)
	}

	// bloom filter stats
	fmt.Println("\nBloom filter Stats:")
	bloomTotalChecks := rs.BloomHitCount + rs.BloomMissCount
	bloomHitRate := float64(0)
	bloomMissRate := float64(0)
	bloomFalsePositiveRate := float64(0)
	if bloomTotalChecks > 0 {
		bloomHitRate = float64(rs.BloomHitCount) / float64(bloomTotalChecks)
		bloomMissRate = float64(rs.BloomMissCount) / float64(bloomTotalChecks)
	}
	if rs.BloomHitCount > 0 {
		bloomFalsePositiveRate = float64(rs.BloomFalsePositiveCount) / float64(rs.BloomHitCount)
	}
	fmt.Printf("  Bloom Filter Total Checks     : %d\n", bloomTotalChecks)
	fmt.Printf("  Bloom Hit Count             : %d (Hit Rate: %.4f)\n", rs.BloomHitCount, bloomHitRate)
	fmt.Printf("  Bloom Miss Count            : %d (Miss Rate: %.4f)\n", rs.BloomMissCount, bloomMissRate)
	fmt.Printf("  Bloom False Positive Count  : %d (False Positive Rate among hits: %.4f)\n",
		rs.BloomFalsePositiveCount, bloomFalsePositiveRate)

	fmt.Println("\nImportant Stats:")
	fakeNon0LevelSum := int64(0)
	for level, cnt := range rs.FakeLevels {
		if level != 0 {
			fakeNon0LevelSum += cnt
		}
	}
	fakeLevelRatio := 0.0
	if rs.ReadRequestCount > 0 {
		fakeLevelRatio = float64(fakeNon0LevelSum) / float64(rs.ReadRequestCount)
	}

	hits := rs.CacheHitCounts["data-block"]
	misses := rs.CacheMissCounts["data-block"]
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100.0
	}

	fmt.Printf("  FakeReads(Level>0) / Read: %.4f (%d / %d)\n", fakeLevelRatio, fakeNon0LevelSum, rs.ReadRequestCount)
	fmt.Printf("  Data-block cache hit rate: %.2f%% (%d / %d)\n", hitRate, hits, total)

	fmt.Println("==================================")
}

func (rs *ReadStats) SaveToFile(filePath string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	data, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}
