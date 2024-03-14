package common

import (
	"fmt"
	"os"
	"sort"
	"sync"
)

var (
	PositionSuffix   = ""
	FoundLevels      = make(map[string]int) // disk level where data to read is found
	FoundLevelsMutex sync.Mutex

	// read for compaction (but maybe only block read is executed)
	MissingReadTableCnt       = 0
	MissingReadBlockCnt       = 0
	MissingReadFilterBlockCnt = 0

	// CacheStats[blockNumStr] = *CacheStat
	CacheStats       = make(map[string]*CacheStat)
	CurrentCacheStat = NewCacheStat()
	CacheStatMutex   sync.Mutex
)

// CAUTION: reads can be executed in parallel, so this might not be an absolutely accurate indicator
// if we need exact performance measure for Geth, refer to tx execution times
type CacheStat struct {
	StartBlockNum uint64
	EndBlockNum   uint64

	//
	// cache stats from Geth's trie
	//

	ReadTrieCleanCacheNum  int64 // # of trie nodes read from clean cache of geth's trie
	ReadTrieCleanCacheTime int64
	ReadTrieDirtyCacheNum  int64 // # of trie nodes read from dirty cache of geth's trie
	ReadTrieDirtyCacheTime int64

	//
	// cache stats in leveldb
	//

	ReadNumsPerPosition  map[string]int64
	ReadTimesPerPosition map[string]int64
	ReadSizesPerPosition map[string]int64

	ReadNumsPerType  map[string]int64 // data's type (ex. trie node, snapshot, contract code)
	ReadSizesPerType map[string]int64
	ReadTimesPerType map[string]int64

	ReadTableRequestNum int // # of requests to read SSTable
	ReadTableMissNum    int // # of SSTables not included in the cache
	ReadTableTime       int64

	ReadBlockRequestNum int // # of requests to read index/data blocks
	ReadBlockMissNum    int // # of index/data blocks not included in the cache
	ReadBlockTime       int64

	ReadFilterRequestNum int // # of requests to read filter blocks
	ReadFilterMissNum    int // # of filter blocks not included in the cache
	ReadFilterTime       int64
}

func NewCacheStat() *CacheStat {
	cs := new(CacheStat)
	cs.ReadNumsPerPosition = make(map[string]int64)
	cs.ReadTimesPerPosition = make(map[string]int64)
	cs.ReadSizesPerPosition = make(map[string]int64)
	cs.ReadNumsPerType = make(map[string]int64)
	cs.ReadSizesPerType = make(map[string]int64)
	cs.ReadTimesPerType = make(map[string]int64)
	return cs
}

func (cs *CacheStat) Add(otherCS *CacheStat) {

	if cs.StartBlockNum == 0 && cs.EndBlockNum == 0 {
		cs.StartBlockNum = otherCS.StartBlockNum
	} else if cs.EndBlockNum+1 != otherCS.StartBlockNum {
		fmt.Println("ERROR: cannot add these cache stats")
		fmt.Println("cs.EndBlockNum:", cs.EndBlockNum)
		fmt.Println("otherCS.StartBlockNum:", otherCS.StartBlockNum)
		os.Exit(1)
	}
	cs.EndBlockNum = otherCS.EndBlockNum

	cs.ReadTrieCleanCacheNum += otherCS.ReadTrieCleanCacheNum
	cs.ReadTrieCleanCacheTime += otherCS.ReadTrieCleanCacheTime
	cs.ReadTrieDirtyCacheNum += otherCS.ReadTrieDirtyCacheNum
	cs.ReadTrieDirtyCacheTime += otherCS.ReadTrieDirtyCacheTime

	for k, v := range otherCS.ReadNumsPerPosition {
		cs.ReadNumsPerPosition[k] += v
		cs.ReadTimesPerPosition[k] += otherCS.ReadTimesPerPosition[k]
		cs.ReadSizesPerPosition[k] += otherCS.ReadSizesPerPosition[k]
	}
	for k, v := range otherCS.ReadNumsPerType {
		cs.ReadNumsPerType[k] += v
		cs.ReadSizesPerType[k] += otherCS.ReadSizesPerType[k]
		cs.ReadTimesPerType[k] += otherCS.ReadTimesPerType[k]
	}

	cs.ReadTableRequestNum += otherCS.ReadTableRequestNum
	cs.ReadTableMissNum += otherCS.ReadTableMissNum
	cs.ReadTableTime += otherCS.ReadTableTime

	cs.ReadBlockRequestNum += otherCS.ReadBlockRequestNum
	cs.ReadBlockMissNum += otherCS.ReadBlockMissNum
	cs.ReadBlockTime += otherCS.ReadBlockTime

	cs.ReadFilterRequestNum += otherCS.ReadFilterRequestNum
	cs.ReadFilterMissNum += otherCS.ReadFilterMissNum
	cs.ReadFilterTime += otherCS.ReadFilterTime
}

func (cs *CacheStat) Print() {

	fmt.Println("print CacheStat -> start block num:", cs.StartBlockNum, "/ end block num:", cs.EndBlockNum)

	//
	// LevelDB
	//

	fmt.Println("print data read stats in LevelDB")
	totalDiskReadCnt := int64(0)
	totalDiskReadTime := int64(0)
	totalDiskReadSize := int64(0)
	mapKeys := make([]string, 0)
	for k, _ := range cs.ReadNumsPerPosition {
		mapKeys = append(mapKeys, k)
	}
	sort.Strings(mapKeys)
	for _, position := range mapKeys {
		fmt.Println("  at position", position, "-> avg:", cs.ReadTimesPerPosition[position]/cs.ReadNumsPerPosition[position], "ns (cnt:", cs.ReadNumsPerPosition[position], "/ time:", cs.ReadTimesPerPosition[position], "ns / size:", cs.ReadSizesPerPosition[position], "B )")
		totalDiskReadCnt += cs.ReadNumsPerPosition[position]
		totalDiskReadTime += cs.ReadTimesPerPosition[position]
		totalDiskReadSize += cs.ReadSizesPerPosition[position]
	}

	mapKeys = make([]string, 0)
	for k, _ := range cs.ReadNumsPerType {
		mapKeys = append(mapKeys, k)
	}
	sort.Strings(mapKeys)
	for _, dataType := range mapKeys {
		fmt.Println("  for type", dataType, "-> avg:", cs.ReadTimesPerType[dataType]/cs.ReadNumsPerType[dataType], "ns (cnt:", cs.ReadNumsPerType[dataType], "/ time:", cs.ReadTimesPerType[dataType], "ns / size:", cs.ReadSizesPerType[dataType], "B )")
	}

	if totalDiskReadCnt != 0 {
		fmt.Println("    => total -> avg:", totalDiskReadTime/totalDiskReadCnt, "ns (cnt:", totalDiskReadCnt, "/ time:", totalDiskReadTime, "/ size:", totalDiskReadSize, "B )")
	}

	if cs.ReadTableRequestNum > 0 {
		fmt.Println("    => SSTable read time -> avg:", cs.ReadTableTime/int64(cs.ReadTableRequestNum), "ns ( cnt:", cs.ReadTableRequestNum, "/ time:", cs.ReadTableTime, "-> portion:", float64(cs.ReadTableTime)/float64(totalDiskReadTime)*100, "% )")
		fmt.Println("    => SSTable cache miss cnt:", cs.ReadTableMissNum)
		fmt.Println("    => SSTable cache hit rate:", 100-float64(cs.ReadTableMissNum)/float64(cs.ReadTableRequestNum)*100, "%")
	} else {
		fmt.Println("    => SSTable read request num:", cs.ReadTableRequestNum)
	}

	if cs.ReadBlockRequestNum > 0 {
		fmt.Println("    => index & data block read time -> avg:", cs.ReadBlockTime/int64(cs.ReadBlockRequestNum), "ns ( cnt:", cs.ReadBlockRequestNum, "/ time:", cs.ReadBlockTime, "-> portion:", float64(cs.ReadBlockTime)/float64(totalDiskReadTime)*100, "% )")
		fmt.Println("    => index & data block cache miss cnt:", cs.ReadBlockMissNum)
		fmt.Println("    => index & data block cache hit rate:", 100-float64(cs.ReadBlockMissNum)/float64(cs.ReadBlockRequestNum)*100, "%")
	} else {
		fmt.Println("    => index & data block read request num:", cs.ReadBlockRequestNum)
	}

	if cs.ReadFilterRequestNum > 0 {
		fmt.Println("    => filter block read time -> avg:", cs.ReadFilterTime/int64(cs.ReadFilterRequestNum), "ns ( cnt:", cs.ReadFilterRequestNum, "/ time:", cs.ReadFilterTime, "-> portion:", float64(cs.ReadFilterTime)/float64(totalDiskReadTime)*100, "% )")
		fmt.Println("    => filter block cache miss cnt:", cs.ReadFilterMissNum)
		fmt.Println("    => filter block cache hit rate:", 100-float64(cs.ReadFilterMissNum)/float64(cs.ReadFilterRequestNum)*100, "%")
	} else {
		fmt.Println("    => filter block read request num:", cs.ReadFilterRequestNum)
	}

	//
	// Geth's trie
	//

	fmt.Println("print trie node read stats in Geth")
	if cs.ReadTrieCleanCacheNum > 0 {
		fmt.Println("  at position clean -> avg:", cs.ReadTrieCleanCacheTime/cs.ReadTrieCleanCacheNum, "ns ( cnt:", cs.ReadTrieCleanCacheNum, "/ time:", cs.ReadTrieCleanCacheTime, ")")
	}
	if cs.ReadTrieDirtyCacheNum > 0 {
		fmt.Println("  at position cirty -> avg:", cs.ReadTrieDirtyCacheTime/cs.ReadTrieDirtyCacheNum, "ns ( cnt:", cs.ReadTrieDirtyCacheNum, "/ time:", cs.ReadTrieDirtyCacheTime, ")")
	}
	if cs.ReadNumsPerType["trieNode"] > 0 {
		dataType := "trieNode"
		fmt.Println("  at position leveldb -> avg:", cs.ReadTimesPerType[dataType]/cs.ReadNumsPerType[dataType], "ns (cnt:", cs.ReadNumsPerType[dataType], "/ time:", cs.ReadTimesPerType[dataType], "ns / size:", cs.ReadSizesPerType[dataType], "B )")
	}
	totalCnt := cs.ReadTrieCleanCacheNum + cs.ReadTrieDirtyCacheNum + cs.ReadNumsPerType["trieNode"]
	totalTime := cs.ReadTrieCleanCacheTime + cs.ReadTrieDirtyCacheTime + cs.ReadTimesPerType["trieNode"]
	if totalCnt > 0 {
		fmt.Println("    => total -> avg:", totalTime/totalCnt, "ns ( cnt:", totalCnt, "/ time:", totalTime, ")")
		fmt.Println("    => node cache hit rate:", 100-float64(cs.ReadNumsPerType["trieNode"])/float64(totalCnt)*100, "%")
	}
}
