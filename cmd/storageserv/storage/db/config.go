//
//  Copyright 2023 PayPal Inc.
//
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package db

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
	"github.com/paypal/junodb/third_party/forked/tecbot/gorocksdb"
)

///TODO need to add validation

// Based on rocksdb 5.5.1
type Config struct {

	// rocksdb: size_t write_buffer_size (Default: 64M)
	// Amount of data to build up in memory (backed by an unsorted log
	// on disk) before converting to a sorted on-disk file
	WriteBufferSize int

	// rocksdb: int max_write_buffer_number (2) (min: 2)
	// The maximum number of write buffers that are built up in memory.
	// The default and the minimum number is 2, so that when 1 write buffer
	// is being flushed to storage, new writes can continue to the other
	// write buffer.
	// If max_write_buffer_number > 3, writing will be slowed down to
	// options.delayed_write_rate if we are writing to the last write buffer
	// allowed.
	MaxWriteBufferNumber int

	// rocksdb: int min_write_buffer_number_to_merge (Default: 1)
	// The minimum number of write buffers that will be merged together
	// before writing to storage.  If set to 1, then
	// all write buffers are flushed to L0 as individual files and this increases
	// read amplification because a get request has to check in all of these
	// files. Also, an in-memory merge may result in writing lesser
	// data to storage if there are duplicate records in each of these
	// individual write buffers.
	MinWriteBufferNumberToMerge int

	// rocksdb: int level0_file_num_compaction_trigger (Default: 4)
	// Number of files to trigger level-0 compaction. A value <0 means that
	// level-0 compaction will not be triggered by number of files at all.
	Level0FileNumCompactionTrigger int

	// rocksdb: int level0_slowdown_writes_trigger (Default: 20)
	// Soft limit on number of level-0 files. We start slowing down writes at this
	// point. A value <0 means that no writing slow down will be triggered by
	// number of files in level-0
	Level0SlowdownWritesTrigger int

	// rocksdb: int level0_stop_writes_trigger (Default: 36)
	// Maximum number of level-0 files.  We stop writes at this point.
	Level0StopWritesTrigger int

	// rocksdb: unsigned int stats_dump_period_sec (Default: 600)
	// if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
	StatsDumpPeriodSec uint

	// rocksdb: uint64_t max_bytes_for_level_base (Default: 256M)
	// Control maximum total data size for a level.
	// max_bytes_for_level_base is the max total for level-1.
	// Maximum number of bytes for level L can be calculated as
	// (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
	// For example, if max_bytes_for_level_base is 200MB, and if
	// max_bytes_for_level_multiplier is 10, total data size for level-1
	// will be 200MB, total file size for level-2 will be 2GB,
	// and total file size for level-3 will be 20GB
	MaxBytesForLevelBase uint64

	// rocksdb: double max_bytes_for_level_multiplier (Default: 10)
	MaxBytesForLevelMultiplier float64

	// rocksdb: uint64_t target_file_size_base (Default: 64M)
	TargetFileSizeBase uint64

	// rocksdb: int target_file_size_multiplier (Default: 1)
	TargetFileSizeMultiplier int

	// rocksdb: size_t keep_log_file_num (Default: 1000)
	KeepLogFileNum int

	// rocksdb: int max_background_flushes (Default: 1)
	// Maximum number of concurrent background memtable flush jobs, submitted to
	// the HIGH priority thread pool.
	//
	// By default, all background jobs (major compaction and memtable flush) go
	// to the LOW priority pool. If this option is set to a positive number,
	// memtable flush jobs will be submitted to the HIGH priority pool.
	// It is important when the same Env is shared by multiple db instances.
	// Without a separate pool, long running major compaction jobs could
	// potentially block memtable flush jobs of other db instances, leading to
	// unnecessary Put stalls.
	//
	// If you're increasing this, also consider increasing number of threads in
	// HIGH priority thread pool. For more information, see
	// Env::SetBackgroundThread
	MaxBackgroundFlushes int

	// rocksdb: int max_background_compactions (Default: 1)
	// Maximum number of concurrent background compaction jobs, submitted to
	// the default LOW priority thread pool.
	// We first try to schedule compactions based on
	// `base_background_compactions`. If the compaction cannot catch up , we
	// will increase number of compaction threads up to
	// `max_background_compactions`.
	// If you're increasing this, also consider increasing number of threads in
	// LOW priority thread pool. For more information, see
	// Env::SetBackgroundThreads
	MaxBackgroundCompactions int

	// rocksdb CompressionType compression (Default: kSnappyCompression)
	// if it's supported. If snappy is not linked
	// with the library, the default is kNoCompression.
	// Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
	//    ~200-500MB/s compression
	//    ~400-800MB/s decompression
	// Note that these speeds are significantly faster than most
	// persistent storage speeds, and therefore it is typically never
	// worth switching to kNoCompression.  Even if the input data is
	// incompressible, the kSnappyCompression implementation will
	// efficiently detect that and will switch to uncompressed mode.
	//  kNoCompression = 0x0,
	//  kSnappyCompression = 0x1,
	//  kZlibCompression = 0x2,
	//  kBZip2Compression = 0x3,
	//  kLZ4Compression = 0x4,
	//  kLZ4HCCompression = 0x5,
	//  kXpressCompression = 0x6,
	//  kZSTD = 0x7
	Compression gorocksdb.CompressionType

	//	DebugInfoLogLevel = InfoLogLevel(0)
	//	InfoInfoLogLevel  = InfoLogLevel(1)
	//	WarnInfoLogLevel  = InfoLogLevel(2)
	//	ErrorInfoLogLevel = InfoLogLevel(3)
	//	FatalInfoLogLevel = InfoLogLevel(4
	InfoLogLevel gorocksdb.InfoLogLevel

	// write option
	// rocksdb: bool sync (Default: false)
	// If true, the write will be flushed from the operating system
	// buffer cache (by calling WritableFile::Sync()) before the write
	// is considered complete.  If this flag is true, writes will be
	// slower.
	//
	// If this flag is false, and the machine crashes, some recent
	// writes may be lost.  Note that if it is just the process that
	// crashes (i.e., the machine does not reboot), no writes will be
	// lost even if sync==false.
	//
	// In other words, a DB write with sync==false has similar
	// crash semantics as the "write()" system call.  A DB write
	// with sync==true has similar crash semantics to a "write()"
	// system call followed by "fdatasync()".
	//
	WriteSync bool

	// write option
	// rocksdb: bool disableWAL (Default: false)
	// If true, writes will not first go to the write ahead log,
	// and the write may got lost after a crash.
	WriteDisableWAL bool

	RandomizeWriteBuffer bool

	RateBytesPerSec int64

	HighPriorityBackgroundThreads int

	LowPriorityBackgroundThreads int

	NewLRUCacheSizeInMB int

	DbPaths []DbPath

	WalDir string
}

type DbPath struct {
	Path       string
	TargetSize uint64
}

func (c *Config) GetPaths() []string {
	paths := make([]string, len(c.DbPaths))
	for i, dbpath := range c.DbPaths {
		paths[i] = dbpath.Path
	}
	return paths
}

var defaultFlashConfig = Config{
	WriteBufferSize:      64000000,
	MaxWriteBufferNumber: 5,
	// options will be sanitized by rocksdb to make
	//  min_write_buffer_number_to_merge = min(min_write_buffer_number_to_merge, (max_write_buffer_number-1))
	MinWriteBufferNumberToMerge:    1,
	Level0FileNumCompactionTrigger: 8,
	Level0SlowdownWritesTrigger:    20,
	Level0StopWritesTrigger:        36,
	StatsDumpPeriodSec:             600,
	MaxBytesForLevelBase:           512000000,
	MaxBytesForLevelMultiplier:     10,
	TargetFileSizeBase:             64000000,
	TargetFileSizeMultiplier:       1,
	KeepLogFileNum:                 2,
	MaxBackgroundFlushes:           6,
	MaxBackgroundCompactions:       10,
	Compression:                    gorocksdb.NoCompression, //NoCompression,
	InfoLogLevel:                   gorocksdb.InfoInfoLogLevel,
	RandomizeWriteBuffer:           true,
	WriteSync:                      false,
	WriteDisableWAL:                true,
	RateBytesPerSec:                0,
	HighPriorityBackgroundThreads:  0,
	LowPriorityBackgroundThreads:   0,
	NewLRUCacheSizeInMB:            0,
}

var DBConfig = defaultFlashConfig

// Note: rocksdb C binding does not support getters for option types
func NewRocksDBptions() *gorocksdb.Options {
	options := gorocksdb.NewDefaultOptions()

	options.SetCreateIfMissing(true)
	if DBConfig.RandomizeWriteBuffer {
		if DBConfig.WriteBufferSize > 0 {
			f := float32(DBConfig.WriteBufferSize)
			sz := f*0.75 + rand.Float32()*f*0.25
			if sz < 4096 {
				sz = 4096
			}
			options.SetWriteBufferSize(int(sz))
		}
	} else {
		if DBConfig.WriteBufferSize > 0 {
			options.SetWriteBufferSize(DBConfig.WriteBufferSize)
		}
	}
	if DBConfig.MaxWriteBufferNumber > 2 {
		options.SetMaxWriteBufferNumber(DBConfig.MaxWriteBufferNumber)
	}
	if DBConfig.MinWriteBufferNumberToMerge > 1 { ///TODO
		options.SetMinWriteBufferNumberToMerge(DBConfig.MinWriteBufferNumberToMerge)
	}

	if DBConfig.Level0FileNumCompactionTrigger != 0 {
		options.SetLevel0FileNumCompactionTrigger(DBConfig.Level0FileNumCompactionTrigger)
	}

	if DBConfig.Level0SlowdownWritesTrigger != 0 {
		options.SetLevel0SlowdownWritesTrigger(DBConfig.Level0SlowdownWritesTrigger)
	}
	if DBConfig.Level0StopWritesTrigger > 0 { ///TODO
		options.SetLevel0StopWritesTrigger(DBConfig.Level0StopWritesTrigger)
	}
	if DBConfig.StatsDumpPeriodSec != 600 { ///TODO. to find out what if set it to zero
		options.SetStatsDumpPeriodSec(DBConfig.StatsDumpPeriodSec)
	}

	if DBConfig.MaxBytesForLevelBase > 0 {
		options.SetMaxBytesForLevelBase(DBConfig.MaxBytesForLevelBase)
	}
	if DBConfig.MaxBytesForLevelMultiplier > 0 {
		options.SetMaxBytesForLevelMultiplier(DBConfig.MaxBytesForLevelMultiplier)
	}
	if DBConfig.TargetFileSizeBase > 0 {
		options.SetTargetFileSizeBase(DBConfig.TargetFileSizeBase)
	}
	if DBConfig.TargetFileSizeMultiplier > 0 {
		options.SetTargetFileSizeMultiplier(DBConfig.TargetFileSizeMultiplier)
	}
	if DBConfig.KeepLogFileNum > 0 { ///TODO
		options.SetKeepLogFileNum(DBConfig.KeepLogFileNum)
	}
	if DBConfig.MaxBackgroundFlushes > 0 {
		options.SetMaxBackgroundFlushes(DBConfig.MaxBackgroundFlushes)
	}
	if DBConfig.MaxBackgroundCompactions > 0 {
		options.SetMaxBackgroundCompactions(DBConfig.MaxBackgroundCompactions)
	}

	if DBConfig.Compression == gorocksdb.NoCompression ||
		DBConfig.Compression == gorocksdb.SnappyCompression ||
		DBConfig.Compression == gorocksdb.ZLibCompression ||
		DBConfig.Compression == gorocksdb.Bz2Compression ||
		DBConfig.Compression == gorocksdb.LZ4Compression ||
		DBConfig.Compression == gorocksdb.LZ4HCCompression {
		options.SetCompression(DBConfig.Compression)
	} else {
		glog.Infof("unsupported compression type %v", DBConfig.Compression)
	}

	options.SetEnablePipelinedWrite(true)
	if DBConfig.RateBytesPerSec > 0 {
		rateLimiter := gorocksdb.NewRateLimiter(DBConfig.RateBytesPerSec, 100*1000, 10)
		options.SetRateLimiter(rateLimiter)
	}

	env := gorocksdb.NewDefaultEnv()
	if DBConfig.HighPriorityBackgroundThreads > 0 {
		env.SetHighPriorityBackgroundThreads(DBConfig.HighPriorityBackgroundThreads)
	}
	if DBConfig.LowPriorityBackgroundThreads > 0 {
		env.SetBackgroundThreads(DBConfig.LowPriorityBackgroundThreads)
	}
	options.SetEnv(env)

	options.SetMaxBytesForLevelBase(uint64(DBConfig.WriteBufferSize) * uint64(DBConfig.MinWriteBufferNumberToMerge*DBConfig.Level0FileNumCompactionTrigger))
	options.SetTargetFileSizeBase(DBConfig.TargetFileSizeBase)

	return options
}

func (cfg *Config) OnLoad() {
	writeOptions.SetSync(cfg.WriteSync)
	writeOptions.DisableWAL(cfg.WriteDisableWAL)

	if !cfg.WriteDisableWAL && len(cfg.WalDir) > 0 {
		if _, err := os.Stat(cfg.WalDir); errors.Is(err, fs.ErrNotExist) {
			err = os.MkdirAll(cfg.WalDir, 0777)
			if err != nil {
				glog.Exit("Error : ", err.Error())
			}
		}
	}
}

func init() {
	///TODO sanitize config
	rand.Seed(int64(os.Getpid()))
}

func ConfigBlockCache() *gorocksdb.BlockBasedTableOptions {
	blockOpts := gorocksdb.NewDefaultBlockBasedTableOptions()
	blockOpts.SetFilterPolicy(gorocksdb.NewBloomFilter(10))
	if DBConfig.NewLRUCacheSizeInMB > 0 {
		cache := gorocksdb.NewLRUCache(1024 * 1024 * DBConfig.NewLRUCacheSizeInMB)
		blockOpts.SetBlockCache(cache)
	}

	msg := fmt.Sprintf("NewLRUCacheSizeInMB=%d ", DBConfig.NewLRUCacheSizeInMB)
	glog.Info(msg)

	return blockOpts
}

func (cfg *Config) Validate() (err error) {
	if len(cfg.DbPaths) == 0 {
		err = fmt.Errorf("db.Config error: DbPaths not defined")
	}
	return
}
