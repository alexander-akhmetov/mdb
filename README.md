# mdb

[![Go Report Card](https://goreportcard.com/badge/github.com/alexander-akhmetov/mdb)](https://goreportcard.com/report/github.com/alexander-akhmetov/mdb)

A simple key-value storage.

It was created for learning purposes: I wanted to learn a bit of Go and create my own small database.

Not intended for production use. :)

Implemented storage types:

* In-memory
* File
* Indexed file  (simple hash map, no sparse indexes)
* Log structured merge tree ([LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree))

## Usage

By default it uses `lsmt.Storage`, but you can change this in the [main.go](db/main.go).

### Command-line interface

```bash
~/ » make run

go run cmd/*.go -i

######### Started #########

>> get key
value='', exists=false

>> set key value
Saved    key=value

>> get key
value='value', exists=true
```

### Go

```go
package main

import (
    "github.com/alexander-akhmetov/mdb/pkg"
    "github.com/alexander-akhmetov/mdb/pkg/lsmt"
)


func main() {
    db := storage.NewLSMTStorage(lsmt.StorageConfig{
        WorkDir:               "./lsmt_data/",
        CompactionEnabled:     true,
        MinimumFilesToCompact: 2,
        MaxMemtableSize:       65536,
        SSTableReadBufferSize: 4096,
    })
    defer db.Stop()

    db.Set("key_1", "value_1")

    value, found := db.Get("key_1")

    println("Found:", found)
    println("Value:", value)
}
```

More information about all these configuration options can be found in the `lsmt.Storage` section below.

## Internals

The database supports different storage types.

### memory.Storage

It's a simple hash map that holds everything in memory.

### file.Storage

It stores all information in a file. When you add a new entry, it simply appends the key and value to the file. So it's very fast to add new information. However, when you try to retrieve a key, it scans the entire file (starting from the beginning, not the end) to find the latest key. Therefore, reading is slow.

### indexedfile.Storage

This is a FileStorage with a simple index (hash map). When you add a new key, it saves the offset in bytes to the map in memory. To process the get command, it checks the index, finds the offset in bytes, and reads only a piece of the file. Writing and reading are fast, but you need a lot of memory to keep all keys in it.

### lsmt.Storage

It stores all data in sorted string tables (SSTables), which are essentially binary files. It supports sparse indexes, so you don't need a lot of memory to store all your keys like in indexedfile.Storage.

However, it will be slower than indexedfile.Storage because it uses a red-black tree to store a sparse index and checks all SSTables when you retrieve a value. This is because it can't determine whether it has this key without checking the SSTables on disk. It could probably use a Bloom filter for that.

```none
                     +------------+
                     |  Client    |
                     +------------+
                        |
                        | GET|SET
                        v
               +------------------------+              +---------------------+
               | +--------------------+ |              | +-----------------+ |
               | |  Append only log   | |              | |   memtable 10   | |
               | +--------------------+ | when memtable| +-----------------+ |
               | +--------------------+ | is too big   |       ...           |
               | |     memtable       | |------------> | +-----------------+ |
               | +--------------------+ |              | |   memtable 1    | |
               |                        |              | +-----------------+ |
               |                        |              |                     |
               | writer                 |              | flush queue         |
               +------------------------+              +---------------------+
                                                             ^
                                                             | flusher dumps memtables
                                                             | to disk in the background (as sstables)
                                                             v
                +------------+                         +---------------------+
                | Compaction |                         |       flusher       |
                +------------+                         +---------------------+
                    ^  periodical compaction process                 |
                    |  merges different small sstable files          |
                    v  into a big one and removes unnnecessary data  v
               +-------------------------------------------------------------+
               | +------------+  +-----------+                 +-----------+ |
               | | sstable 10 |  | sstable 9 |     ...         | sstable 0 | |
               | +------------+  +-----------+                 +-----------+ |
               |                                                             |
               |                                                             |
               | SSTables storage                                            |
               +-------------------------------------------------------------+

```

Main parts:

* Writer (memtable)
* Flush queue (list of memtables)
* Flusher (dumps a memtable to a disk)
* SSTables storage (main storage for the data)
* Compaction (background process to remove old keys that were updated)

#### GET process

1. Check memtable
2. Check memtables in flush queue
3. Check SSTables

It checks all these parts in this order to be sure that it returns the latest version of the key.
Each SSTable has its own index. It can be sparse: it will not keep each key-offset pair in the index,
but it will store keys every N bytes. We can do this because SSTable files are sorted and read-only. When we need to find a
key, we find its offset or closest minimal to this key. After we can load part of the file into memory and find the value for the key.

#### SET

1. Save value to append only log
2. Save value to memtable

#### Flush

When the memtable becomes bigger than some threshold, the core component puts it to the flush queue and initializes a new memtable. 
The flusher is a background process that checks the queue and dumps memtables as SSTables to disk.

#### Compaction

It's a periodical background process that merges small SSTable files into a larger one and removes old key-value pairs that can be removed.

#### SSTables storage

It's a disk storage. During start-up, mdb checks this folder, registers all files, and builds indexes. 
Files are read-only; mdb never changes them. It can only merge them into a larger file, but without modifying old files.

#### File format

Binary file format:

```none
[entry_type: 1byte][key_length: 4bytes][value_length: 4bytes][key][value]

entry_type:

* 0 - value

```

##### Configuration

```none
CompactionEnabled     bool  // Enable/disable the background compaction process
MinimumFilesToCompact int   // How many files are needed to start the compaction process
MaxMemtableSize       int64 // max size for memtable
MaxCompactFileSize    int64 // Do not compact files bigger than this size
SSTableReadBufferSize int   // Read buffer size: the database will build indexes every
                            // <SSTableReadBufferSize> bytes. If you want to have a non-sparse index
                            // put 1 here
```

#### performance test mode

Start performance test: insert 10000 keys (`-k 10000`) and then check them (`-c`):

```bash
go run cmd/*.go -p -k 10000 -c 2>&1 | grep -v 'Adding'
```

It will print something like that (it prints `Inserted: <count>` every second):

```none
[DEBUG] Read buffer size: 65536
[DEBUG] Maximum memtable size: 16384
[DEBUG] Starting lsmt storage
[DEBUG] Creating dir lsmt_data/sstables
[DEBUG] Creating dir lsmt_data/aolog_tf
[DEBUG] Creating dir lsmt_data/tmp
[DEBUG] Initializing a new SSTable instance...
[DEBUG] initialized sstables: 16
[DEBUG] Restoring flush queue...
[DEBUG] Flush queue has been restored with size= 0
[DEBUG] AOLog file exists, restoring...
[DEBUG] Restored 11710 entries
[DEBUG] Storage ready
[DEBUG] Started flusher process
[DEBUG] Started compaction process
[DEBUG] Started compaction process

[DEBUG] Inserted: 7440
[DEBUG] Inserted: 2560

[DEBUG] OK. Inserted keys checked: 10000
```

## TODO

* delete command
* bloom filter
* range queries
