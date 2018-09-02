# mdb

A simple key-value storage.

It was created for learning purposes: I wanted to know Go more and create my own small database.

Not for production usage. :)

Implemented storage types:

* In-memory
* File
* Indexed file  (simple hash map, no sparse indexes)
* Log structured merge tree ([LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree))

## Usage

By default it uses `lsmt.Storage`, but you can change it in the [main.go](db/main.go).

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

More about all this configuration options you can read in the `lsmt.Storage` section below.

## Internals

The database supports four different storage types.

### memory.Storage

It's a simple hash map which holds all in the memory. Nothing else :)

### file.Storage

It stores all information in the file. When you add a new entry, it just appends key and value to the file. So it's very fast to add new information.
But when you are trying to get the key, it scans the whole file (and from the beginning, not from the end) to find the latest key. So reading is slow.

### indexedfile.Storage

It is a `FileStorage` with a simple index (hash map). When you add a new key, it saves offset in bytes to the map in the memory. To process `get` command it checks the index, finds offset in bytes and reads only a piece of the file. Writing and reading are fast, but you need a lot of memory to keep all keys in it.

### lsmt.Storage

The most interesting part of this project. :)

It's something similar to Google's LevelDB or Facebook's RocksDB.
It keeps all data in sorted string tables (SSTable) which are basically binary files.
Supports sparse indexes, so you don't need a lot of memory to store all your keys like in `indexedfile.Storage`.

But it will be slower than `indexedfile.Storage`, because it uses a red-black tree to store sparse index and it checks all SSTables when you retrieve a value because it can't say that it doesn't have this key without checking the SSTables on a disk.

To make it faster in this situation, we can use a Bloom filter.

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
* Compaction (background process to remove old keys which were updated)

#### GET process

1. Check memtable
2. Check memtables in flush queue
3. Check SSTables

It checks all these parts in this order to be sure that it returns the latest version of the key.
Each SSTable has its own index (red-black tree). It can be sparse: it will not keep each key-offset pair in the index,
but it will store keys every N bytes. We can do this because SSTable files are sorted and read-only. When we need to find a
key, we find its offset or closest minimal to this key. After we can load part of the file into memory and find the value for the key.

#### SET

1. Save value to append only log
2. Save value to memtable

#### Flush

When memtable becomes bigger than some threshold, core component puts it to the flush queue and initializes a new memtable.
Flusher is a background process which checks the queue and dumps memtables as sstables to a disk.

#### Compaction

It's a periodical background process.
It merges small SSTable files into a bigger one and removes old key-value pairs which can be removed.

#### SSTables storage

It's a disk storage. On start-up time `mdb` checks this folder and registers all files and builds indexes.
Files are read-only, `mdb` never change them. It can only merge them into a big one file, but without modifying old files.

#### File format

Binary file format:

```none
[entry_type: 1byte][key_length: 4bytes][value_length: 4bytes][key][value]

entry_type:

* 0 - value

```

##### Configuration

```none
CompactionEnabled     bool  // you can disable background compaction process
MinimumFilesToCompact int  // how many files does it need to start compaction process
MaxMemtableSize       int64 // max size for memtable
MaxCompactFileSize    int64 // do not compact files bigger than this size
SSTableReadBufferSize int  // read buffer size: database will build indexes each
                           // <SSTableReadBufferSize> bytes. If you want to have non-sparse index
                           // put 1 here
```

#### Perfomance test mode

Start perfomance test: insert 10000 keys (`-k 10000`) and then check them (`-c`):

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
