package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/alexander-akhmetov/mdb/pkg"
)

// It's better to start the performance test with external output filtering:
//
// go run db/*.go -p 2>&1 |  grep -v 'DEBUG'
//
// Otherwise, it will print a lot of additional log information. For example, a log line for each inserted key.
// TODO: Add log filtering to the performance test.

// With this counter, we will calculate how many
// inserts were made in the previous second.
var counter = 0

// This is an infinite loop that writes random keys to the storage
// and prints the output every second: how many keys were inserted.
// For example:
//
// 2018/08/17 07:21:39.010602 Inserted: 13141
// 2018/08/17 07:21:40.010651 Inserted: 13169
//
// It doesn't check whether the inserted values are valid.
// It simply inserts keys as fast as possible, nothing more.
func performanceTest(db mdb.Storage, maxKeys int, checkKeys bool) {
	go printStatsEverySecond()

	keyValues := map[string]string{}

	cycleCounter := 0
	for true == true {
		k := randString(20)
		v := randString(30)
		db.Set(k, v)
		counter++
		cycleCounter++

		if checkKeys {
			keyValues[k] = v
		}

		if maxKeys != -1 && cycleCounter >= maxKeys {
			log.Printf("Max insert keys reached: %v, stopping...", cycleCounter)
			break
		}
	}

	if checkKeys {
		assertKeyValues(db, keyValues)
	}

}

// printStatsEverySecond prints the counter value, resets it,
// sleeps for a second, and repeats the process.
func printStatsEverySecond() {
	for true == true {
		log.Printf("%sInserted: %v%s", colorGreen, counter, colorNeutral)
		counter = 0
		time.Sleep(time.Second)
	}
}

func assertKeyValues(db mdb.Storage, keyValues map[string]string) {
	log.Println("Checking inserted keys")
	for k, v := range keyValues {
		value, found := db.Get(k)
		if !found || v != value {
			log.Panicf(
				"Key '%s' has not been found or returned wrong value! returned='%s' found=%v",
				k,
				value,
				found,
			)
		}
	}
	log.Printf("OK. Inserted keys checked: %v", len(keyValues))
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
