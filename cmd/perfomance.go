package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/alexander-akhmetov/mdb/pkg"
)

// now it's better to start performance test with external output filtering:
//
// go run db/*.go -p 2>&1 |  grep -v 'DEBUG'
//
// Otherwise it will print a lot of additional log informarion: per each inserted key.
// Later I will add log filtering to the performance test.

// with this counter we will calculate how many
// inserts were made for the previous second
var counter = 0

// This is an infinite loop which just writes random keys to the storage
// and every second it prints output: how many keys were inserted for the previous second,
// for example:
//
// 2018/08/17 07:21:39.010602 Inserted: 13141
// 2018/08/17 07:21:40.010651 Inserted: 13169
//
// It doesn't check are the inserted values valid or not.
// It just inserts key as fast as it can, nothing else.
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

// printStatsEverySecond prints counter value and sets it to 0,
// then it sleeps for a second and does the same, again and again :)
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
