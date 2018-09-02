package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/alexander-akhmetov/mdb/pkg"
	"github.com/alexander-akhmetov/mdb/pkg/lsmt"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	perfomanceMode := flag.Bool("perfomance", false, "Run perfomance test")
	flag.BoolVar(perfomanceMode, "p", false, "Run perfomance test")

	perfomanceMaxKeysHelp := "Perfomance: maximum keys to insert"
	perfomanceMaxKeys := flag.Int("perfomance-max-keys", -1, perfomanceMaxKeysHelp)
	flag.IntVar(perfomanceMaxKeys, "k", -1, perfomanceMaxKeysHelp)

	checkKeys := flag.Bool("check-keys", false, "Perfomance: check inserted keys")
	flag.BoolVar(checkKeys, "c", false, "Perfomance: check inserted keys")

	interactiveMode := flag.Bool("interactive", false, "Run interactive mode")
	flag.BoolVar(interactiveMode, "i", false, "Run interactive mode")

	readBufferSizeHelp := "Read buffer size: determines index size"
	readBufferSize := flag.Int("read-buffer-size", 65536, readBufferSizeHelp)
	flag.IntVar(readBufferSize, "r", 65536, readBufferSizeHelp)

	maxMemtableSizeHelp := "Read buffer size: determines index size"
	maxMemtableSize := flag.Int64("max-memtable-size", 16384, maxMemtableSizeHelp)
	flag.Int64Var(maxMemtableSize, "m", 16384, maxMemtableSizeHelp)

	flag.Parse()

	log.Printf("Read buffer size: %v", *readBufferSize)
	log.Printf("Maximum memtable size: %v", *maxMemtableSize)

	if *perfomanceMode {
		db := initStorage(*maxMemtableSize, *readBufferSize)
		defer db.Stop()
		perfomanceTest(db, *perfomanceMaxKeys, *checkKeys)
		return
	}

	if *interactiveMode {
		db := initStorage(*maxMemtableSize, *readBufferSize)
		defer db.Stop()
		startMainWorkingLoop(db)
		return
	}

	flag.Usage()
}

func initStorage(maxMemtableSize int64, readBufferSize int) mdb.Storage {
	db := mdb.NewLSMTStorage(lsmt.StorageConfig{
		WorkDir:               "./lsmt_data/",
		CompactionEnabled:     true,
		MinimumFilesToCompact: 2,
		MaxMemtableSize:       maxMemtableSize,
		SSTableReadBufferSize: readBufferSize,
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			exitCommand(db)
		}
	}()

	return db
}

func startMainWorkingLoop(db mdb.Storage) {
	printlnGreen("######### Started #########")
	helpCommand()

	for true == true {
		fmt.Print(">> ")
		parseAndExecuteCommand(db)
	}
}

func parseAndExecuteCommand(db mdb.Storage) {
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	text = strings.TrimRight(text, "\n")

	cmd := strings.Split(text, " ")

	oneWordCommands := map[string]bool{"exit": true, "stop": true, "help": true}
	if !oneWordCommands[cmd[0]] && len(cmd) < 2 {
		printlnRed("Unknown command")
	}

	switch cmd[0] {
	case "set":
		setCommand(db, cmd[1:])
	case "s":
		setCommand(db, cmd[1:])
	case "get":
		getCommand(db, cmd[1:])
	case "g":
		getCommand(db, cmd[1:])
	case "exit":
		exitCommand(db)
	case "stop":
		exitCommand(db)
	case "help":
		helpCommand()
	default:
		printlnRed("Unknown command")
	}
}

func setCommand(db mdb.Storage, cmd []string) {
	if len(cmd) != 2 {
		printlnRed("Unknown command")
		return
	}
	db.Set(cmd[0], cmd[1])
	printlnYellow(fmt.Sprintf("\nSaved    %s=%s", cmd[0], cmd[1]))
}

func getCommand(db mdb.Storage, cmd []string) {
	if len(cmd) != 1 {
		printlnRed("Unknown command")
		return
	}

	value, exists := db.Get(cmd[0])
	printlnGreen(fmt.Sprintf("\nvalue='%s', exists=%v", value, exists))
}

func exitCommand(db mdb.Storage) {
	printlnYellow("\nShutting down...")
	db.Stop()
	os.Exit(0)
}

func helpCommand() {
	help := `
	Simple KV storage commands:

		set {key} {value}
		get {key}
		help
		exit
	`
	fmt.Println(help)
}
