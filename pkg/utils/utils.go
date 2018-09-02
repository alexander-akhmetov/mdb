package utils

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const filePermissions = 0600
const pidFileName = "mdb.pid"

// GetKeyValueFromString returns key and value from string
func GetKeyValueFromString(line string) (string, string) {
	splitted := strings.SplitN(line, ";", 2)
	if len(splitted) != 2 {
		log.Panicln("Wrong line: ", line)
	}
	return splitted[0], strings.TrimRight(splitted[1], "\n")
}

// TrimKey removes key and ";" prefix from line
// to get value only
func TrimKey(key string, line string) string {
	return strings.TrimPrefix(line, fmt.Sprintf("%s;", key))
}

// FindLineByKeyInFile returns the last line which starts with given key and boolean indicator that line has been found
// if it's false - line has not been found
func FindLineByKeyInFile(filename string, key string) (string, bool) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	resultLine := ""
	found := false

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, fmt.Sprintf("%s;", key)) {
			resultLine = line
			found = true
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return resultLine, found
}

// AppendToFile appends given string to a file with given filename
func AppendToFile(filename string, appendString string) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	_, err = file.WriteString(appendString)
	if err != nil {
		log.Panic(err)
	}
}

// RecreateFile removes old file and creates a new one
func RecreateFile(filename string) {
	os.Remove(filename)
	CreateFileIfNotExists(filename)
}

// CreateFileIfNotExists creates file and all dirs if it doesn't exist
func CreateFileIfNotExists(filename string) {
	dir, _ := filepath.Split(filename)
	CreateDir(dir)

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, filePermissions)
	}
}

// CreateDir creates dir like `mkdir -p`
func CreateDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
}

// GetFileSize returns file size
func GetFileSize(filename string) int64 {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, filePermissions)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		log.Panic(err)
	}
	return fi.Size()
}

// ReadLineByOffset reads line from file by given offset
func ReadLineByOffset(filename string, offset int64) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		log.Panic(err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	return scanner.Text()
}

// StartFileDB creates temporary .pid file to lock file usage
func StartFileDB() {
	CheckAndCreatePIDFile(pidFileName)
	AppendToFile(pidFileName, fmt.Sprintf("%v", os.Getpid()))
}

// CheckAndCreatePIDFile checks and creates .pid file if it does not exist
// if it exists, it will panic, because if you use it only one
// instance of the DB must be started at the same time
func CheckAndCreatePIDFile(path string) {
	if _, err := os.Stat(path); err == nil {
		log.Panicf("Can't start the database: %s file already exists!", path)
	}
	CreateFileIfNotExists(path)
}

// StopFileDB removes temporary .pid file
func StopFileDB() {
	RemovePIDFile(pidFileName)
}

// RemovePIDFile removes .pid file
func RemovePIDFile(path string) {
	err := os.Remove(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("[WARN] .pid file does not exist! Can't remove it")
		} else {
			log.Panicln("Can't stop the DB properly, can't remove the .pid file: ", err)
		}
	}
}

// FileInfo is a struct with file information
type FileInfo struct {
	Name string
	Size int64
}

// ListFilesOrdered returns filenames ordered by their name (descending)
// files *MUST* be with integer names
func ListFilesOrdered(dir string, filterBySuffix string) []FileInfo {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Panic(err)
	}

	sort.Slice(files, func(i, j int) bool {
		filei, _ := strconv.Atoi(strings.Split(files[i].Name(), ".")[0])
		filej, _ := strconv.Atoi(strings.Split(files[j].Name(), ".")[0])
		return filei > filej
	})

	filenames := []FileInfo{}
	for _, file := range files {
		filename := file.Name()
		if filterBySuffix == "" || strings.HasSuffix(filename, filterBySuffix) {
			f := FileInfo{
				Name: filepath.Join(dir, filename),
				Size: file.Size(),
			}
			filenames = append(filenames, f)
		}
	}

	return filenames
}
