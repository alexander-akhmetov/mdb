package main

import (
	"fmt"
	"math/rand"
)

const colorRed = "\x1b[31;1m"
const colorGreen = "\x1b[32;1m"
const colorYellow = "\x1b[33;1m"
const colorNeutral = "\x1b[0m"

func printlnGreen(str string) {
	fmt.Println(colorGreen, str, colorNeutral)
}

func printlnRed(str string) {
	fmt.Println(colorRed, str, colorNeutral)
}

func printlnYellow(str string) {
	fmt.Println(colorYellow, str, colorNeutral)
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
