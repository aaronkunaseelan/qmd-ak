package main

import (
	fetch "github.com/05802/qmd2/capture"
	"os"
)

func main() { os.Exit(fetch.Main(os.Args[1:])) }
