package main

import (
	"fmt"
	"os"
	"time"

	"github.com/yassinezakhama/mapreduce/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
