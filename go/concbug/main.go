package main

import (
	"fmt"
	"time"
)

func main() {
	for good := 0; good < 1000; good++ {
		if run2() != 1 {
			panic(fmt.Errorf("bummer (%d)", good))
		}
		fmt.Print("\r", good, "\r")
	}
	fmt.Println("Good")
}

func run() int {
	drainChan := make(chan int, 30)
	nameChan := make(chan int, 30)

	results := 0

	// foo
	go func() {
		drainChan <- 1
		nameChan <- 1
	}()

	// drain go routine
	go func() {
		<-drainChan
		results += 1
	}()

	// main loop
	<-nameChan

	for len(drainChan) > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	return results
}

func run1() int {
	drainChan := make(chan int, 30)
	nameChan := make(chan int, 30)

	results := 0

	// foo
	go func() {
		drainChan <- 1
		nameChan <- 1
	}()

	// drain go routine
	var eating bool
	go func() {
		_, eating = <-drainChan
		results += 1
		eating = false
	}()

	// main loop
	<-nameChan

	// fmt.Println("1", eating)
	for len(drainChan) > 0 || eating {
		time.Sleep(100 * time.Millisecond)
	}

	// fmt.Println("ending")
	return results
}

func run2() int {
	drainChan := make(chan int, 30)
	nameChan := make(chan int, 30)

	results := 0

	// foo
	go func() {
		drainChan <- 1
		nameChan <- 1
	}()

	// drain go routine
	var eating bool
	go func() {
		_, eating = <-drainChan
		// time.Sleep(200 * time.Millisecond)
		results += 1
		eating = false
	}()

	// main loop
	var done bool
	for !done || (len(drainChan) > 0 || eating) {
		select {
		case <-nameChan:
			done = true
		default:
			time.Sleep(100*time.Millisecond)
		}
	}

	// fmt.Println("ending")
	return results
}
