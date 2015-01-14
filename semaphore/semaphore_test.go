package semaphore

import (
	"testing"
	"time"
)

func TestSemaphore(t *testing.T) {
	numbers := []int{}
	negatives := []int{}
	for i := 1; i < 1000; i++ {
		numbers = append(numbers, i)
		negatives = append(negatives, -i)
	}
	numbers = append(numbers, negatives...)
	semaphore := New()
	type Ent struct {
		done bool
		num  int
	}
	numChan := make(chan Ent)

	// Fork off routines to publish numbers
	goRoutineCount := 10
	for i := 0; i < goRoutineCount; i++ {
		go func() {
			for {
				ent := <-numChan
				if ent.done {
					return
				}
				semaphore.Add(ent.num)
			}
		}()
	}

	//detect allGood
	allDone := make(chan bool)
	go func() {
		semaphore.Wait()
		allDone <- true
	}()

	// send the numbers off
	for _, n := range numbers {
		numChan <- Ent{false, n}
	}

	// send the end markers
	for i := 0; i < goRoutineCount; i++ {
		numChan <- Ent{true, 0}
	}

	// wait for allGood
	duration, _ := time.ParseDuration("100ms")
	select {
	case <-allDone:
	case <-time.After(duration):
		t.Fatalf("Did not get allGood message")
	}
}
