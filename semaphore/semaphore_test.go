package semaphore

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	semaphore := New()
	if semaphore.count != 0 {
		t.Fatalf("New semaphore would block!")
	}
}

func TestAddIncrementsCount(t *testing.T) {
	semaphore := New()
	semaphore.Add(1)
	if semaphore.count != 1 {
		t.Fatalf("After Add(1) applied, found semaphore with count not equal to 1")
	}
}

func TestDone(t *testing.T) {
	semaphore := New()
	semaphore.Add(1)
	semaphore.Done()
	if semaphore.count != 0 {
		t.Fatalf("After Add(1), followed by Done(), count not returned to 0")
	}
}

func TestReset(t *testing.T) {
	semaphore := New()
	semaphore.Add(100)
	done := make(chan int)
	go func() {
		semaphore.Reset()
		semaphore.Wait() // After a reset the count should be zero, so Wait() will return immediately.
		done <- 1
	}()

	// wait for allGood
	duration := time.Millisecond * 100
	select {
	case <-done:
	case <-time.After(duration):
		t.Fatalf("Did not get done message")
	}
}

func TestConcurrent(t *testing.T) {
	// Test description:
	//   * Creates numPositives positive numbers, and numPositives negative numbers. The sum of the 2*numPositive ints
	//     will sum to zero. So if you Add() all the numbers to the Semaphore, the object should end up with a count of
	//     0.
	//   * Spin up goRoutineCount go-routines. Each ones of these will push numPerThread numbers to the semaphore using
	//     Add().
	//   * We want to make sure that the Semaphore ends up correctly at count zero after all the numbers are pushed.
	//     Hence we Wait() on a separate thread to see when that condition happens.
	//   * If count never reaches zero, Wait will never return, and test will time-out.
	numPositives := 1000
	goRoutineCount := 10
	numPerThread := 2 * numPositives / goRoutineCount

	numbers := []int{}
	negatives := []int{}
	for i := 1; i < numPositives; i++ {
		numbers = append(numbers, i)
		negatives = append(negatives, -i)
	}
	numbers = append(numbers, negatives...)
	semaphore := New()
	numChan := make(chan int)

	// Fork off routines to publish numbers
	for i := 0; i < goRoutineCount; i++ {
		go func() {
			for j := 0; j < numPerThread; j++ {
				ent := <-numChan
				semaphore.Add(ent)
			}
		}()
	}

	//detect allGood
	done := make(chan bool)
	go func() {
		semaphore.Wait()
		done <- true
	}()

	// send the numbers off
	for _, n := range numbers {
		numChan <- n
	}

	// wait for done
	duration := time.Millisecond * 100
	select {
	case <-done:
	case <-time.After(duration):
		t.Fatalf("Did not get done message")
	}
}
