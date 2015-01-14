/*
   A semaphore that doesn't trip up the race detector like WaitGroup does.
*/
package semaphore

import (
	"sync"
)

type Semaphore struct {
	cond  *sync.Cond
	lock  sync.Mutex
	count int
}

func New() *Semaphore {
	s := &Semaphore{}
	s.cond = sync.NewCond(&s.lock)
	return s
}

func (sm *Semaphore) Reset() {
	sm.count = 0
	sm.cond.Broadcast()
}

func (sm *Semaphore) Add(i int) {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	sm.count += i
	if sm.count <= 0 {
		sm.cond.Broadcast()
	}
}

func (sm *Semaphore) Done() {
	sm.Add(-1)
}

func (sm *Semaphore) Wait() {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	for sm.count <= 0 {
		sm.cond.Wait()
	}
}
