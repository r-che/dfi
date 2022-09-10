package types

import "sync"

/*
 * Currently is not used
// Synchronized counter
type intCounter struct {
	v int
	mtx sync.Mutex
}

func NewIntCounter() *intCounter {
	return &intCounter{}
}

func (ic *intCounter) Set(v int) {
	ic.mtx.Lock()
	defer ic.mtx.Unlock()
	ic.v = v
}

func (ic *intCounter) Add(v int) {
	ic.mtx.Lock()
	defer ic.mtx.Unlock()
	ic.v += v
}

func (ic *intCounter) Inc() {
	ic.mtx.Lock()
	defer ic.mtx.Unlock()
	ic.v++
}

func (ic *intCounter) Val() int {
	ic.mtx.Lock()
	defer ic.mtx.Unlock()
	return ic.v
}
*/

// Synchronized map
type syncMap struct {
	m map[string]any
	mtx sync.Mutex
}

func NewSyncMap() *syncMap {
	return &syncMap{m: map[string]any{}}
}

func (sm *syncMap) Set(k string, v any) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	sm.m[k] = v
}

func (sm *syncMap) Get(k string) (any, bool) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	v, ok := sm.m[k]
	return v, ok
}

func (sm *syncMap) Del(k string) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	delete(sm.m, k)
}

func (sm *syncMap) Len() int {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	return len(sm.m)
}
