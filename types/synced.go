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
type SyncMap struct {
	m map[string]any
	mtx sync.Mutex
}

func NewSyncMap() *SyncMap {
	return &SyncMap{m: map[string]any{}}
}

func (sm *SyncMap) Set(k string, v any) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	sm.m[k] = v
}

func (sm *SyncMap) Get(k string) (any, bool) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	v, ok := sm.m[k]
	return v, ok
}

func (sm *SyncMap) Val(k string) any {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	v, ok := sm.m[k]
	if !ok {
		panic("(SyncMap) Trying to return value for non-existing key " + k)
	}
	return v
}

func (sm *SyncMap) Del(k string) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	delete(sm.m, k)
}

func (sm *SyncMap) Len() int {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	return len(sm.m)
}

func (sm *SyncMap) Apply(f func(k string, v any)) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	for k, v := range sm.m {
		f(k, v)
	}
}
