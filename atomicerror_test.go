package mapreduce

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

var errAtomicDummy = errors.New("hello")

func TestAtomicError(t *testing.T) {
	var err AtomicError
	err.Set(errAtomicDummy)
	assert.Equal(t, errAtomicDummy, err.Load())
}

func TestAtomicErrorSetNil(t *testing.T) {
	var (
		errNil error
		err    AtomicError
	)
	err.Set(errNil)
	assert.Equal(t, errNil, err.Load())
}

func TestAtomicErrorNil(t *testing.T) {
	var err AtomicError
	assert.Nil(t, err.Load())
}

func BenchmarkAtomicError(b *testing.B) {
	var aerr AtomicError
	wg := sync.WaitGroup{}

	b.Run("Load", func(b *testing.B) {
		var done uint32
		go func() {
			for {
				if atomic.LoadUint32(&done) != 0 {
					break
				}
				wg.Add(1)
				go func() {
					aerr.Set(errAtomicDummy)
					wg.Done()
				}()
			}
		}()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = aerr.Load()
		}
		b.StopTimer()
		atomic.StoreUint32(&done, 1)
		wg.Wait()
	})
	b.Run("Set", func(b *testing.B) {
		var done uint32
		go func() {
			for {
				if atomic.LoadUint32(&done) != 0 {
					break
				}
				wg.Add(1)
				go func() {
					_ = aerr.Load()
					wg.Done()
				}()
			}
		}()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			aerr.Set(errAtomicDummy)
		}
		b.StopTimer()
		atomic.StoreUint32(&done, 1)
		wg.Wait()
	})
}
