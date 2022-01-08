package mapreduce

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var errDummy = errors.New("dummy")

func init() {
	log.SetOutput(ioutil.Discard)
}

func TestFinish(t *testing.T) {
	var total uint32
	err := Finish(func() error {
		atomic.AddUint32(&total, 2)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 3)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 5)
		return nil
	})

	assert.Equal(t, uint32(10), atomic.LoadUint32(&total))
	assert.Nil(t, err)
}

func TestFinishNone(t *testing.T) {
	assert.Nil(t, Finish())
}

func TestFinishVoidNone(t *testing.T) {
	FinishVoid()
}

func TestFinishErr(t *testing.T) {
	var total uint32
	err := Finish(func() error {
		atomic.AddUint32(&total, 2)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 3)
		return errDummy
	}, func() error {
		atomic.AddUint32(&total, 5)
		return nil
	})

	assert.Equal(t, errDummy, err)
}

func TestFinishVoid(t *testing.T) {
	var total uint32
	FinishVoid(func() {
		atomic.AddUint32(&total, 2)
	}, func() {
		atomic.AddUint32(&total, 3)
	}, func() {
		atomic.AddUint32(&total, 5)
	})

	assert.Equal(t, uint32(10), atomic.LoadUint32(&total))
}

func TestMap(t *testing.T) {
	tests := []struct {
		name   string
		mapper MapFunc[int, int]
		expect int
	}{
		{
			name: "simple",
			mapper: func(v int, writer Writer[int]) {
				writer.Write(v * v)
			},
			expect: 30,
		},
		{
			name: "half",
			mapper: func(v int, writer Writer[int]) {
				if v%2 == 0 {
					return
				}
				writer.Write(v * v)
			},
			expect: 10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := Map(func(source chan<- int) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, WithWorkers(-1))

			var result int
			for v := range channel {
				result += v
			}

			assert.Equal(t, test.expect, result)
		})
	}
}

func TestMapReduce(t *testing.T) {
	tests := []struct {
		name        string
		mapper      MapperFunc[int, int]
		reducer     ReducerFunc[int, int]
		expectErr   error
		expectValue int
	}{
		{
			name:        "simple",
			expectErr:   nil,
			expectValue: 30,
		},
		{
			name: "cancel with error",
			mapper: func(v int, writer Writer[int], cancel func(error)) {
				if v%3 == 0 {
					cancel(errDummy)
				}
				writer.Write(v * v)
			},
			expectErr: errDummy,
		},
		{
			name: "cancel with nil",
			mapper: func(v int, writer Writer[int], cancel func(error)) {
				if v%3 == 0 {
					cancel(nil)
				}
				writer.Write(v * v)
			},
			expectErr:   ErrCancelWithNil,
			expectValue: 0,
		},
		{
			name: "cancel with more",
			reducer: func(pipe <-chan int, writer Writer[int], cancel func(error)) {
				var result int
				for item := range pipe {
					result += item
					if result > 10 {
						cancel(errDummy)
					}
				}
				writer.Write(result)
			},
			expectErr: errDummy,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.mapper == nil {
				test.mapper = func(item int, writer Writer[int], cancel func(error)) {
					v := item
					writer.Write(v * v)
				}
			}
			if test.reducer == nil {
				test.reducer = func(pipe <-chan int, writer Writer[int], cancel func(error)) {
					var result int
					for item := range pipe {
						result += item
					}
					writer.Write(result)
				}
			}
			value, err := MapReduce(func(source chan<- int) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, test.reducer, WithWorkers(runtime.NumCPU()))

			assert.Equal(t, test.expectErr, err)
			assert.Equal(t, test.expectValue, value)
		})
	}
}

func TestMapReduceWithReduerWriteMoreThanOnce(t *testing.T) {
	assert.Panics(t, func() {
		MapReduce(func(source chan<- int) {
			for i := 0; i < 10; i++ {
				source <- i
			}
		}, func(item int, writer Writer[string], cancel func(error)) {
			writer.Write(strconv.Itoa(item))
		}, func(pipe <-chan string, writer Writer[string], cancel func(error)) {
			drain(pipe)
			writer.Write("one")
			writer.Write("two")
		})
	})
}

func TestMapReduceVoid(t *testing.T) {
	var value uint32
	tests := []struct {
		name        string
		mapper      MapperFunc[int, int]
		reducer     VoidReducerFunc[int]
		expectValue uint32
		expectErr   error
	}{
		{
			name:        "simple",
			expectValue: 30,
			expectErr:   nil,
		},
		{
			name: "cancel with error",
			mapper: func(v int, writer Writer[int], cancel func(error)) {
				if v%3 == 0 {
					cancel(errDummy)
				}
				writer.Write(v * v)
			},
			expectErr: errDummy,
		},
		{
			name: "cancel with nil",
			mapper: func(v int, writer Writer[int], cancel func(error)) {
				if v%3 == 0 {
					cancel(nil)
				}
				writer.Write(v * v)
			},
			expectErr: ErrCancelWithNil,
		},
		{
			name: "cancel with more",
			reducer: func(pipe <-chan int, cancel func(error)) {
				for item := range pipe {
					result := atomic.AddUint32(&value, uint32(item))
					if result > 10 {
						cancel(errDummy)
					}
				}
			},
			expectErr: errDummy,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			atomic.StoreUint32(&value, 0)

			if test.mapper == nil {
				test.mapper = func(v int, writer Writer[int], cancel func(error)) {
					writer.Write(v * v)
				}
			}
			if test.reducer == nil {
				test.reducer = func(pipe <-chan int, cancel func(error)) {
					for item := range pipe {
						atomic.AddUint32(&value, uint32(item))
					}
				}
			}
			err := MapReduceVoid(func(source chan<- int) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, test.reducer)

			assert.Equal(t, test.expectErr, err)
			if err == nil {
				assert.Equal(t, test.expectValue, atomic.LoadUint32(&value))
			}
		})
	}
}

func TestMapReduceVoidWithDelay(t *testing.T) {
	var result []int
	err := MapReduceVoid(func(source chan<- int) {
		source <- 0
		source <- 1
	}, func(i int, writer Writer[int], cancel func(error)) {
		if i == 0 {
			time.Sleep(time.Millisecond * 50)
		}
		writer.Write(i)
	}, func(pipe <-chan int, cancel func(error)) {
		for item := range pipe {
			i := item
			result = append(result, i)
		}
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, 1, result[0])
	assert.Equal(t, 0, result[1])
}

func TestMapVoid(t *testing.T) {
	const tasks = 1000
	var count uint32
	MapVoid(func(source chan<- int) {
		for i := 0; i < tasks; i++ {
			source <- i
		}
	}, func(item int) {
		atomic.AddUint32(&count, 1)
	})

	assert.Equal(t, tasks, int(count))
}

func TestMapReducePanic(t *testing.T) {
	v, err := MapReduce(func(source chan<- int) {
		source <- 0
		source <- 1
	}, func(i int, writer Writer[int], cancel func(error)) {
		writer.Write(i)
	}, func(pipe <-chan int, writer Writer[int], cancel func(error)) {
		for range pipe {
			panic("panic")
		}
	})
	assert.Equal(t, 0, v)
	assert.NotNil(t, err)
	assert.Equal(t, "panic", err.Error())
}

func TestMapReduceVoidCancel(t *testing.T) {
	var result []int
	err := MapReduceVoid(func(source chan<- int) {
		source <- 0
		source <- 1
	}, func(i int, writer Writer[int], cancel func(error)) {
		if i == 1 {
			cancel(errors.New("anything"))
		}
		writer.Write(i)
	}, func(pipe <-chan int, cancel func(error)) {
		for item := range pipe {
			i := item
			result = append(result, i)
		}
	})
	assert.NotNil(t, err)
	assert.Equal(t, "anything", err.Error())
}

func TestMapReduceVoidCancelWithRemains(t *testing.T) {
	var done int32
	var result []int
	err := MapReduceVoid(func(source chan<- int) {
		for i := 0; i < defaultWorkers*2; i++ {
			source <- i
		}
		atomic.AddInt32(&done, 1)
	}, func(i int, writer Writer[int], cancel func(error)) {
		if i == defaultWorkers/2 {
			cancel(errors.New("anything"))
		}
		writer.Write(i)
	}, func(pipe <-chan int, cancel func(error)) {
		for item := range pipe {
			result = append(result, item)
		}
	})
	assert.NotNil(t, err)
	assert.Equal(t, "anything", err.Error())
	assert.Equal(t, int32(1), done)
}

func TestMapReduceWithoutReducerWrite(t *testing.T) {
	uids := []int{1, 2, 3}
	res, err := MapReduce(func(source chan<- int) {
		for _, uid := range uids {
			source <- uid
		}
	}, func(item int, writer Writer[int], cancel func(error)) {
		writer.Write(item)
	}, func(pipe <-chan int, writer Writer[int], cancel func(error)) {
		drain(pipe)
		// not calling writer.Write(...), should not panic
	})
	assert.Equal(t, ErrReduceNoOutput, err)
	assert.Equal(t, 0, res)
}

func TestMapReduceVoidPanicInReducer(t *testing.T) {
	const message = "foo"
	var done int32
	err := MapReduceVoid(func(source chan<- int) {
		for i := 0; i < defaultWorkers*2; i++ {
			source <- i
		}
		atomic.AddInt32(&done, 1)
	}, func(i int, writer Writer[int], cancel func(error)) {
		writer.Write(i)
	}, func(pipe <-chan int, cancel func(error)) {
		panic(message)
	}, WithWorkers(1))
	assert.NotNil(t, err)
	assert.Equal(t, message, err.Error())
	assert.Equal(t, int32(1), done)
}

func TestMapReduceWithContext(t *testing.T) {
	var done int32
	var result []int
	ctx, cancel := context.WithCancel(context.Background())
	err := MapReduceVoid(func(source chan<- int) {
		for i := 0; i < defaultWorkers*2; i++ {
			source <- i
		}
		atomic.AddInt32(&done, 1)
	}, func(i int, writer Writer[int], c func(error)) {
		if i == defaultWorkers/2 {
			cancel()
		}
		writer.Write(i)
	}, func(pipe <-chan int, cancel func(error)) {
		for item := range pipe {
			result = append(result, item)
		}
	}, WithContext(ctx))
	assert.NotNil(t, err)
	assert.Equal(t, ErrReduceNoOutput, err)
}

func BenchmarkMapReduce(b *testing.B) {
	b.ReportAllocs()

	mapper := func(v int64, writer Writer[int64], cancel func(error)) {
		writer.Write(v * v)
	}
	reducer := func(input <-chan int64, writer Writer[int64], cancel func(error)) {
		var result int64
		for v := range input {
			result += v
		}
		writer.Write(result)
	}

	for i := 0; i < b.N; i++ {
		MapReduce(func(input chan<- int64) {
			for j := 0; j < 2; j++ {
				input <- int64(j)
			}
		}, mapper, reducer)
	}
}
