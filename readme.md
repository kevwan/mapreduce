<img align="right" width="150px" src="https://raw.githubusercontent.com/zeromicro/zero-doc/main/doc/images/go-zero.png">

# mapreduce

English | [简体中文](readme-cn.md)

[![Go](https://github.com/kevwan/mapreduce/workflows/Go/badge.svg?branch=main)](https://github.com/kevwan/mapreduce/actions)
[![codecov](https://codecov.io/gh/kevwan/mapreduce/branch/main/graph/badge.svg)](https://codecov.io/gh/kevwan/mapreduce)
[![Go Report Card](https://goreportcard.com/badge/github.com/kevwan/mapreduce)](https://goreportcard.com/report/github.com/kevwan/mapreduce)
[![Release](https://img.shields.io/github/v/release/kevwan/mapreduce.svg?style=flat-square)](https://github.com/kevwan/mapreduce)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why we have this repo?

`mapreduce` is part of [go-zero](https://github.com/zeromicro/go-zero), but a few people asked if mapreduce can be used separately. But I recommend you to use `go-zero` for many more features.

## Design ideas

Let's try to put ourselves in the author's shoes and sort out the possible business scenarios for the concurrency tool:

1. querying product details: supporting concurrent calls to multiple services to combine product attributes, and supporting call errors that can be ended immediately.
2. automatic recommendation of user card coupons on product details page: support concurrently verifying card coupons, automatically rejecting them if they fail, and returning all of them.

The above is actually processing the input data and finally outputting the cleaned data. There is a very classic asynchronous pattern for data processing: the producer-consumer pattern. So we can abstract the life cycle of data batch processing, which can be roughly divided into three phases.

![](https://cdn.learnku.com/uploads/images/202112/27/73865/aGKm4Lgw5r.png!large)

1. data production generate
2. data processing mapper
3. data aggregation reducer

Data producing is an indispensable stage, data processing and data aggregation are optional stages, data producing and processing support concurrent calls, data aggregation is basically a pure memory operation, so a single concurrent process can do it.

Since different stages of data processing are performed by different goroutines, it is natural to consider the use of channel to achieve communication between goroutines.

![](https://cdn.learnku.com/uploads/images/202112/27/73865/qoucKqA8jH.png!large)

How can I terminate the process at any time?

It's simple, just listen to a global end channel or the given context in the goroutine.

## A simple example

Calculate the sum of squares, simulating the concurrency.

```go
package main

import (
    "fmt"
    "log"

    "github.com/kevwan/mapreduce"
)

func main() {
    val, err := mapreduce.MapReduce(func(source chan<- interface{}) {
        // generator
        for i := 0; i < 10; i++ {
            source <- i
        }
    }, func(item interface{}, writer mapreduce.Writer, cancel func(error)) {
        // mapper
        i := item.(int)
        writer.Write(i * i)
    }, func(pipe <-chan interface{}, writer mapreduce.Writer, cancel func(error)) {
        // reducer
        var sum int
        for i := range pipe {
            sum += i.(int)
        }
        writer.Write(sum)
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("result:", val)
}
```

## References

go-zero: [https://github.com/zeromicro/go-zero](https://github.com/zeromicro/go-zero)

## Give a Star! ⭐

If you like or are using this project to learn or start your solution, please give it a star. Thanks!