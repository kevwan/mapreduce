package main

import (
	"fmt"
	"log"

	"github.com/kevwan/mapreduce/v1"
)

func main() {
	val, err := mapreduce.MapReduce(func(source chan<- interface{}) {
		for i := 0; i < 10; i++ {
			source <- i
		}
	}, func(item interface{}, writer mapreduce.Writer, cancel func(error)) {
		i := item.(int)
		writer.Write(i * i)
	}, func(pipe <-chan interface{}, writer mapreduce.Writer, cancel func(error)) {
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
