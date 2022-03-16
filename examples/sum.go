package main

import (
	"fmt"
	"log"

	"github.com/kevwan/mapreduce"
)

func main() {
	val, err := mapreduce.MapReduce(func(source chan<- int) {
		for i := 0; i < 10; i++ {
			source <- i
		}
	}, func(item int, writer mapreduce.Writer[int], cancel func(error)) {
		writer.Write(item * item)
	}, func(pipe <-chan int, writer mapreduce.Writer[int], cancel func(error)) {
		var sum int
		for i := range pipe {
			sum += i
		}
		writer.Write(sum)
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("result:", val)
}
