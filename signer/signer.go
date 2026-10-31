package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	var wg sync.WaitGroup
	in := make(chan interface{})
	close(in)

	for _, currentJob := range jobs {
		out := make(chan interface{})
		wg.Add(1)

		go func(currentJob job, in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			currentJob(in, out)
		}(currentJob, in, out)

		in = out
	}

	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	sem := make(chan struct{}, 1)
	var wg sync.WaitGroup

	for val := range in {
		wg.Add(1)
		go func(val interface{}) {
			defer wg.Done()

			data := fmt.Sprintf("%v", val)

			crc32Md5HashCh := make(chan string)
			defer close(crc32Md5HashCh)
			crc32HashCh := make(chan string)
			defer close(crc32HashCh)

			go func() {
				sem <- struct{}{}
				md5Hash := DataSignerMd5(data)
				<-sem
				crc32Md5HashCh <- DataSignerCrc32(md5Hash)
			}()

			go func() {
				crc32Hash := DataSignerCrc32(data)
				crc32HashCh <- crc32Hash
			}()

			out <- <-crc32HashCh + "~" + <-crc32Md5HashCh
		}(val)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := sync.WaitGroup{}

	for val := range in {
		wg.Add(1)

		go func(val interface{}) {
			wgw := sync.WaitGroup{}
			defer wg.Done()

			results := [6]string{}

			for i := 0; i < 6; i++ {
				wgw.Add(1)

				go func(i int) {
					defer wgw.Done()
					data := fmt.Sprintf("%v", val)
					dataToHash := fmt.Sprintf("%v", i) + data
					crc32Hash := DataSignerCrc32(dataToHash)
					results[i] = crc32Hash
				}(i)
			}

			wgw.Wait()
			out <- strings.Join(results[:], "")

		}(val)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var results []string

	for val := range in {
		results = append(results, val.(string))
	}

	sort.Strings(results)
	out <- strings.Join(results, "_")
}
