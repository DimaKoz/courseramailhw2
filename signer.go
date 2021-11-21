package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	var in = make(chan interface{})

	for i, jobFunc := range jobs {
		wg.Add(1)
		out := make(chan interface{})
		go processJob(wg, i, jobFunc, in, out)
		in = out

	}

	wg.Wait()
}

func processJob(wg *sync.WaitGroup, i int, passedJob job, in chan interface{}, out chan interface{}) {
	defer func() {
		fmt.Printf("processJob(%v) done\n", i)
		close(out)
		wg.Done()
	}()
	fmt.Printf("processJob(%v) started\n", i)
	passedJob(in, out)
}

func MultiHash(in, out chan interface{}) {
	const TH int = 6
	wg := &sync.WaitGroup{}

	for i := range in {
		wg.Add(1)
		go processMultiHash(fmt.Sprintf("%v", i), out, TH, wg)
		runtime.Gosched()
	}
	wg.Wait()
}

func processMultiHash(in string, out chan interface{}, th int, wg *sync.WaitGroup) {
	defer wg.Done()

	m := &sync.Mutex{}
	w := &sync.WaitGroup{}
	combinedChunks := make([]string, th)

	for i := 0; i < th; i++ {
		w.Add(1)
		data := fmt.Sprintf("%v%v", i, in)

		go func(acc []string, index int, data string, wg *sync.WaitGroup, lock *sync.Mutex) {
			defer wg.Done()
			data = DataSignerCrc32(data)

			lock.Lock()
			acc[index] = data
			lock.Unlock()
		}(combinedChunks, i, data, w, m)

	}

	w.Wait()

	out <- strings.Join(combinedChunks, "")
}

func CombineResults(in, out chan interface{}) {
	var result []string

	for i := range in {
		result = append(result, i.(string))
	}

	sort.Strings(result)
	out <- strings.Join(result, "_")
}

func SingleHash(in, out chan interface{}) {
	w := &sync.WaitGroup{}
	m := &sync.Mutex{}

	for i := range in {
		w.Add(1)
		go processSingleHash(i, out, w, m)
		runtime.Gosched()
	}
	w.Wait()
}

func processSingleHash(in interface{}, out chan interface{}, wg *sync.WaitGroup, lock *sync.Mutex) {
	defer wg.Done()
	crc32Chan := make(chan string)
	crc32Md5Chan := make(chan string)

	data := fmt.Sprintf("%v", in)

	lock.Lock()
	md5Data := DataSignerMd5(data)
	lock.Unlock()

	go asyncDataSignerCrc32(data, crc32Chan)
	go asyncDataSignerCrc32(md5Data, crc32Md5Chan)
	crc32Data := <-crc32Chan
	crc32Md5Data := <-crc32Md5Chan
	out <- strings.Join([]string{crc32Data, crc32Md5Data}, "~")
}

func asyncDataSignerCrc32(data string, out chan string) {
	out <- DataSignerCrc32(data)
}
