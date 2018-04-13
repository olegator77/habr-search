package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var repo Repo
var process = 4
var wg = sync.WaitGroup{}

const dumpPostsPath = "/root/hposts"

func dload(ofs int) {
	for i := 141000 + ofs; i < 200000; i = i + process {

		habrPost, err := DownloadPost(i /*353210*/)
		if habrPost != nil && err == nil {
			fmt.Printf("ID %d (at %s) - %s, %d comments\n", i, time.Unix(habrPost.Time, 0).Format("02.01.06"), habrPost.Title, len(habrPost.Comments))
			data, _ := json.Marshal(habrPost)
			ioutil.WriteFile(fmt.Sprintf("%s/%d.json", dumpPostsPath, i), data, 0666)

		} else {
			// fmt.Printf("ID %d - error %s\n", i, err.Error())
		}
	}
	wg.Done()

}

func downloadFiles() {
	wg := sync.WaitGroup{}
	os.Mkdir(dumpPostsPath, os.ModePerm)

	for i := 0; i < process; i++ {
		wg.Add(1)
		go dload(i)
	}
	wg.Wait()
}

func main() {

	repo.Init()
	repo.RestoreFromFiles(dumpPostsPath)
	repo.Warmup()
	StartHTTP()
}
