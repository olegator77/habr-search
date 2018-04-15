package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var repo Repo

var numParallelImports = 5

var httpAddr = flag.String("httpaddr", ":8881", "HTTP listen address:port")
var importStartID = flag.Int("startid", 1, "Import post start ID")
var importFinishID = flag.Int("finishid", 360000, "Import post finish ID")
var dumpPostsPath = flag.String("dumppath", "/tmp/habrimport", "Path, where imported posts are stored")
var webRootPath = flag.String("webrootpath", "/Users/ogerasimov/habrdemo", "Path, where HTML static data is hosted")

func dload(wg *sync.WaitGroup, dlChannel chan int) {
	for i := range dlChannel {
		habrPost, imgData, err := DownloadPost(i)
		if habrPost != nil && err == nil {
			fmt.Printf("ID %d (at %s) - %s, %d comments, %d views, %d likes, %d bookmarks\n",
				i, time.Unix(habrPost.Time, 0).Format("02.01.06"), habrPost.Title, len(habrPost.Comments), habrPost.Views, habrPost.Likes, habrPost.Favorites)
			data, _ := json.Marshal(habrPost)
			ioutil.WriteFile(fmt.Sprintf("%s/%d.json", *dumpPostsPath, i), data, 0666)

			if imgData != nil {
				ioutil.WriteFile(fmt.Sprintf("%s/%d.jpeg", filepath.Join(*dumpPostsPath, "images"), i), imgData, 0666)

			}
		} else {
			fmt.Printf("ID %d - error %s\n", i, err.Error())
		}
	}
	wg.Done()
}

func downloadFiles() {
	dlChannel := make(chan int)
	wg := sync.WaitGroup{}
	os.Mkdir(*dumpPostsPath, os.ModePerm)
	os.Mkdir(filepath.Join(*dumpPostsPath, "images"), os.ModePerm)

	for i := 0; i < numParallelImports; i++ {
		wg.Add(1)
		go dload(&wg, dlChannel)
	}
	for i := *importStartID; i < *importFinishID; i++ {
		dlChannel <- i
	}

	close(dlChannel)
	wg.Wait()
}

func usage() {
	fmt.Printf(
		"usage: %s <command> [<args>]\n"+
			"The available commands are:\n"+
			" run       Run HTTP API server\n"+
			" import    Import posts from habrhabr site\n"+
			" load      Load imported data to reindexer\n",
		os.Args[0],
	)
	os.Exit(-1)

}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	flag.CommandLine.Parse(os.Args[2:])

	switch os.Args[1] {
	case "run":
		repo.Init()
		StartHTTP(*httpAddr)
	case "import":
		downloadFiles()
	case "load":
		os.RemoveAll("/tmp/reindex")
		repo.Init()
		repo.RestoreFromFiles(*dumpPostsPath)
		repo.Done()
	default:
		usage()
	}

}
