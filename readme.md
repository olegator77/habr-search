# Habrahabr livesearch demo application

This is small proof-of-concept application, which implements live full text search by popular russian site habrahabr.ru.

The [Reindexer](https://github.com/Restream/reindexer) in-memory DB is used as storage and full text search engine. 

Current dataset contains about 5GB of data: 170K aricles with 6M commentaries.

The frontend for project is written with vue.js and located in [repository](https://github.com/igtulm/reindex-search-ui)

![](https://habrastorage.org/webt/ob/eo/lq/obeolqk0_j5nu0junamkmqwdltq.gif)


## Build & install

1. Install & build [reindexers dependencies](https://github.com/Restream/reindexer/blob/master/readme.md#installation)
2. Install habr-search
```bash
    go get github.com/olegator77/habr-search
```

## Usage

1. Import (download) dataset from habrahabr.ru
```
    habr-search import -startid 1 -finishid 355000 -dumppath <path-to-store-data> -webrootpath <path to store images>
```

This step is very long, and can take about 8+ hours to download all data, and requires about 5GB of free disk space. You can reduce time 
and size by decrease ID range, e.g. set startid to 350000.

2. Load imported data to Reindexer

```
habr-search load -dumppath <path-to-store-data> -webrootpath <path to store images>
```

This step takes about 5 minutes for all dataset

3. Install and build frontend 

- Follow the [instructions](https://github.com/igtulm/reindex-search-ui)
- Copy built fronened to `webrootpath` folder

4. Run service

```
    habr-search run -dumppath <path-to-store-data> -webrootpath <path of webroot>
```

Open http://127.0.0.1:8881 in your browser.

