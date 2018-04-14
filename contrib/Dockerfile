FROM ubuntu:16.04

RUN apt-get -y update && \
    apt-get install -y curl wget git && \
    curl https://raw.githubusercontent.com/Restream/reindexer/master/dependencies.sh | bash -s

RUN wget https://redirector.gvt1.com/edgedl/go/go1.9.3.linux-amd64.tar.gz -nv -O - | tar xzv -C /usr/local
ENV GOROOT=/usr/local/go
ENV GOPATH=/go
ENV PATH=$PATH:$GOROOT/bin:$GOPATH/bin

RUN go get github.com/buaazp/fasthttprouter && \
    go get github.com/PuerkitoBio/goquery && \
    go get github.com/nfnt/resize
