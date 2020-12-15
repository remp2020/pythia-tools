FROM golang:1.13

RUN go get golang.org/x/tools/cmd/goimports

RUN mkdir -p /src/build

RUN mkdir -p /go/src/gitlab.com/remp/pythia/cmd/pythia_segments

WORKDIR /go/src/gitlab.com/remp/pythia/cmd/pythia_segments

COPY build /usr/local/bin/

RUN chmod +x /usr/local/bin/build

CMD ["build"]
