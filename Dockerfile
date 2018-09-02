FROM golang:1.10.3-alpine3.8 as builder

COPY . /go/src/github.com/alexander-akhmetov/mdb/

WORKDIR /go/src/github.com/alexander-akhmetov/mdb/

RUN go build -o mdb cmd/*.go



FROM golang:1.10.3-alpine3.8

WORKDIR /app/

COPY --from=builder /go/src/github.com/alexander-akhmetov/mdb/mdb /app/

CMD ["/app/mdb"]
