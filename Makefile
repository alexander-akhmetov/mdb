test:
	go test -timeout 30s ./...

build:
	go build -o mdb cmd/*.go

run:
	go run cmd/*.go -i


run-docker-perfomance:
	docker-compose -f docker-compose.perfomance.yml up --build

