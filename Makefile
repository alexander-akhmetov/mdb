test:
	go test -timeout 30s ./...

build:
	go build -o mdb cmd/*.go

run:
	go run cmd/*.go -i


run-docker-performance:
	docker-compose -f docker-compose.performance.yml up --build

