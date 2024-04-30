run: build
	@./bin/zendb
build:
	@go build -o bin/zendb .
