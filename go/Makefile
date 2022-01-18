.PHONY: build clean test testv bench tb

build: test
	go build -o bin/

clean: 
	rm -rf bin
	go clean -testcache

test: 
	go test ./pkg/fs/... -timeout 5m -cover
	go test ./pkg/fassst/... -timeout 5m -cover

testv: 
	go test -v ./pkg/fs/... -timeout 5m -cover
	go test -v ./pkg/fassst/... -timeout 5m -cover

bench: 
	go test -benchmem -run=^$$ -bench ^.*$$ ./pkg/fs/...

tb: test bench
	echo "done."
