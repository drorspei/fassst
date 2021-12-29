.PHONY: build fbuild clean test bench tb accept

build: test
	go build -o bin/

fbuild: 
	go build -o bin/

clean: 
	rm -rf bin
	go clean -testcache

test: 
	go test -v ./pkg/fs/... -timeout 5m

bench: 
	go test -v -benchmem -run=^$$ -bench ^.*$$ ./pkg/fs/...

tb: test bench
	echo "done."
