

.PHONY: all buid server benchmark markbench

all: build

build: server benchmark markbench

server:
	go build

benchmark:
	cd benchmark && go build

markbench:
	cd markbench && go build
