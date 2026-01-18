.PHONY: build test clean release

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

build:
	go build $(LDFLAGS) -o pgdump-offline .

test:
	go test -v ./...

test-local:
	@echo "Testing with local PostgreSQL data..."
	@for v in 12 13 14 15 16 17; do \
		if [ -d "/tmp/pg_test_v$$v/data" ]; then \
			echo "=== PostgreSQL $$v ==="; \
			PGDUMP_TESTDATA=/tmp/pg_test_v$$v/data go test -v ./... -run TestDumpDataDir; \
		fi; \
	done

clean:
	rm -f pgdump-offline pgdump-offline-* dist/*

dist: clean
	mkdir -p dist
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o dist/pgdump-offline-linux-amd64 .
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o dist/pgdump-offline-linux-arm64 .
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o dist/pgdump-offline-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o dist/pgdump-offline-darwin-arm64 .
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o dist/pgdump-offline-windows-amd64.exe .
	cd dist && sha256sum * > checksums.txt

release: dist
	@echo "Release $(VERSION) ready in dist/"
