test-demo:
	go build -race -tags netgo && ./promxy --rules.alertbackfill --log-level=debug --http.shutdown-delay=0s --config=demo.conf --web.external-url=http://localhost:8082/promxy


test-local:
	go build -race -tags netgo && ./promxy --http.shutdown-delay=0s --config=local_test.conf --web.external-url=http://localhost:8082/promxy --log-level=debug

version:
	go build -race -tags netgo && ./promxy --version
