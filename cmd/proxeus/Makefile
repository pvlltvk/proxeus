test-demo:
	go build -race -tags netgo && ./proxeus --rules.alertbackfill --log-level=debug --http.shutdown-delay=0s --config=demo.conf --web.external-url=http://localhost:8082/proxeus


test-local:
	go build -race -tags netgo && ./proxeus --http.shutdown-delay=0s --config=local_test.conf --web.external-url=http://localhost:8082/proxeus --log-level=debug

version:
	go build -race -tags netgo && ./proxeus --version
