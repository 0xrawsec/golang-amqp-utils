RELEASE="release"

all:
	$(MAKE) clean
	$(MAKE) init
	$(MAKE) compile

init:
	mkdir -p $(RELEASE)
	mkdir -p $(RELEASE)/linux
	mkdir -p $(RELEASE)/windows
	mkdir -p $(RELEASE)/darwin

compile:linux windows darwin

linux:
	GOARCH=386 GOOS=linux go build -o $(RELEASE)/linux/print-consumer-386 print-consumer.go
	GOARCH=amd64 GOOS=linux go build -o $(RELEASE)/linux/print-consumer-amd64 print-consumer.go
	go install print-consumer.go

windows:
	GOARCH=386 GOOS=windows go build -o $(RELEASE)/windows/print-consumer-386.exe print-consumer.go
	GOARCH=amd64 GOOS=windows go build -o $(RELEASE)/windows/print-consumer-amd64.exe print-consumer.go

darwin:
	GOARCH=386 GOOS=darwin go build -o $(RELEASE)/darwin/print-consumer-386 print-consumer.go
	GOARCH=amd64 GOOS=darwin go build -o $(RELEASE)/darwin/print-consumer-amd64 print-consumer.go

clean:
	rm -rf $(RELEASE)/*
