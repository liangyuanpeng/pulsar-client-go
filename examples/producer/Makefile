fmt:
	@go fmt $(CURDIR)/...
	@go mod tidy

builda: 
	@echo "begin build bin"
	@rm -rf build
	@GOOS=linux GOARCH=amd64 go build -tags netgo -o build/producer producer.go

buildc: builda
	@echo "begin building docker image..."
	@docker build -t lanpulsar/producer:0.2 .