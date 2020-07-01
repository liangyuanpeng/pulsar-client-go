fmt:
	@go fmt $(CURDIR)/...
	@go mod tidy

builda: 
	@echo "begin build bin"
	rm -rf build
	@GOOS=linux GOARCH=amd64 go build -tags netgo -o build/consumer consumer.go

#--ldflags "-extldflags -static" 

buildc: builda
	@echo "begin building docker image..."
	@docker build -t lanpulsar/consumer:0.2 .