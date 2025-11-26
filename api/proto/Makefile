# Require exactly one argument after "gen"
# make gen ./user/user.proto
gen:
ifeq ($(words $(MAKECMDGOALS)),1)
	$(error Usage: make gen <proto-file>)
endif
	protoc --go_out=. --go-grpc_out=. $(word 2, $(MAKECMDGOALS))

# Allow passing proto file as argument (avoid "No rule to make target")
%:
	@:

# ================================
# Install protoc Go plugins
# ================================
ins:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	sudo apt install -y protobuf-compiler
	export PATH="$PATH:/home/jukebox/go/bin"