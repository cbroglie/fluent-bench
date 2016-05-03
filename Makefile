.PHONY: all
all: pb

.PHONY: clean
clean: clean-pb

.PHONY: pb
pb: $(shell find kpl -name '*.proto' | sed -e 's/\.proto$$/.pb.go/')

%.pb.go: %.proto
	protoc -I=kpl/ --go_out=import_path=kpl:kpl/ kpl/*.proto

.PHONY: clean-pb
clean-pb:
	rm -f kpl/*.pb.go

