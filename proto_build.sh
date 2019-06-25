#bash

protoc --proto_path=proto/pmongo --go_out=../../../ objectid.proto

# protoc --proto_path=test --proto_path=proto --proto_path=proto/third_party --go_out=test codecs_test.proto

protoc --proto_path=test -I=proto --proto_path=proto/third_party -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gofast_out=Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types:test codecs_test.proto