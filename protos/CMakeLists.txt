file(GLOB ProtoFiles
        "*.proto"
        )
PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})
set(GENERATED_PROTO_SRCS
  "${CMAKE_BINARY_DIR}/protos/request.pb.cc"
  "${CMAKE_BINARY_DIR}/protos/response.pb.cc"
  "${CMAKE_BINARY_DIR}/protos/common.pb.cc"
  "${CMAKE_BINARY_DIR}/protos/schema.pb.cc"
)
add_custom_target(build_protos
  DEPENDS ${ProtoSources} ${GENERATED_PROTO_SRCS}
)
