file(GLOB ProtoFiles
        "../third_party/tsubakuro/modules/proto/src/main/protos/*.proto"
        )

PROTOBUF_GENERATE_CPP(ProtobufSources ProtobufHeaders ${ProtoFiles})

set(GENERATED_PROTO_SRCS ${ProtobufSources} PARENT_SCOPE)

add_custom_target(build_protos
        DEPENDS ${ProtobufSources}
)
