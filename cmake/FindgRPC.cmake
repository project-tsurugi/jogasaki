find_path(GRPC_INCLUDE_DIR grpcpp/grpcpp.h PATHS /usr/include
                                                 /usr/local/include)

find_library(GRPC_LIBRARY grpc PATHS /usr/lib /usr/lib/x86_64-linux-gnu)
find_library(GRPC_GRPCPP_LIBRARY grpc++ PATHS /usr/lib
                                              /usr/lib/x86_64-linux-gnu)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  gRPC REQUIRED_VARS GRPC_INCLUDE_DIR GRPC_LIBRARY GRPC_GRPCPP_LIBRARY)

if(gRPC_FOUND)
  set(GRPC_LIBRARIES ${GRPC_LIBRARY} ${GRPC_GRPCPP_LIBRARY})
  set(GRPC_INCLUDE_DIRS ${GRPC_INCLUDE_DIR})
endif()
