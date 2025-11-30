if(TARGET gRPC::grpc
   AND TARGET gRPC::grpc++
   AND TARGET gRPC::gpr)
  return()
endif()

find_library(GRPC_GPR_LIBRARY NAMES gpr)
find_library(GRPC_GRPC_LIBRARY NAMES grpc)
find_library(GRPC_GRPCPP_LIBRARY NAMES grpc++)

find_path(GRPC_INCLUDE_DIR NAMES grpcpp/grpcpp.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  gRPC DEFAULT_MSG GRPC_INCLUDE_DIR GRPC_GPR_LIBRARY GRPC_GRPC_LIBRARY
  GRPC_GRPCPP_LIBRARY)

if(gRPC_FOUND)
  add_library(gRPC::gpr SHARED IMPORTED)
  set_target_properties(
    gRPC::gpr PROPERTIES IMPORTED_LOCATION "${GRPC_GPR_LIBRARY}"
                         INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc SHARED IMPORTED)
  set_target_properties(
    gRPC::grpc
    PROPERTIES IMPORTED_LOCATION "${GRPC_GRPC_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}"
               INTERFACE_LINK_LIBRARIES gRPC::gpr)

  add_library(gRPC::grpc++ SHARED IMPORTED)
  set_target_properties(
    gRPC::grpc++
    PROPERTIES IMPORTED_LOCATION "${GRPC_GRPCPP_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}"
               INTERFACE_LINK_LIBRARIES "gRPC::grpc;gRPC::gpr")

  set(GRPC_LIBRARIES ${GRPC_GRPCPP_LIBRARY} ${GRPC_GRPC_LIBRARY}
                     ${GRPC_GPR_LIBRARY})
  set(GRPC_INCLUDE_DIRS ${GRPC_INCLUDE_DIR})
endif()

unset(GRPC_GPR_LIBRARY CACHE)
unset(GRPC_GRPC_LIBRARY CACHE)
unset(GRPC_GRPCPP_LIBRARY CACHE)
unset(GRPC_INCLUDE_DIR CACHE)
