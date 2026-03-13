#pragma once

#include <filesystem>
#include <google/protobuf/descriptor.pb.h>

namespace jogasaki::udf::descriptor {

bool read_file_descriptor_set(
    std::filesystem::path const& desc_path, google::protobuf::FileDescriptorSet& fds);

} // namespace jogasaki::udf::descriptor
