#include "descriptor_reader.h"

#include <fstream>

#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/udf/log/logging_prefix.h>

namespace jogasaki::udf::descriptor {

bool read_file_descriptor_set(
    std::filesystem::path const& desc_path, google::protobuf::FileDescriptorSet& fds) {
    std::ifstream input(desc_path, std::ios::binary);
    if (!input) {
        LOG_LP(WARNING) << jogasaki::udf::log::prefix
                        << "cannot open descriptor: " << desc_path.string();
        return false;
    }

    if (!fds.ParseFromIstream(&input)) {
        LOG_LP(WARNING) << jogasaki::udf::log::prefix
                        << "failed to parse descriptor: " << desc_path.string();
        return false;
    }

    return true;
}

} // namespace jogasaki::udf::descriptor
