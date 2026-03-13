#pragma once

#include <filesystem>
#include <vector>

namespace jogasaki::udf::descriptor::validation {

bool validate_rpc_method_duplicates(std::vector<std::filesystem::path> const& desc_files);

} // namespace jogasaki::udf::descriptor::validation
