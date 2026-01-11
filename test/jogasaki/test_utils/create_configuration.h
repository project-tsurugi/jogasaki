/*
 * Copyright 2018-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <string>

#include <tateyama/api/configuration.h>

namespace jogasaki::test_utils {

/**
 * @brief Create a test configuration with default settings.
 * @param log_location the directory path for datastore log location
 * @param session_store the directory path for blob relay session store
 * @return configuration object for testing
 */
std::shared_ptr<tateyama::api::configuration::whole> create_configuration(
    std::string const& log_location,
    std::string const& session_store,
    std::optional<std::size_t> grpc_port = 52345
);

}  // namespace jogasaki::test_utils
