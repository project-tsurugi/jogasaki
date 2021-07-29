/*
 * Copyright 2018-2021 tsurugi project.
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

#include <stdexcept>

#include "boost/filesystem.hpp"

namespace jogasaki::test {

class temporary_folder {
public:
    void prepare() {
        auto pattern = boost::filesystem::temp_directory_path();
        pattern /= "jogasaki-test-%%%%%%%%";
        for (std::size_t i = 0; i < 10U; ++i) {
            auto candidate = boost::filesystem::unique_path(pattern);
            if (boost::filesystem::create_directories(candidate)) {
                path_ = candidate;
                break;
            }
        }
    }

    void clean() {
        if (!path_.empty()) {
            boost::filesystem::remove_all(path_);
            path_.clear();
        }
    }

    [[nodiscard]] std::string path() const {
        if (path_.empty() || !boost::filesystem::exists(path_)) {
            throw std::runtime_error("temporary folder has not been initialized yet");
        }
        return path_.string();
    }

private:
    boost::filesystem::path path_;
};

}