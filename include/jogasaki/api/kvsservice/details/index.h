/*
 * Copyright 2018-2023 tsurugi project.
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

namespace jogasaki::api::kvsservice::details {

/**
 * @brief the target index specification.
 */
class index {
public:
    index(std::string_view table_name, std::string_view index_name = {}) noexcept;

    std::string_view table_name() const noexcept;
    std::string_view index_name() const noexcept;
private:
    std::string table_name_ {};
    std::string index_name_ {};
};

}
