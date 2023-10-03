/*
 * Copyright 2018-2023 Project Tsurugi.
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

namespace jogasaki::api::kvsservice {

/**
 * @brief the target index specification.
 */
class index {
public:
    /**
     * @brief create new object
     * @param table_name the name of the table
     * @param index_name the name of the index
     * @param schema_name the name of the schema where index belongs
     */
    explicit index(std::string_view table_name, std::string_view index_name = {},
                   std::string_view schema_name = {}) noexcept;

    /**
     * @brief return the table name
     * @return the table name
     */
    [[nodiscard]] std::string_view table_name() const noexcept;

    /**
     * @brief return the index name
     * @return the index name
     */
    [[nodiscard]] std::string_view index_name() const noexcept;

    /**
     * @brief return the schema name
     * @return the schema name
     */
    [[nodiscard]] std::string_view schema_name() const noexcept;

private:
    std::string table_name_ {};
    std::string index_name_ {};
    std::string schema_name_ {};
};

}
