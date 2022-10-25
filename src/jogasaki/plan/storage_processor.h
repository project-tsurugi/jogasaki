/*
 * Copyright 2018-2020 tsurugi project.
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

#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/storage/basic_prototype_processor.h>

namespace jogasaki::plan {

using ::yugawara::storage::index;
using ::yugawara::storage::table;
namespace schema = ::yugawara::schema;

/**
 * @brief result information on prototype processing
 */
class storage_processor_result {
public:
    storage_processor_result() = default;

    explicit storage_processor_result(
        bool primary_key_generated,
        std::shared_ptr<yugawara::storage::sequence> primary_key_sequence
    );

    [[nodiscard]] bool primary_key_generated() const noexcept;

    [[nodiscard]] std::shared_ptr<yugawara::storage::sequence> primary_key_sequence() const noexcept;

private:
    bool primary_key_generated_{};
    std::shared_ptr<yugawara::storage::sequence> primary_key_sequence_{};
};

/**
 * @brief prototype processor to adapt jogasaki specific table/index impl. details
 */
class storage_processor : public ::yugawara::storage::basic_prototype_processor {
public:
    /**
     * @brief create empty object
     */
    storage_processor() = default;

    /**
     * @brief destruct the object
     */
    ~storage_processor() override = default;

    storage_processor(storage_processor const& other) = delete;
    storage_processor& operator=(storage_processor const& other) = delete;
    storage_processor(storage_processor&& other) noexcept = delete;
    storage_processor& operator=(storage_processor&& other) noexcept = delete;

    /**
     * @see basic_prototype_processor::ensure()
     */
    bool ensure(
        schema::declaration const& location,
        table& table_prototype,
        index& primary_index_prototype,
        diagnostic_consumer_type const& diagnostic_consumer
    ) override;

    /**
     * @see basic_prototype_processor::ensure()
     */
    bool ensure(
        schema::declaration const& location,
        index& secondary_index_prototype,
        diagnostic_consumer_type const& diagnostic_consumer) override;

    /**
     * @brief accessor to the prototype processing result
     * @return the result of ensure()
     */
    [[nodiscard]] storage_processor_result result() const noexcept;

private:
    bool primary_key_generated_{};
    std::shared_ptr<yugawara::storage::sequence> primary_key_sequence_;
};

}
