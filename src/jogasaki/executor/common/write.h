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

#include <takatori/statement/write.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/data/aligned_buffer.h>

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using yugawara::compiled_info;

namespace details {

/**
 * @brief tuple holds the buffer for tuple values
 */
class cache_align write_tuple {
public:
    write_tuple() = default;

    /**
     * @brief create new write field
     * @param size size in byte of the tuple to be written
     */
    explicit write_tuple(std::string_view data);

    [[nodiscard]] void* data() const noexcept;

    [[nodiscard]] std::size_t size() const noexcept;

    [[nodiscard]] explicit operator std::string_view() const noexcept {
        return {static_cast<char*>(buf_.data()), buf_.size()};
    }

private:
    data::aligned_buffer buf_{};
};

/**
 * @brief field info. for write
 */
struct write_field {
    write_field(
        std::size_t index,
        meta::field_type type,
        kvs::coding_spec spec,
        bool nullable
    ) :
        index_(index),
        type_(std::move(type)),
        spec_(spec),
        nullable_(nullable)
    {}

    std::size_t index_{};  //NOLINT
    meta::field_type type_{};  //NOLINT
    kvs::coding_spec spec_{};  //NOLINT
    bool nullable_{};  //NOLINT
};

class write_target {
public:
    write_target(
        std::string_view storage_name,
        std::vector<details::write_tuple> keys,
        std::vector<details::write_tuple> values
    ) :
        storage_name_(storage_name),
        keys_(std::move(keys)),
        values_(std::move(values))
    {}

    std::string storage_name_{};
    std::vector<details::write_tuple> keys_{};
    std::vector<details::write_tuple> values_{};
};
} // namespace

/**
 * @brief write statement
 */
class write : public model::statement {
public:
    using column = takatori::statement::write::column;
    using tuple = takatori::statement::write::tuple;

    /**
     * @brief create empty object
     */
    write() = default;

    /**
     * @brief create new object
     * @param kind
     * @param storage_name
     * @param keys
     * @param values
     */
    write(
        write_kind kind,
        std::vector<details::write_target> targets
    ) noexcept;

    write(
        write_kind kind,
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        takatori::tree::tree_fragment_vector<tuple> const& tuples,
        memory::lifo_paged_memory_resource& resource,
        compiled_info const& info,
        executor::process::impl::variable_table const* host_variables
    ) noexcept;

    [[nodiscard]] model::statement_kind kind() const noexcept override;

    bool operator()(request_context& context) const;

private:
    write_kind kind_{};
    std::vector<details::write_target> targets_{};

    std::vector<details::write_tuple> create_tuples(
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        takatori::tree::tree_fragment_vector<tuple> const& tuples,
        compiled_info const& info,
        memory::lifo_paged_memory_resource& resource,
        executor::process::impl::variable_table const* host_variables,
        bool key,
        std::vector<details::write_tuple> const& primary_key_tuples = {}
    );

    std::vector<details::write_target> create_targets(
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        takatori::tree::tree_fragment_vector<tuple> const& tuples,
        compiled_info const& info,
        memory::lifo_paged_memory_resource& resource,
        executor::process::impl::variable_table const* host_variables
    );
};

}
