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

#include <vector>
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/aggregate_function_info.h>
#include <jogasaki/executor/function/aggregator_info.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::exchange::aggregate {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

using kind = meta::field_type_kind;
using function::aggregate_function_info;
using function::aggregator_info;
using function::field_locator;

/**
 * @brief information to execute aggregate exchange, used to extract schema and record layout information for key/value parts
 * @details there are two group meta involved in aggregate output. Intermediate group meta (prefixed with mid-) is the
 * intermediate output, where key has internal pointer field and value has calculation fields. Post group meta (prefixed with post-)
 * is the final output metadata of the aggregate exchange.
 */
class aggregate_info {
public:
    using field_index_type = meta::record_meta::field_index_type;

    class value_spec {
    public:
        value_spec() = default;

        value_spec(
            aggregate_function_info const& function_info,
            std::vector<std::size_t> argument_indices,
            meta::field_type type
        ) noexcept;

        value_spec(
            class aggregator_info const& aggregator_info,
            std::vector<std::size_t> argument_indices,
            meta::field_type type
        ) noexcept;

        [[nodiscard]] aggregate_function_info const& function_info() const noexcept;

        [[nodiscard]] class aggregator_info const& aggregator_info() const noexcept;

        [[nodiscard]] sequence_view<std::size_t const> argument_indices() const noexcept;

        [[nodiscard]] meta::field_type const& type() const noexcept;
    private:
        aggregate_function_info const* function_info_{};
        class aggregator_info const* aggregator_info_{};
        std::vector<std::size_t> argument_indices_{};
        meta::field_type type_{};
    };

    enum class output_kind {
        pre,
        mid,
        post,
    };

    class output_info {
    public:
        output_info() = default;

        output_info(
            std::vector<value_spec> value_specs,
            output_kind kind,
            maybe_shared_ptr<meta::record_meta> const& pre_input_meta,
            maybe_shared_ptr<meta::record_meta> const& record,
            std::vector<field_index_type> const& key_indices
        );

        /**
         * @brief returns metadata for key/value parts at once
         */
        [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& group_meta() const noexcept;

        /**
         * @brief returns aggregator specs
         */
        [[nodiscard]] sequence_view<value_spec const> value_specs() const noexcept;

        /**
         * @brief returns the number of value fields
         */
        [[nodiscard]] std::size_t value_count() const noexcept;

        /**
         * @brief returns aggregator args
         */
        [[nodiscard]] sequence_view<field_locator const> aggregator_args(std::size_t aggregator_index) const noexcept;

        /**
         * @brief returns target field locator
         */
        [[nodiscard]] field_locator const& target_field_locator(std::size_t aggregator_index) const noexcept;

    private:
        std::vector<value_spec> value_specs_{};
        output_kind kind_{};
        maybe_shared_ptr<meta::record_meta> pre_input_meta_{};
        maybe_shared_ptr<meta::record_meta> record_{};
        std::vector<field_index_type> const* key_indices_{};
        maybe_shared_ptr<meta::group_meta> group_{std::make_shared<meta::group_meta>()};
        std::vector<std::vector<field_locator>> args_{};
        std::vector<field_locator> target_field_locs_{};

        std::shared_ptr<meta::record_meta> create_key_meta(output_kind kind);
        std::shared_ptr<meta::record_meta> create_value_meta(output_kind kind);
        std::vector<std::vector<field_locator>> create_source_field_locs(output_kind kind);
        std::vector<field_locator> create_target_field_locs(output_kind kind);
    };
    /**
     * @brief construct empty object
     */
    aggregate_info() = default;

    /**
     * @brief construct new object
     * @param record the metadata of the input record for aggregate operation
     * @param key_indices the ordered indices to choose the keys from the record fields
     * @param value_specs the specification for the values generated
     */
    aggregate_info(
        maybe_shared_ptr<meta::record_meta> record,
        std::vector<field_index_type> key_indices,
        std::vector<value_spec> value_specs
    );

    /**
     * @brief extract key part from the input record
     * @details the key part is based on the input record and has the meta returned by extracted_key_meta()
     */
    [[nodiscard]] accessor::record_ref extract_key(accessor::record_ref record) const noexcept;

    /**
     * @brief extract output key from the intermediate key
     * @details the returned record is the output key record and has the meta returned by post_group_meta()->key()
     */
    [[nodiscard]] accessor::record_ref output_key(accessor::record_ref mid) const noexcept;

    /**
     * @brief extract output value from the intermediate value
     * @details the returned record is the output value record and has the meta returned by post_group_meta()->value()
     */
    [[nodiscard]] accessor::record_ref output_value(accessor::record_ref mid) const noexcept;

    /**
     * @brief returns metadata for input record
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept;

    /**
     * @brief returns metadata for the key extracted by extract_key()
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& extracted_key_meta() const noexcept;

    /**
     * @brief returns key indices
     */
    [[nodiscard]] sequence_view<field_index_type const> key_indices() const noexcept;

    [[nodiscard]] output_info const& pre() const noexcept {
        return pre_;
    }

    [[nodiscard]] output_info const& mid() const noexcept {
        return mid_;
    }

    [[nodiscard]] output_info const& post() const noexcept {
        return post_;
    }
private:
    maybe_shared_ptr<meta::record_meta> record_{std::make_shared<meta::record_meta>()};
    std::vector<field_index_type> key_indices_{};
    maybe_shared_ptr<meta::record_meta> extracted_key_meta_{};
    output_info pre_{};
    output_info mid_{};
    output_info post_{};
    std::shared_ptr<meta::record_meta> create_extracted_meta(std::vector<std::size_t> const& indices);
    output_info create_output(
        std::vector<value_spec> const& value_specs,
        output_kind kind,
        maybe_shared_ptr<meta::record_meta> record,
        maybe_shared_ptr<meta::record_meta> pre_input_meta,
        std::vector<field_index_type> const& key_indices
    );
};

}
