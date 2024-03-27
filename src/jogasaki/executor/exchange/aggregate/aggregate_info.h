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

#include <cstddef>
#include <memory>
#include <set>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/incremental/aggregate_function_info.h>
#include <jogasaki/executor/function/incremental/aggregator_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::exchange::aggregate {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;

using kind = meta::field_type_kind;
using function::incremental::aggregate_function_info;
using function::incremental::aggregator_info;
using function::field_locator;

/**
 * @brief information to execute aggregate exchange, used to extract schema and record layout
 * information for key/value parts
 * @details there are two group meta involved in aggregate output. Intermediate group meta (prefixed with mid-) is the
 * intermediate output, where key has internal pointer field and value has calculation fields.
 * Post group meta (prefixed with post-) is the final output metadata of the aggregate exchange.
 */
class aggregate_info {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief the specification of the value newly generated by this aggregate operation
     */
    class value_spec {
    public:
        /**
         * @brief create empty object
         */
        value_spec() = default;

        /**
         * @brief create new object
         * @param function_info the function to generate this value
         * @param argument_indices the indices of the input record field used as args to the function
         * @param type the result type of the aggregate
         */
        value_spec(
            aggregate_function_info const& function_info,
            std::vector<std::size_t> argument_indices,
            meta::field_type type
        ) noexcept;

        /**
         * @brief accessor to the function info
         */
        [[nodiscard]] aggregate_function_info const& function_info() const noexcept;

        /**
         * @brief accessor to the argument indices
         */
        [[nodiscard]] sequence_view<std::size_t const> argument_indices() const noexcept;

        /**
         * @brief accessor to the result type
         */
        [[nodiscard]] meta::field_type const& type() const noexcept;

    private:
        aggregate_function_info const* function_info_{};
        std::vector<std::size_t> argument_indices_{};
        meta::field_type type_{};
    };

    /**
     * @brief the specification of the aggregator generating a value
     * @details this corresponds to the concrete aggregator function, while value_spec corresponds to consolidated
     * (i.e. pre/mid/post aggregators) aggregated function.
     */
    class aggregator_spec {
    public:
        /**
         * @brief create empty object
         */
        aggregator_spec() = default;

        /**
         * @brief create new object
         * @param aggregator_info the aggregator function to generate value
         * @param argument_indices the indices of the record field used as args to the function
         * @param type the result type of the aggregator
         */
        aggregator_spec(
            class aggregator_info const& aggregator_info,
            std::vector<std::size_t> argument_indices,
            meta::field_type type
        ) noexcept;

        /**
         * @brief accessor to the aggregator info
         */
        [[nodiscard]] class aggregator_info const& aggregator_info() const noexcept;

        /**
         * @brief accessor to the argument indices
         */
        [[nodiscard]] sequence_view<std::size_t const> argument_indices() const noexcept;

        /**
         * @brief accessor to the result type
         */
        [[nodiscard]] meta::field_type const& type() const noexcept;

    private:
        class aggregator_info const* aggregator_info_{};
        std::vector<std::size_t> argument_indices_{};
        meta::field_type type_{};
    };

    /**
     * @brief output kind
     * @details specifies the phase/output of the aggregate
     * @see output_info
     */
    enum class output_kind {
        pre,
        mid,
        post,
    };

    /**
     * @brief output information from the aggregate operation
     * @details this object tells the metadata of the output, and which fields should be used to generate the output.
     * The aggregate operation are categorized to three groups depending on the operation phase,
     * and each has its output info.
     *   - pre the pre-aggregation. The output consists of the key (holding internal data) and
     *     values (extended fields for calculation)
     *     The input flat record to the aggregate exchange are separated to key/values and pre-aggregation is
     *     conducted in this phase.
     *   - mid the intermediate incremental aggregation. The output consists of the same fields as pre output.
     *     The input for this phase is the values part from pre agg. and incremental aggregation is
     *     conducted (i.e. merging values.)
     *   - post the post aggregation. The output consists of the final value fields of this aggregate operation.
     *     The input for this phase is the output from mid agg. and calculation fields are consolidated
     *     to generate result field.
     */
    class output_info {
    public:
        /**
         * @brief create empty object
         */
        output_info() = default;

        /**
         * @brief create new object
         * @param kind output kind
         * @param aggregator_specs the aggregators spec executed for this phase
         * @param aggregate_input the input flat record for the aggregate operation
         * @param phase_input the input record for this phase (the total input record for pre,
         * and value record for mid/post)
         * @param key_indices the indices of fields (0-origin) indicating key columns in aggregate_input
         */
        output_info(
            output_kind kind,
            std::vector<aggregator_spec> aggregator_specs,
            maybe_shared_ptr<meta::record_meta> const& aggregate_input,
            maybe_shared_ptr<meta::record_meta> phase_input,
            std::vector<field_index_type> const& key_indices
        );

        /**
         * @brief returns metadata for key/value parts at once
         */
        [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& group_meta() const noexcept;

        /**
         * @brief returns aggregator specs
         */
        [[nodiscard]] sequence_view<aggregator_spec const> aggregator_specs() const noexcept;

        /**
         * @brief returns the number of value fields
         */
        [[nodiscard]] std::size_t value_count() const noexcept;

        /**
         * @brief returns aggregator args
         * @param aggregator_index specifies the aggregator index (0-origin)
         */
        [[nodiscard]] sequence_view<field_locator const> source_field_locators(
            std::size_t aggregator_index
        ) const noexcept;

        /**
         * @brief returns target field locator
         * @param aggregator_index specifies the aggregator index (0-origin)
         */
        [[nodiscard]] field_locator const& target_field_locator(std::size_t aggregator_index) const noexcept;

        /**
         * @brief returns key compare info
         */
        [[nodiscard]] compare_info const& key_compare_info() const noexcept;

    private:
        output_kind kind_{};
        std::vector<aggregator_spec> aggregator_specs_{};
        maybe_shared_ptr<meta::record_meta> phase_input_{};
        maybe_shared_ptr<meta::group_meta> group_{std::make_shared<meta::group_meta>()};
        std::vector<std::vector<field_locator>> source_field_locators_{};
        std::vector<field_locator> target_field_locators_{};
        compare_info key_compare_info_{};

        std::shared_ptr<meta::record_meta> create_key_meta(
            output_kind kind,
            std::vector<field_index_type> const& key_indices,
            maybe_shared_ptr<meta::record_meta> const& aggregate_input
        );
        std::shared_ptr<meta::record_meta> create_value_meta(
            std::vector<aggregator_spec> const& aggregator_specs
        );
        std::vector<std::vector<field_locator>> create_source_field_locators(
            std::vector<aggregator_spec> const& aggregator_specs,
            maybe_shared_ptr<meta::record_meta> const& phase_input
        );
        std::vector<field_locator> create_target_field_locators(
            std::vector<aggregator_spec> const& aggregator_specs,
            maybe_shared_ptr<meta::group_meta> const& group_meta
        );
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
     * @param generate_record_on_empty specify whether a record will be generated when input records are all empty
     */
    aggregate_info(
        maybe_shared_ptr<meta::record_meta> record,
        std::vector<field_index_type> key_indices,
        std::vector<value_spec> const& value_specs,
        bool generate_record_on_empty = false
    );

    /**
     * @brief extract key part from the input record
     * @details the key part is based on the input record and has the meta returned by extracted_key_meta()
     */
    [[nodiscard]] accessor::record_ref extract_key(accessor::record_ref record) const noexcept;

    /**
     * @brief extract output key from the intermediate key
     * @details the returned record is the output key record and has the meta returned by post().group_meta()->key()
     */
    [[nodiscard]] accessor::record_ref output_key(accessor::record_ref mid) const noexcept;

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

    /**
     * @brief returns pre-output info
     */
    [[nodiscard]] output_info const& pre() const noexcept;

    /**
     * @brief returns mid-output info
     */
    [[nodiscard]] output_info const& mid() const noexcept;

    /**
     * @brief returns post-output info
     */
    [[nodiscard]] output_info const& post() const noexcept;

    /**
     * @brief returns generate_record_on_empty flag
     */
    [[nodiscard]] bool generate_record_on_empty() const noexcept;
private:
    maybe_shared_ptr<meta::record_meta> record_{std::make_shared<meta::record_meta>()};
    std::vector<field_index_type> key_indices_{};
    maybe_shared_ptr<meta::record_meta> extracted_key_meta_{};
    output_info pre_{};
    output_info mid_{};
    output_info post_{};
    bool generate_record_on_empty_{};

    std::shared_ptr<meta::record_meta> create_extracted_meta(
        std::vector<std::size_t> const& indices,
        maybe_shared_ptr<meta::record_meta> const& aggregate_input
    );
    output_info create_output(
        output_kind kind,
        std::vector<value_spec> const& value_specs,
        maybe_shared_ptr<meta::record_meta> const& phase_input,
        maybe_shared_ptr<meta::record_meta> const& aggregate_input,
        std::vector<field_index_type> const& key_indices
    );
};

}
