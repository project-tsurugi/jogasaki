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

#include <jogasaki/constants.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/functions.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::exchange::aggregate {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

using kind = meta::field_type_kind;

/**
 * @brief information to execute aggregate exchange, used to extract schema and record layout information for key/value parts
 */
class aggregate_info {
public:
    using field_index_type = meta::record_meta::field_index_type;

    class value_spec {
    public:
        value_spec() = default;

        value_spec(
            aggregator_type aggregator,
            std::vector<std::size_t> argument_indices,
            meta::field_type type
        ) noexcept :
            aggregator_(std::move(aggregator)),
            argument_indices_(std::move(argument_indices)),
            type_(std::move(type))
        {}

        [[nodiscard]] aggregator_type const& aggregator() const noexcept {
            return aggregator_;
        }

        [[nodiscard]] sequence_view<std::size_t const> argument_indices() const noexcept {
            return argument_indices_;
        }

        [[nodiscard]] meta::field_type const& type() const noexcept {
            return type_;
        }
    private:
        aggregator_type aggregator_{};
        std::vector<std::size_t> argument_indices_{};
        meta::field_type type_{};
    };
    /**
     * @brief construct empty object
     */
    aggregate_info() = default;

    /**
     * @brief construct new object
     * @param record the metadata of the input record for aggregate operation
     * @param key_indices the ordered indices to choose the keys from the record fields
     * @param aggregator the aggregation function
     */
    aggregate_info(
        maybe_shared_ptr<meta::record_meta> record,
        std::vector<field_index_type> key_indices,
        std::vector<value_spec> value_specs
    ) :
        record_(std::move(record)),
        key_indices_(std::move(key_indices)),
        value_specs_(std::move(value_specs)),
        group_(std::make_shared<meta::group_meta>(create_key_meta(), create_value_meta()))
    {
        args_.reserve(value_specs_.size());
        for(auto&& vs : value_specs_) {
            std::vector<aggregator_arg> arg{};
            arg.reserve(vs.argument_indices().size());
            for(auto i : vs.argument_indices()) {
                arg.emplace_back(
                    record_->at(i),
                    record_->value_offset(i),
                    record_->nullity_offset(i)
                );
            }
            args_.emplace_back(std::move(arg));
        }
    }


    /**
     * @brief extract key part from the input record
     */
    [[nodiscard]] accessor::record_ref extract_key(accessor::record_ref record) const noexcept {
        return accessor::record_ref(record.data(), record_->record_size());
    }

    /**
     * @brief returns metadata for input record
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept {
        return record_;
    }

    /**
     * @brief returns metadata for key part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept {
        return group_->key_shared();
    }

    /**
     * @brief returns metadata for value part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept {
        return group_->value_shared();
    }

    /**
     * @brief returns metadata for key/value parts at once
     */
    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& group_meta() const noexcept {
        return group_;
    }

    /**
     * @brief returns aggregator specs
     */
    [[nodiscard]] sequence_view<value_spec const> value_specs() const noexcept {
        return value_specs_;
    }

    /**
     * @brief returns key indices
     */
    [[nodiscard]] sequence_view<field_index_type const> key_indices() const noexcept {
        return key_indices_;
    }

    /**
     * @brief returns aggregator args
     */
    [[nodiscard]] sequence_view<aggregator_arg const> aggregators_args(std::size_t idx) const noexcept {
        return args_[idx];
    }
private:
    maybe_shared_ptr<meta::record_meta> record_{std::make_shared<meta::record_meta>()};
    std::vector<field_index_type> key_indices_{};
    std::vector<value_spec> value_specs_{};
    maybe_shared_ptr<meta::group_meta> group_{std::make_shared<meta::group_meta>()};
    std::vector<std::vector<aggregator_arg>> args_{};

    [[nodiscard]] std::shared_ptr<meta::record_meta> create_key_meta() {
        auto num = key_indices_.size();
        meta::record_meta::fields_type fields{};
        meta::record_meta::nullability_type  nullables(num+1);
        fields.reserve(num+1);
        for(std::size_t i=0; i < num; ++i) {
            auto ind = key_indices_[i];
            fields.emplace_back(record_->at(ind));
            if (record_->nullable(ind)) {
                nullables.set(i);
            }
        }
        fields.emplace_back(meta::field_type{enum_tag<kind::pointer>});
        nullables.set(num);
        return std::make_shared<meta::record_meta>(
            std::move(fields),
            std::move(nullables)
        );
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> create_value_meta() {
        auto num = value_specs_.size();
        meta::record_meta::fields_type fields{};
        meta::record_meta::nullability_type  nullables(num);
        nullables.flip(); // assuming all values can be null
        fields.reserve(num);
        for(std::size_t i=0; i < num; ++i) {
            auto&& v = value_specs_[i];
            fields.emplace_back(v.type());
        }
        return std::make_shared<meta::record_meta>(
            std::move(fields),
            std::move(nullables)
        );
    }
};

}
