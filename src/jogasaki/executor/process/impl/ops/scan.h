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

#include <takatori/util/sequence_view.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "scan_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

namespace details {

struct cache_align scan_field {
    scan_field(
        meta::field_type type,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool nullable
    ) :
        type_(std::move(type)),
        target_offset_(target_offset),
        target_nullity_offset_(target_nullity_offset),
        nullable_(nullable)
    {}

    meta::field_type type_{}; //NOLINT
    std::size_t target_offset_{}; //NOLINT
    std::size_t target_nullity_offset_{}; //NOLINT
    bool nullable_{}; //NOLINT
};

}

/**
 * @brief scanner
 */
class scan : public operator_base {
public:
    friend class scan_context;

    using column = takatori::relation::scan::column;
    /**
     * @brief create empty object
     */
    scan() = default;

    /**
     * @brief create new object
     */
    scan(
        processor_info const& info,
        block_index_type block_index,
        std::string_view storage_name,
        std::vector<details::scan_field> key_fields,
        std::vector<details::scan_field> value_fields,
        relation::expression const* downstream
    ) : operator_base(info, block_index),
        storage_name_(storage_name),
        key_fields_(std::move(key_fields)),
        value_fields_(std::move(value_fields)),
        downstream_(downstream)
    {}

    /**
     * @brief create new object from takatori/yugawara
     */
    scan(
        processor_info const& info,
        block_index_type block_index,
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        std::vector<column, takatori::util::object_allocator<column>> const& columns,
        relation::expression const* downstream
    ) : scan(
        info,
        block_index,
        storage_name,
        create_fields(idx, columns, info, block_index, true),
        create_fields(idx, columns, info, block_index, false),
        downstream
    ) {}

    template <typename Callback = void>
    void operator()(scan_context& ctx, Callback* visitor = nullptr) {
        open(ctx);
        auto target = ctx.variables().store().ref();
        while(ctx.it_ && ctx.it_->next()) { // TODO assume ctx.it_ always exist
            std::string_view k{};
            std::string_view v{};
            if(!ctx.it_->key(k) || !ctx.it_->value(v)) {
                fail();
            }
            kvs::stream keys{const_cast<char*>(k.data()), k.length()}; //TODO create read-only stream
            kvs::stream values{const_cast<char*>(v.data()), v.length()}; //   and avoid using const_cast
            for(auto&& f : key_fields_) {
                kvs::decode(keys, f.type_, target, f.target_offset_, ctx.resource());
            }
            for(auto&& f : value_fields_) {
                kvs::decode(values, f.type_, target, f.target_offset_, ctx.resource());
            }
            if (visitor) {
                dispatch(*visitor, *downstream_);
            }
        }
        close(ctx);
    }

    void open(scan_context& ctx) {
        if (ctx.stg_ && ctx.tx_ && !ctx.it_) {
            if(auto res = ctx.stg_->scan(
                    *ctx.tx_,
                    ctx.scan_info_->begin_key(),
                    ctx.scan_info_->begin_endpoint(),
                    ctx.scan_info_->end_key(),
                    ctx.scan_info_->end_endpoint(),
                    ctx.it_);
                !res) {
                fail();
            }
        }
    }

    void close(scan_context& ctx) {
        ctx.it_.reset();
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::scan;
    }

    [[nodiscard]] std::string_view storage_name() const noexcept {
        return storage_name_;
    }
private:
    std::string storage_name_{};
    std::vector<details::scan_field> key_fields_{};
    std::vector<details::scan_field> value_fields_{};
    relation::expression const* downstream_{};

    std::vector<details::scan_field> create_fields(
        yugawara::storage::index const& idx,
        std::vector<column, takatori::util::object_allocator<column>> const& columns,
        processor_info const& info,
        block_index_type block_index,
        bool key) {
        // TODO currently using the order passed from the index. Re-visit column order.
        std::vector<details::scan_field> ret{};
        using variable = takatori::descriptor::variable;
        yugawara::binding::factory bindings{};
        std::unordered_map<variable, variable> table_to_stream{};
        for(auto&& m : columns) {
            table_to_stream.emplace(m.source(), m.destination());
        }
        auto num_keys = idx.keys().size();
        auto num_values = idx.table().columns().size() - num_keys;

        auto& block = info.scopes_info()[block_index];
        if (key) {
            ret.reserve(num_keys);
            for(auto&& k : idx.keys()) {
                auto b = bindings(k.column());
                auto&& var = table_to_stream.at(b);
                ret.emplace_back(
                    utils::type_for(info.compiled_info(), var),
                    block.value_map().at(var).value_offset(),
                    block.value_map().at(var).nullity_offset(),
                    false // TODO nullity
                );
            }
        } else {
            ret.reserve(num_values);
            for(auto&& v : idx.table().columns()) {
                bool not_key = true;
                for(auto&& k : idx.keys()) {
                    if (k == v) {
                        not_key = false;
                        break;
                    }
                }
                if (not_key) {
                    auto b = bindings(v);
                    auto&& var = table_to_stream.at(b);
                    ret.emplace_back(
                        utils::type_for(info.compiled_info(), var),
                        block.value_map().at(var).value_offset(),
                        block.value_map().at(var).nullity_offset(),
                        false // TODO nullity
                    );
                }
            }
        }
        return ret;
    }
};

}


