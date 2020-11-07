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
class scan : public record_operator {
public:
    friend class scan_context;

    using column = takatori::relation::scan::column;
    /**
     * @brief create empty object
     */
    scan() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param storage_name the storage name to scan
     * @param key_fields field offset information for keys
     * @param value_fields field offset information for values
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    scan(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::string_view storage_name,
        std::vector<details::scan_field> key_fields,
        std::vector<details::scan_field> value_fields,
        std::unique_ptr<operator_base> downstream = nullptr
    ) : record_operator(index, info, block_index),
        storage_name_(storage_name),
        key_fields_(std::move(key_fields)),
        value_fields_(std::move(value_fields)),
        downstream_(std::move(downstream))
    {}

    /**
     * @brief create new object from takatori columns
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param storage_name the storage name to scan
     * @param columns takatori scan column information
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    scan(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        std::vector<column, takatori::util::object_allocator<column>> const& columns,
        std::unique_ptr<operator_base> downstream
    ) : scan(
        index,
        info,
        block_index,
        storage_name,
        create_fields(idx, columns, info, block_index, true),
        create_fields(idx, columns, info, block_index, false),
        std::move(downstream)
    ) {}

    void process_record(operator_executor* parent) override {
        BOOST_ASSERT(parent != nullptr);  //NOLINT
        context_container& container = parent->contexts();
        auto* p = find_context<scan_context>(index(), container);
        if (! p) {
            auto stg = parent->database()->get_storage(storage_name());
            // FIXME transaction should be passed from upper api
            p = parent->make_context<scan_context>(index(),
                parent->get_block_variables(block_index()),
                std::move(stg),
                parent->database()->create_transaction(),
                static_cast<impl::scan_info const*>(parent->task_context()->scan_info()),  //NOLINT
                parent->resource());
        }
        (*this)(*p, parent);

        close(*p);
        if (auto&& tx = p->transaction(); tx) {
            if(! tx->commit()) {
                fail();
            }
        }
    }


    void operator()(scan_context& ctx, operator_executor* parent = nullptr) {
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
            if (downstream_) {
                static_cast<record_operator*>(downstream_.get())->process_record(parent);
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
    std::unique_ptr<operator_base> downstream_{};

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


