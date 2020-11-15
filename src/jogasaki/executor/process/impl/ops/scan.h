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
#include <takatori/util/downcast.h>
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
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field info of the scan operation
 */
struct cache_align scan_field {
    /**
     * @brief create new scan field
     * @param type type of the scanned field
     * @param target_exists whether the target storage exists. If not, there is no room to copy the data to.
     * @param target_offset byte offset of the target field in the target record reference
     * @param target_nullity_offset bit offset of the target field nullity in the target record reference
     * @param nullable whether the target field is nullable or not
     * @param order the ordering (asc/desc) of the target field used for encode/decode
     */
    scan_field(
        meta::field_type type,
        bool target_exists,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool nullable,
        kvs::order order
    ) :
        type_(std::move(type)),
        target_exists_(target_exists),
        target_offset_(target_offset),
        target_nullity_offset_(target_nullity_offset),
        nullable_(nullable),
        order_(order)
        {}

    meta::field_type type_{}; //NOLINT
    bool target_exists_{}; //NOLINT
    std::size_t target_offset_{}; //NOLINT
    std::size_t target_nullity_offset_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::order order_{}; //NOLINT
};

}

/**
 * @brief scan operator
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

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<scan_context>(index(), ctx.contexts());
        if (! p) {
            auto stg = ctx.database()->get_storage(storage_name());
            p = ctx.make_context<scan_context>(index(),
                ctx.block_scope(block_index()),
                std::move(stg),
                ctx.transaction(),
                static_cast<impl::scan_info const*>(ctx.task_context()->scan_info()),  //NOLINT
                ctx.resource());
        }
        (*this)(*p, context);

        close(*p);
        if (auto&& tx = p->transaction(); tx) {
            if(! tx->commit()) {
                fail();
            }
        }
    }

    /**
     * @brief process record with context object
     * @details process record, fill variables with scanned result, and invoke downstream
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     */
    void operator()(scan_context& ctx, abstract::task_context* context = nullptr) {
        open(ctx);
        auto target = ctx.variables().store().ref();
        while(ctx.it_->next()) {
            std::string_view k{};
            std::string_view v{};
            if(!ctx.it_->key(k) || !ctx.it_->value(v)) {
                fail();
            }
            kvs::stream keys{const_cast<char*>(k.data()), k.length()}; //TODO create read-only stream
            kvs::stream values{const_cast<char*>(v.data()), v.length()}; //   and avoid using const_cast
            for(auto&& f : key_fields_) {
                if (! f.target_exists_) {
                    kvs::consume_stream(keys, f.type_, f.order_);
                    continue;
                }
                kvs::decode(keys, f.type_, f.order_, target, f.target_offset_, ctx.resource());
            }
            for(auto&& f : value_fields_) {
                if (! f.target_exists_) {
                    kvs::consume_stream(values, f.type_, f.order_);
                    continue;
                }
                kvs::decode(values, f.type_, f.order_, target, f.target_offset_, ctx.resource());
            }
            if (downstream_) {
                unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
            }
        }
        close(ctx);
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::scan;
    }

    /**
     * @brief return storage name
     * @return the storage name of the scan target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept {
        return storage_name_;
    }
private:
    std::string storage_name_{};
    std::vector<details::scan_field> key_fields_{};
    std::vector<details::scan_field> value_fields_{};
    std::unique_ptr<operator_base> downstream_{};

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

    std::vector<details::scan_field> create_fields(
        yugawara::storage::index const& idx,
        std::vector<column, takatori::util::object_allocator<column>> const& columns,
        processor_info const& info,
        block_index_type block_index,
        bool key) {
        std::vector<details::scan_field> ret{};
        using variable = takatori::descriptor::variable;
        yugawara::binding::factory bindings{};
        std::unordered_map<variable, variable> table_to_stream{};
        for(auto&& m : columns) {
            table_to_stream.emplace(m.source(), m.destination());
        }
        auto& block = info.scopes_info()[block_index];
        if (key) {
            ret.reserve(idx.keys().size());
            for(auto&& k : idx.keys()) {
                auto b = bindings(k.column());
                auto t = utils::type_for(k.column().type());
                auto odr = k.direction() == relation::sort_direction::ascendant ?
                    kvs::order::ascending : kvs::order::descending;
                if (table_to_stream.count(b) == 0) {
                    ret.emplace_back(
                        t,
                        false,
                        0,
                        0,
                        false,// TODO nullity
                        odr
                    );
                    continue;
                }
                auto&& var = table_to_stream.at(b);
                ret.emplace_back(
                    t,
                    true,
                    block.value_map().at(var).value_offset(),
                    block.value_map().at(var).nullity_offset(),
                    false, // TODO nullity
                    odr
                );
            }
            return ret;
        }
        ret.reserve(idx.values().size());
        for(auto&& v : idx.values()) {
            auto b = bindings(v);
            auto t = utils::type_for(static_cast<yugawara::storage::column const&>(v).type());
            if (table_to_stream.count(b) == 0) {
                ret.emplace_back(
                    t,
                    false,
                    0,
                    0,
                    false,// TODO nullity
                    kvs::order::undefined
                );
                continue;
            }
            auto&& var = table_to_stream.at(b);
            ret.emplace_back(
                t,
                true,
                block.value_map().at(var).value_offset(),
                block.value_map().at(var).nullity_offset(),
                false, // TODO nullity
                kvs::order::undefined
            );
        }
        return ret;
    }
};

}


