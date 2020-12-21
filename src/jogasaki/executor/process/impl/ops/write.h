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

#include <takatori/relation/write.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "write_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field info of the write operation
 * @details write operator uses these fields to know how the scope variables or input record fields are are mapped to
 * key/value fields.
 */
struct cache_align write_field {
    /**
     * @brief create new write field
     * @param type type of the write field
     * @param source_offset byte offset of the source field in the source record reference
     * @param source_nullity_offset bit offset of the source field nullity in the source record reference
     * @param source_nullable whether the target field is nullable or not
     * @param spec the spec of the source field used for encode/decode
     */
    write_field(
        meta::field_type type,
        std::size_t source_offset,
        std::size_t source_nullity_offset,
        bool target_nullable,
        kvs::coding_spec spec
    ) :
        type_(std::move(type)),
        source_offset_(source_offset),
        source_nullity_offset_(source_nullity_offset),
        target_nullable_(target_nullable),
        spec_(spec)
    {}

    meta::field_type type_{}; //NOLINT
    std::size_t source_offset_{}; //NOLINT
    std::size_t source_nullity_offset_{}; //NOLINT
    bool target_nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};

}

/**
 * @brief write kind corresponding to takatori::relation::write_kind
 */
enum class write_kind {
    insert,
    update,
    delete_,
    insert_or_update,
};

/**
 * @brief write operator
 */
class write : public record_operator {
public:
    friend class write_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    write() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param kind write operation kind
     * @param storage_name the storage name to write
     * @param key_fields field offset information for keys
     * @param value_fields field offset information for values
     */
    write(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        std::string_view storage_name,
        std::vector<details::write_field> key_fields,
        std::vector<details::write_field> value_fields
    ) :
        kind_(kind),
        storage_name_(storage_name),
        key_fields_(std::move(key_fields)),
        value_fields_(std::move(value_fields))
    {}

    /**
     * @brief create new object from takatori columns
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param kind write operation kind
     * @param storage_name the storage name to write
     * @param idx target index information
     * @param keys takatori write keys information
     * @param columns takatori write columns information
     */
    write(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns
    ) :
        write(
            index,
            info,
            block_index,
            kind,
            storage_name,
            create_fields(kind, idx, keys, columns, info, block_index, true),
            create_fields(kind, idx, keys, columns, info, block_index, false)
        )
    {}


    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<write_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<write_context>(index(),
                ctx.block_scope(block_index()),
                ctx.database()->get_storage(storage_name()),
                ctx.transaction(),
                ctx.resource(),
                ctx.varlen_resource()
            );
        }
        (*this)(*p);
    }

    /**
     * @brief process record with context object
     * @details process record, construct key/value sequences and invoke kvs to conduct write operations
     * @param ctx operator context object for the execution
     */
    void operator()(write_context& ctx) {
        if (! opened_) {
            open(ctx);
            opened_ = true;
        }
        auto source = ctx.variables().store().ref();
        auto resource = ctx.varlen_resource();

        switch(kind_) {
            case write_kind::insert:
                do_insert(kind_, ctx);
                break;
            case write_kind::update:
                do_insert(kind_, ctx);
                break;
            case write_kind::insert_or_update:
                do_insert(kind_, ctx);
                break;
            case write_kind::delete_:
                do_delete(kind_, ctx);
                break;
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::write;
    }
    /**
     * @brief return storage name
     * @return the storage name of the write target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept {
        return storage_name_;
    }

    void finish(abstract::task_context* context) override {
        context_helper ctx{*context};
        auto* p = find_context<write_context>(index(), ctx.contexts());
        if (p) {
            close(*p);
        }
    }
private:
    write_kind kind_{};
    std::string storage_name_{};
    std::vector<details::write_field> key_fields_{};
    std::vector<details::write_field> value_fields_{};
    bool opened_{};

    void open(write_context& ctx) {
        // TODO
    }

    void close(write_context& ctx) {
        // TODO
        opened_ = false;
    }

    void encode_fields(
        std::vector<details::write_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref source,
        memory_resource* resource
    ) {
        for(auto const& f : fields) {
            kvs::encode_nullable(source, f.source_offset_, f.source_nullity_offset_, f.type_, f.spec_, stream);
        }
    }

    write_kind from(relation::write_kind kind) {
        using k = relation::write_kind;
        switch (kind) {
            case k::insert: return write_kind::insert;
            case k::update: return write_kind::update;
            case k::delete_: return write_kind::delete_;
            case k::insert_or_update: return write_kind::insert_or_update;
        }
        fail();
    }
    std::vector<details::write_field> create_fields(
        write_kind kind,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        processor_info const& info,
        block_index_type block_index,
        bool key
    ) {
        std::vector<details::write_field> ret{};
        using variable = takatori::descriptor::variable;
        yugawara::binding::factory bindings{};
        auto& block = info.scopes_info()[block_index];
        std::unordered_map<variable, variable> table_to_stream{};
        if (key) {
            for(auto&& c : keys) {
                table_to_stream.emplace(c.destination(), c.source());
            }
            ret.reserve(idx.keys().size());
            for(auto&& k : idx.keys()) {
                auto kc = bindings(k.column());
                auto t = utils::type_for(k.column().type());
                auto spec = k.direction() == relation::sort_direction::ascendant ?
                    kvs::spec_key_ascending : kvs::spec_key_descending;
                if (table_to_stream.count(kc) == 0) {
                    fail();
                }
                auto&& var = table_to_stream.at(kc);
                ret.emplace_back(
                    t,
                    block.value_map().at(var).value_offset(),
                    block.value_map().at(var).nullity_offset(),
                    k.column().criteria().nullity().nullable(),
                    spec
                );
            }
            return ret;
        }
        if (kind == write_kind::delete_) return ret;
        for(auto&& c : columns) {
            table_to_stream.emplace(c.destination(), c.source());
        }
        ret.reserve(idx.values().size());
        for(auto&& v : idx.values()) {
            auto b = bindings(v);
            auto& c = static_cast<yugawara::storage::column const&>(v);
            auto t = utils::type_for(c.type());
            if (table_to_stream.count(b) == 0) {
                fail();
            }
            auto&& var = table_to_stream.at(b);
            ret.emplace_back(
                t,
                block.value_map().at(var).value_offset(),
                block.value_map().at(var).nullity_offset(),
                c.criteria().nullity().nullable(),
                kvs::spec_value
            );
        }
        return ret;
    }

    void check_length_and_extend_buffer(
        write_context& ctx,
        std::vector<details::write_field> const& fields,
        data::aligned_buffer& buffer
    ) {
        auto source = ctx.variables().store().ref();
        auto resource = ctx.varlen_resource();
        kvs::stream null_stream{};
        encode_fields(fields, null_stream, source, resource);
        if (null_stream.length() > buffer.size()) {
            buffer.resize(null_stream.length());
        }
    }

    void do_insert(write_kind kind, write_context& ctx) {
        auto source = ctx.variables().store().ref();
        auto resource = ctx.varlen_resource();
        // calculate length first, then put
        check_length_and_extend_buffer(ctx, key_fields_, ctx.key_buf_);
        check_length_and_extend_buffer(ctx, value_fields_, ctx.value_buf_);
        auto* k = static_cast<char*>(ctx.key_buf_.data());
        auto* v = static_cast<char*>(ctx.value_buf_.data());
        kvs::stream keys{k, ctx.key_buf_.size()};
        kvs::stream values{v, ctx.value_buf_.size()};
        encode_fields(key_fields_, keys, source, resource);
        encode_fields(value_fields_, values, source, resource);
        if(auto res = ctx.stg_->put(
                *ctx.tx_,
                {k, keys.length()},
                {v, values.length()}
            ); !res) {
            fail();
        }
    }

    void do_delete(write_kind kind, write_context& ctx) {
        auto source = ctx.variables().store().ref();
        auto resource = ctx.varlen_resource();
        // calculate length first, and then put
        check_length_and_extend_buffer(ctx, key_fields_, ctx.key_buf_);
        auto* k = static_cast<char*>(ctx.key_buf_.data());
        kvs::stream keys{k, ctx.key_buf_.size()};
        encode_fields(key_fields_, keys, source, resource);
        if(auto res = ctx.stg_->remove(
                *ctx.tx_,
                {k, keys.length()}
            ); !res) {
            LOG(WARNING) << "deletion target not found";
        }
    }
};


}


