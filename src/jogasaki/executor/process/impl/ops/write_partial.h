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
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "write_partial_context.h"
#include "context_helper.h"
#include "write_kind.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field info of the update operation
 * @details update operation uses these fields to know how the scope variables or input record fields are are mapped to
 * key/value fields. The update operation retrieves the key/value records from kvs and decode to
 * the record (of key/value respectively), updates the record fields by replacing the value with one from scope variable
 * record (source), encodes the record and puts into kvs.
 */
struct cache_align write_partial_field {
    /**
     * @brief undefined offset value
     */
    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create new object
     * @param type type of the field
     * @param source_offset byte offset of the field in the input variables record
     * @param source_nullity_offset bit offset of the field nullity in the input variables record
     * @param target_offset byte offset of the field in the decoded record
     * @param target_nullity_offset bit offset of the field nullity in the decoded record
     * @param nullable whether the target field is nullable or not
     * @param spec the spec of the source field used for encode/decode
     * @param updated indicates whether the field will be updated or not
     * @param update_source_offset byte offset of the field in the source variables record (used if updated is true)
     * @param update_source_nullity_offset bit offset of the field nullity in the source variables record (used if updated is true)
     */
    write_partial_field(
        meta::field_type type,
        std::size_t source_offset,
        std::size_t source_nullity_offset,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool nullable,
        kvs::coding_spec spec,
        bool updated,
        std::size_t update_source_offset,
        std::size_t update_source_nullity_offset
    ) :
        type_(std::move(type)),
        source_offset_(source_offset),
        source_nullity_offset_(source_nullity_offset),
        target_offset_(target_offset),
        target_nullity_offset_(target_nullity_offset),
        nullable_(nullable),
        spec_(spec),
        updated_(updated),
        update_source_offset_(update_source_offset),
        update_source_nullity_offset_(update_source_nullity_offset)
    {}

    meta::field_type type_{}; //NOLINT
    std::size_t source_offset_{}; //NOLINT
    std::size_t source_nullity_offset_{}; //NOLINT
    std::size_t target_offset_{}; //NOLINT
    std::size_t target_nullity_offset_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
    bool updated_{}; //NOLINT
    std::size_t update_source_offset_{}; //NOLINT
    std::size_t update_source_nullity_offset_{}; //NOLINT
};

}

/**
 * @brief write operator
 */
class write_partial : public record_operator {
public:
    friend class write_partial_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    write_partial() = default;

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
    write_partial(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        std::string_view storage_name,
        std::vector<details::write_partial_field> key_fields,
        std::vector<details::write_partial_field> value_fields,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta
    ) :
        kind_(kind),
        storage_name_(storage_name),
        key_fields_(std::move(key_fields)),
        value_fields_(std::move(value_fields)),
        key_meta_(std::move(key_meta)),
        value_meta_(std::move(value_meta))
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
    write_partial(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns
    ) :
        write_partial(
            index,
            info,
            block_index,
            kind,
            storage_name,
            create_fields(kind, idx, keys, columns, info, block_index, true),
            create_fields(kind, idx, keys, columns, info, block_index, false),
            create_meta(idx, true),
            create_meta(idx, false)
        )
    {}


    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<write_partial_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<write_partial_context>(index(),
                ctx.block_scope(block_index()),
                ctx.database()->get_storage(storage_name()),
                ctx.transaction(),
                key_meta_,
                value_meta_,
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
    void operator()(write_partial_context& ctx) {
        auto source = ctx.variables().store().ref();
        auto resource = ctx.varlen_resource();
        find_record_and_extract(ctx);
        update_record(ctx);
        encode_and_put(ctx);
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::write_partial;
    }

    /**
     * @brief return storage name
     * @return the storage name of the write target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept {
        return storage_name_;
    }

    void finish(abstract::task_context* context) override {
        //no-op
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept {
        return key_meta_;
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept {
        return value_meta_;
    }
private:
    write_kind kind_{};
    std::string storage_name_{};
    std::vector<details::write_partial_field> key_fields_{};
    std::vector<details::write_partial_field> value_fields_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    maybe_shared_ptr<meta::record_meta> value_meta_{};

    void encode_fields(
        bool from_variable,
        std::vector<details::write_partial_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref source,
        memory_resource* resource
    ) {
        for(auto const& f : fields) {
            std::size_t offset = from_variable ? f.source_offset_ : f.target_offset_;
            std::size_t nullity_offset = from_variable ? f.source_nullity_offset_ : f.target_nullity_offset_;
            kvs::encode_nullable(source, offset, nullity_offset, f.type_, f.spec_, stream);
        }
    }

    maybe_shared_ptr<meta::record_meta> create_meta(yugawara::storage::index const& idx, bool for_key) {
        std::vector<meta::field_type> types{};
        boost::dynamic_bitset<std::uint64_t> nullities{};
        if (for_key) {
            for(auto&& k : idx.keys()) {
                types.emplace_back(utils::type_for(k.column().type()));
                nullities.push_back(true);
            }
        } else {
            for(auto&& v : idx.values()) {
                types.emplace_back(utils::type_for(static_cast<yugawara::storage::column const&>(v).type()));
                nullities.push_back(true);
            }
        }
        return std::make_shared<meta::record_meta>(std::move(types), std::move(nullities));
    }

    std::vector<details::write_partial_field> create_fields(
        write_kind kind,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        processor_info const& info,
        block_index_type block_index,
        bool key
    ) {
        std::vector<details::write_partial_field> ret{};
        using variable = takatori::descriptor::variable;
        yugawara::binding::factory bindings{};
        auto& block = info.scopes_info()[block_index];
        std::unordered_map<variable, variable> table_key_to_stream{};
        std::unordered_map<variable, variable> table_columns_to_stream{};
        for(auto&& c : keys) {
            table_key_to_stream.emplace(c.destination(), c.source());
        }
        for(auto&& c : columns) {
            table_columns_to_stream.emplace(c.destination(), c.source());
        }
        if (key) {
            auto meta = create_meta(idx, true);
            ret.reserve(idx.keys().size());
            for(std::size_t i=0, n=idx.keys().size(); i<n; ++i) {
                auto&& k = idx.keys()[i];
                auto kc = bindings(k.column());
                auto t = utils::type_for(k.column().type());
                auto spec = k.direction() == relation::sort_direction::ascendant ?
                    kvs::spec_key_ascending : kvs::spec_key_descending;
                if (table_key_to_stream.count(kc) == 0) {
                    fail();
                }
                auto&& var = table_key_to_stream.at(kc);
                std::size_t source_offset{block.value_map().at(var).value_offset()};
                std::size_t source_nullity_offset{block.value_map().at(var).nullity_offset()};
                bool updated = false;
                std::size_t update_source_offset{npos};
                std::size_t update_source_nullity_offset{npos};
                if (table_columns_to_stream.count(kc) != 0) {
                    updated = true;
                    auto&& src = table_columns_to_stream.at(kc);
                    update_source_offset = block.value_map().at(src).value_offset();
                    update_source_nullity_offset = block.value_map().at(src).nullity_offset();
                }
                ret.emplace_back(
                    t,
                    source_offset,
                    source_nullity_offset,
                    meta->value_offset(i),
                    meta->nullity_offset(i),
                    k.column().criteria().nullity().nullable(),
                    spec,
                    updated,
                    update_source_offset,
                    update_source_nullity_offset
                );
            }
            return ret;
        }
        auto meta = create_meta(idx, false);
        ret.reserve(idx.values().size());
        for(std::size_t i=0, n=idx.values().size(); i<n; ++i) {
            auto&& v = idx.values()[i];
            auto b = bindings(v);
            auto& c = static_cast<yugawara::storage::column const&>(v);
            auto t = utils::type_for(c.type());
            bool updated = false;
            std::size_t update_source_offset{npos};
            std::size_t update_source_nullity_offset{npos};
            if (table_columns_to_stream.count(b) != 0) {
                updated = true;
                auto&& src = table_columns_to_stream.at(b);
                update_source_offset = block.value_map().at(src).value_offset();
                update_source_nullity_offset = block.value_map().at(src).nullity_offset();
            }
            ret.emplace_back(
                t,
                npos,
                npos,
                meta->value_offset(i),
                meta->nullity_offset(i),
                c.criteria().nullity().nullable(),
                kvs::spec_value,
                updated,
                update_source_offset,
                update_source_nullity_offset
            );
        }
        return ret;
    }

    void check_length_and_extend_buffer(
        bool from_variables,
        write_partial_context& ctx,
        std::vector<details::write_partial_field> const& fields,
        data::aligned_buffer& buffer,
        accessor::record_ref source,
        memory_resource* resource
    ) {
        kvs::stream null_stream{};
        encode_fields(from_variables, fields, null_stream, source, resource);
        if (null_stream.length() > buffer.size()) {
            buffer.resize(null_stream.length());
        }
    }

    void decode_fields(
        std::vector<details::write_partial_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref target,
        memory_resource* resource
    ) {
        for(auto&& f : fields) {
            if (f.nullable_) {
                kvs::decode_nullable(stream, f.type_, f.spec_, target, f.target_offset_, f.target_nullity_offset_, resource);
                continue;
            }
            kvs::decode(stream, f.type_, f.spec_, target, f.target_offset_, resource);
            target.set_null(f.target_nullity_offset_, false); // currently assuming fields are nullable and f.nullity_offset_ is valid even if f.nullable_ is false
        }
    }

    void update_fields(
        std::vector<details::write_partial_field> const& fields,
        accessor::record_ref target,
        accessor::record_ref source
    ) {
        for(auto const& f : fields) {
            if (! f.updated_) continue;
            utils::copy_nullable_field(
                f.type_,
                target,
                f.target_offset_,
                f.target_nullity_offset_,
                source,
                f.update_source_offset_,
                f.update_source_nullity_offset_
            );
        }
    }

    void find_record_and_extract(write_partial_context& ctx) {
        auto resource = ctx.varlen_resource();
        auto key_source = ctx.key_store_.ref();
        auto val_source = ctx.value_store_.ref();
        auto k = prepare_encoded_key(ctx);
        std::string_view v{};
        if(auto res = ctx.stg_->get(
                *ctx.tx_,
                k,
                v
            ); !res) {
            //TODO handle error
            fail();
        }
        kvs::stream keys{const_cast<char*>(k.data()), k.size()};
        kvs::stream values{const_cast<char*>(v.data()), v.size()};
        decode_fields(key_fields_, keys, key_source, resource);
        decode_fields(value_fields_, values, val_source, resource);
        if(auto res = ctx.stg_->remove(
                *ctx.tx_,
                k
            ); !res) {
            //TODO handle error
            fail();
        }
    }

    void update_record(write_partial_context& ctx) {
        auto resource = ctx.varlen_resource();
        auto key_source = ctx.key_store_.ref();
        auto val_source = ctx.value_store_.ref();
        auto variables = ctx.variables().store().ref();
        {
            update_fields(key_fields_, key_source, variables);
            update_fields(value_fields_, val_source, variables);
        }
    }

    void encode_and_put(write_partial_context& ctx) {
        auto resource = ctx.varlen_resource();
        auto key_source = ctx.key_store_.ref();
        auto val_source = ctx.value_store_.ref();
        // calculate length first, then put
        check_length_and_extend_buffer(false, ctx, key_fields_, ctx.key_buf_, key_source, resource);
        check_length_and_extend_buffer(false, ctx, value_fields_, ctx.value_buf_, val_source, resource);
        auto* k = static_cast<char*>(ctx.key_buf_.data());
        auto* v = static_cast<char*>(ctx.value_buf_.data());
        kvs::stream keys{k, ctx.key_buf_.size()};
        kvs::stream values{v, ctx.value_buf_.size()};
        encode_fields(false, key_fields_, keys, key_source, resource);
        encode_fields(false, value_fields_, values, val_source, resource);
        if(auto res = ctx.stg_->put(
                *ctx.tx_,
                {k, keys.length()},
                {v, values.length()}
            ); !res) {
            fail();
        }
    }

    std::string_view prepare_encoded_key(write_partial_context& ctx) {
        auto source = ctx.variables().store().ref();
        auto resource = ctx.varlen_resource();
        // calculate length first, and then put
        check_length_and_extend_buffer(true, ctx, key_fields_, ctx.key_buf_, source, resource);
        auto* k = static_cast<char*>(ctx.key_buf_.data());
        kvs::stream keys{k, ctx.key_buf_.size()};
        encode_fields(true, key_fields_, keys, source, resource);
        return {k, keys.length()};
    }
};

}
