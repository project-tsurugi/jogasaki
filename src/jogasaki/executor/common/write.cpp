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
#include "write.h"

#include <takatori/util/fail.h>
#include <takatori/statement/write.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/error.h>
#include <jogasaki/logging.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/coder.h>
#include <jogasaki/utils/convert_any.h>

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using jogasaki::executor::process::impl::expression::evaluator;
using yugawara::compiled_info;

using takatori::util::fail;
using executor::process::impl::expression::any;
using executor::process::impl::expression::index;

write::write(
    write_kind kind,
    yugawara::storage::index const& idx,
    takatori::statement::write const& wrt,
    memory::lifo_paged_memory_resource& resource,
    compiled_info info,
    executor::process::impl::variable_table const* host_variables
) noexcept:
    kind_(kind),
    idx_(std::addressof(idx)),
    wrt_(std::addressof(wrt)),
    resource_(std::addressof(resource)),
    info_(std::move(info)),
    host_variables_(host_variables)
{}

model::statement_kind write::kind() const noexcept {
    return model::statement_kind::write;
}

bool write::operator()(request_context& context) const {
    auto& tx = context.transaction();
    auto* db = tx->database();
    // TODO is there the case insert_or_update?
    kvs::put_option opt = kind_ == write_kind::insert ?
        kvs::put_option::create :
        kvs::put_option::create_or_update;
    auto targets = create_targets(context, *idx_, wrt_->columns(), wrt_->tuples(), info_, *resource_, host_variables_);
    for(auto&& e : targets) {
        auto stg = db->get_or_create_storage(e.storage_name_);
        if(! stg) {
            fail();
        }
        BOOST_ASSERT(e.keys_.size() == e.values_.size() || e.values_.empty());  //NOLINT
        for(std::size_t i=0, n=e.keys_.size(); i<n; ++i) {
            auto& key = e.keys_[i];
            auto const& value = e.values_.empty() ? details::write_tuple{} : e.values_[i];
            if(auto res = stg->put(
                    *tx,
                    {static_cast<char*>(key.data()), key.size()},
                    {static_cast<char*>(value.data()), value.size()},
                    opt
                ); ! is_ok(res)) {
                context.status_code(res);
                return false;
            }
        }
    }
    return true;
}

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

sequence_value next_sequence_value(request_context& ctx, sequence_definition_id def_id) {
    BOOST_ASSERT(ctx.sequence_manager() != nullptr); //NOLINT
    auto& mgr = *ctx.sequence_manager();
    auto* seq = mgr.find_sequence(def_id);
    return seq->next(*ctx.transaction());
}

// encode tuple into buf, and return result data length
std::size_t encode_tuple(
    request_context& ctx,
    write::tuple const& t,
    std::vector<details::write_field> const& fields,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& buf,
    executor::process::impl::variable_table const* host_variables,
    details::write_tuple const* primary_key_tuple = nullptr
) {
    utils::checkpoint_holder cph(std::addressof(resource));
    std::size_t length = 0;
    for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
        auto capacity = loop == 0 ? 0 : buf.size(); // capacity 0 makes stream empty write to calc. length
        kvs::writable_stream s{buf.data(), capacity};
        for(auto&& f : fields) {
            if (f.index_ == npos) {
                // value not specified for the field use default value or null
                switch(f.kind_) {
                    case process::impl::ops::default_value_kind::nothing:
                        if (! f.nullable_) {
                            fail();
                        }
                        kvs::encode_nullable({}, f.type_, f.spec_, s);
                        break;
                    case process::impl::ops::default_value_kind::immediate: {
                        auto d = f.default_value_;
                        s.write(static_cast<char const*>(d.data()), d.size());
                        break;
                    }
                    case process::impl::ops::default_value_kind::sequence:
                        auto v = next_sequence_value(ctx, f.def_id_);
                        executor::process::impl::expression::any a{std::in_place_type<std::int64_t>, v};
                        if (f.nullable_) {
                            kvs::encode_nullable(a, f.type_, f.spec_, s);
                        } else {
                            kvs::encode(a, f.type_, f.spec_, s);
                        }
                        break;
                }
            } else {
                evaluator eval{t.elements()[f.index_], info, host_variables};
                process::impl::variable_table empty{};
                auto res = eval(empty, &resource);
                if (res.error()) {
                    VLOG(log_error) << "evaluation error: " << res.to<process::impl::expression::error>();
                    //TODO fill status code 
                }
                if(! utils::convert_any(res, f.type_)) {
                    VLOG(log_error) << "type mismatch: expected " << f.type_ << ", value index is " << res.type_index();
                    //TODO fill status code 
                }
                if (f.nullable_) {
                    kvs::encode_nullable(res, f.type_, f.spec_, s);
                } else {
                    kvs::encode(res, f.type_, f.spec_, s);
                }
                cph.reset();
            }
        }
        if (primary_key_tuple != nullptr) {
            s.write(static_cast<char*>(primary_key_tuple->data()), primary_key_tuple->size());
        }
        if (loop == 0) {
            length = s.size();
            if (buf.size() < length) {
                buf.resize(length);
            }
        }
    }
    return length;
}

void create_generated_field(
    std::vector<details::write_field>& ret,
    std::size_t index,
    yugawara::storage::column_value const& dv,
    takatori::type::data const& type,
    bool nullable,
    kvs::coding_spec spec
) {
    using yugawara::storage::column_value_kind;
    sequence_definition_id def_id{};
    data::aligned_buffer buf{};
    auto t = utils::type_for(type);
    auto knd = process::impl::ops::default_value_kind::nothing;
    switch(dv.kind()) {
        case column_value_kind::nothing:
            break;
        case column_value_kind::immediate: {
            knd = process::impl::ops::default_value_kind::immediate;
            auto src = utils::as_any(
                *dv.element<column_value_kind::immediate>(),
                type,
                nullptr
            );
            utils::encode_any( buf, t, nullable, spec, {src});
            break;
        }
        case column_value_kind::sequence: {
            knd = process::impl::ops::default_value_kind::sequence;
            if (auto id = dv.element<column_value_kind::sequence>()->definition_id()) {
                def_id = *id;
            } else {
                fail("sequence must be defined with definition_id");
            }
        }
    }
    ret.emplace_back(
        index,
        t,
        spec,
        nullable,
        knd,
        static_cast<std::string_view>(buf),
        def_id
    );
}

std::vector<details::write_field> write::create_fields(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    bool key
) const {
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::vector<details::write_field> fields{};
    std::unordered_map<variable, std::size_t> variable_indices{};
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto&& c = columns[i];
        variable_indices[c] = i;
    }
    if (key) {
        fields.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto kc = bindings(k.column());
            auto& type = k.column().type();
            auto t = utils::type_for(type);
            auto spec = k.direction() == takatori::relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            bool nullable = k.column().criteria().nullity().nullable();
            if(variable_indices.count(kc) == 0) {
                // no column specified - use default value
                auto& dv = k.column().default_value();
                create_generated_field(fields, npos, dv, type, nullable, spec);
                continue;
            }
            fields.emplace_back(
                variable_indices[kc],
                t,
                spec,
                nullable
            );
        }
    } else {
        fields.reserve(idx.values().size());
        for(auto&& v : idx.values()) {
            auto b = bindings(v);

            auto& c = static_cast<yugawara::storage::column const&>(v);
            auto& type = c.type();
            auto t = utils::type_for(type);
            bool nullable = c.criteria().nullity().nullable();
            if(variable_indices.count(b) == 0) {
                // no column specified - use default value
                auto& dv = c.default_value();
                create_generated_field(fields, npos, dv, type, nullable, kvs::spec_value);
                continue;
            }
            fields.emplace_back(
                variable_indices[b],
                t,
                kvs::spec_value,
                nullable
            );
        }
    }
    return fields;
}

std::vector<details::write_tuple> write::create_tuples(
    request_context& ctx,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    bool key,
    std::vector<details::write_tuple> const& primary_key_tuples
) const {
    std::vector<details::write_tuple> ret{};
    auto fields = create_fields(idx, columns, key);
    data::aligned_buffer buf{};
    std::size_t count = 0;
    for(auto&& tuple: tuples) {
        auto sz = encode_tuple(ctx, tuple, fields, info, resource, buf, host_variables, primary_key_tuples.empty() ? nullptr : &primary_key_tuples[count]);
        std::string_view sv{static_cast<char*>(buf.data()), sz};
        ret.emplace_back(sv);
        ++count;
    }
    return ret;
}

std::vector<details::write_target> write::create_targets(
    request_context& ctx,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables
) const {
    std::vector<details::write_target> ret{};
    auto& table = idx.table();
    auto primary = table.owner()->find_primary_index(table);
    BOOST_ASSERT(primary != nullptr); //NOLINT
    // first entry is primary index
    ret.emplace_back(
        primary->simple_name(),
        create_tuples(ctx, *primary, columns, tuples, info, resource, host_variables, true),
        create_tuples(ctx, *primary, columns, tuples, info, resource, host_variables, false)
    );
    table.owner()->each_index(
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (entry->table() != table || entry == primary) {
                return;
            }
            ret.emplace_back(
                entry->simple_name(),
                create_tuples(ctx, *entry, columns, tuples, info, resource, host_variables, true, ret[0].keys_),
                std::vector<details::write_tuple>{}
            );
        }
    );
    return ret;
}

details::write_tuple::write_tuple(std::string_view data) :
    buf_(data.size())
{
    std::memcpy(buf_.data(), data.data(), data.size());
}

void* details::write_tuple::data() const noexcept {
    return buf_.data();
}

std::size_t details::write_tuple::size() const noexcept {
    return buf_.size();
}

}
