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
#include <jogasaki/utils/storage_utils.h>

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
    (void)kind_;
    auto& tx = context.transaction();
    auto* db = tx->database();
    std::vector<details::write_target> targets{};
    if(auto res = create_targets(context, *idx_, wrt_->columns(), wrt_->tuples(),
            info_, *resource_, host_variables_, targets); res != status::ok) {
        context.status_code(res);
        return false;
    }
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
                    kvs::put_option::create  // assuming this class is for Insert only
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
    auto ret = seq->next(*ctx.transaction());
    mgr.notify_updates(*ctx.transaction());
    return ret;
}

// encode tuple into buf, and return result data length
status encode_tuple(
    request_context& ctx,
    write::tuple const& t,
    std::vector<details::write_field> const& fields,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& buf,
    executor::process::impl::variable_table const* host_variables,
    std::size_t& length,
    details::write_tuple const* primary_key_tuple = nullptr
) {
    utils::checkpoint_holder cph(std::addressof(resource));
    length = 0;
    for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
        auto capacity = loop == 0 ? 0 : buf.size(); // capacity 0 makes stream empty write to calc. length
        kvs::writable_stream s{buf.data(), capacity};
        for(auto&& f : fields) {
            if (f.index_ == npos) {
                // value not specified for the field use default value or null
                switch(f.kind_) {
                    case process::impl::ops::default_value_kind::nothing:
                        if (! f.nullable_) {
                            VLOG(log_error) << "Null assigned for non-nullable field.";
                            return status::err_integrity_constraint_violation;
                        }
                        kvs::encode_nullable({}, f.type_, f.spec_, s);
                        break;
                    case process::impl::ops::default_value_kind::immediate: {
                        auto d = f.default_value_;
                        s.write(static_cast<char const*>(d.data()), d.size());
                        break;
                    }
                    case process::impl::ops::default_value_kind::sequence:
                        // increment sequence only in the second loop
                        auto v = loop == 1 ? next_sequence_value(ctx, f.def_id_) : sequence_value{};
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
                    return status::err_expression_evaluation_failure;
                }
                if(! utils::convert_any(res, f.type_)) {
                    VLOG(log_error) << "type mismatch: expected " << f.type_ << ", value index is " << res.type_index();
                    return status::err_expression_evaluation_failure;
                }
                if (f.nullable_) {
                    kvs::encode_nullable(res, f.type_, f.spec_, s);
                } else {
                    if(! res) {
                        VLOG(log_error) << "Null assigned for non-nullable field.";
                        return status::err_integrity_constraint_violation;
                    }
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
    return status::ok;
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
    using reference = takatori::descriptor::variable::reference_type;
    yugawara::binding::factory bindings{};
    std::vector<details::write_field> fields{};
    std::unordered_map<reference, std::size_t> variable_indices{};
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto&& c = columns[i];
        variable_indices[c.reference()] = i;
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
            if(variable_indices.count(kc.reference()) == 0) {
                // no column specified - use default value
                auto& dv = k.column().default_value();
                create_generated_field(fields, npos, dv, type, nullable, spec);
                continue;
            }
            fields.emplace_back(
                variable_indices[kc.reference()],
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
            if(variable_indices.count(b.reference()) == 0) {
                // no column specified - use default value
                auto& dv = c.default_value();
                create_generated_field(fields, npos, dv, type, nullable, kvs::spec_value);
                continue;
            }
            fields.emplace_back(
                variable_indices[b.reference()],
                t,
                kvs::spec_value,
                nullable
            );
        }
    }
    return fields;
}

status write::create_tuples(
    request_context& ctx,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    bool key,
    std::vector<details::write_tuple>& out,
    std::vector<details::write_tuple> const& primary_key_tuples
) const {
    auto fields = create_fields(idx, columns, key);
    data::aligned_buffer buf{};
    std::size_t count = 0;
    out.clear();
    out.reserve(tuples.size());
    for(auto&& tuple: tuples) {
        std::size_t sz = 0;
        if(auto res = encode_tuple(ctx, tuple, fields, info, resource, buf, host_variables, sz,
            primary_key_tuples.empty() ? nullptr : &primary_key_tuples[count]); res != status::ok) {
            return res;
        }
        std::string_view sv{static_cast<char*>(buf.data()), sz};
        out.emplace_back(sv);
        ++count;
    }
    return status::ok;
}

status write::create_targets(
    request_context& ctx,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    std::vector<details::write_target>& out
) const {
    out.clear();
    out.reserve(5);  // approx. number for primary+secondary indices for a table
    auto& table = idx.table();
    auto primary = table.owner()->find_primary_index(table);
    BOOST_ASSERT(primary != nullptr); //NOLINT
    std::vector<details::write_tuple> ks{};
    if (auto res = create_tuples(ctx, *primary, columns, tuples, info, resource, host_variables, true, ks);
        res != status::ok) {
        return res;
    }
    std::vector<details::write_tuple> vs{};
    if (auto res = create_tuples(ctx, *primary, columns, tuples, info, resource, host_variables, false, vs);
        res != status::ok) {
        return res;
    }
    // first entry is primary index
    out.emplace_back(primary->simple_name(), std::move(ks), std::move(vs));

    status ret_status{status::ok};
    table.owner()->each_table_index(table,
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (ret_status != status::ok) return;
            if (entry == primary) {
                return;
            }
            std::vector<details::write_tuple> ts{};
            if (auto res = create_tuples(ctx, *entry, columns, tuples, info, resource,
                    host_variables, true, ts, out[0].keys_); res != status::ok) {
                ret_status = res;
                return;
            }
            out.emplace_back(
                entry->simple_name(),
                std::move(ts),
                std::vector<details::write_tuple>{}
            );
        }
    );
    return ret_status;
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
