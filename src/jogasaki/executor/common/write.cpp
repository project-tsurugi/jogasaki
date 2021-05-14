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
#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/writable_stream.h>

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using jogasaki::executor::process::impl::expression::evaluator;
using yugawara::compiled_info;

using takatori::util::fail;

write::write(
    write_kind kind,
    std::vector<details::write_target> targets
) noexcept:
    kind_(kind),
    targets_(std::move(targets))
{}

write::write(
    write_kind kind,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    memory::lifo_paged_memory_resource& resource,
    compiled_info const& info,
    executor::process::impl::variable_table const* host_variables
) noexcept:
    write(
        kind,
        create_targets(idx, columns, tuples, info, resource, host_variables)
    )
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
    for(auto&& e : targets_) {
        auto stg = db->get_storage(e.storage_name_);
        if(! stg) {
            stg = db->create_storage(e.storage_name_);
        }
        if(! stg) {
            fail();
        }
        BOOST_ASSERT(e.keys_.size() == e.values_.size() || e.values_.empty());  //NOLINT
        for(std::size_t i=0, n=e.keys_.size(); i<n; ++i) {
            auto& key = e.keys_[i];
            auto& value = e.values_.empty() ? details::write_tuple{} : e.values_[i];
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

// encode tuple into buf, and return result data length
std::size_t encode_tuple(
    write::tuple const& t,
    std::vector<details::write_field> const& fields,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& buf,
    executor::process::impl::variable_table const* host_variables,
    details::write_tuple const* primary_key_tuple = nullptr
) {
    BOOST_ASSERT(fields.size() <= t.elements().size());  //NOLINT
    utils::checkpoint_holder cph(std::addressof(resource));
    std::size_t length = 0;
    for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
        auto capacity = loop == 0 ? 0 : buf.size(); // capacity 0 makes stream empty write to calc. length
        kvs::writable_stream s{buf.data(), capacity};
        for(auto&& f : fields) {
            if (f.index_ == npos) {
                // value not specified for the field
                if (! f.nullable_) {
                    fail();
                }
                kvs::encode_nullable({}, f.type_, f.spec_, s);
            } else {
                evaluator eval{t.elements()[f.index_], info, host_variables};
                process::impl::variable_table empty{};
                auto res = eval(empty, &resource);

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

std::vector<details::write_tuple> write::create_tuples(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    bool key,
    std::vector<details::write_tuple> const& primary_key_tuples
) {
    std::vector<details::write_tuple> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, std::size_t> variable_indices{};
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto&& c = columns[i];
        variable_indices[c] = i;
    }
    std::vector<details::write_field> fields{};
    if (key) {
        fields.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto v = bindings(k.column());
            std::size_t index{npos};
            if(variable_indices.count(v) != 0) {
                index = variable_indices[v];
            }
            fields.emplace_back(
                index,
                utils::type_for(k.column().type()),
                k.direction() == takatori::relation::sort_direction::ascendant ?
                    kvs::spec_key_ascending: kvs::spec_key_descending,
                k.column().criteria().nullity().nullable()
            );
        }
    } else {
        fields.reserve(idx.values().size());
        for(auto&& c : idx.values()) {
            auto v = bindings(c);
            std::size_t index{npos};
            if(variable_indices.count(v) != 0) {
                index = variable_indices[v];
            }
            auto& casted = static_cast<yugawara::storage::column const&>(c);
            fields.emplace_back(
                index,
                utils::type_for(casted.type()),
                kvs::spec_value,
                casted.criteria().nullity().nullable()
            );
        }
    }
    data::aligned_buffer buf{};
    std::size_t count = 0;
    for(auto&& tuple: tuples) {
        auto sz = encode_tuple(tuple, fields, info, resource, buf, host_variables, primary_key_tuples.empty() ? nullptr : &primary_key_tuples[count]);
        std::string_view sv{static_cast<char*>(buf.data()), sz};
        ret.emplace_back(sv);
        ++count;
    }
    return ret;
}

std::vector<details::write_target> write::create_targets(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables
) {
    std::vector<details::write_target> ret{};
    auto& table = idx.table();
    auto primary = table.owner()->find_primary_index(table);
    BOOST_ASSERT(primary != nullptr); //NOLINT
    // first entry is primary index
    auto& t = ret.emplace_back(
        primary->simple_name(),
        create_tuples(*primary, columns, tuples, info, resource, host_variables, true),
        create_tuples(*primary, columns, tuples, info, resource, host_variables, false)
    );
    auto& keys = t.keys_;
    table.owner()->each_index(
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (entry->table() != table || entry == primary) {
                return;
            }
            ret.emplace_back(
                entry->simple_name(),
                create_tuples(*entry, columns, tuples, info, resource, host_variables, true, keys),
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
