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

#include <takatori/util/optional_ptr.h>
#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>
#include <takatori/statement/write.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/error.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/data/aligned_buffer.h>

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using jogasaki::executor::process::impl::expression::evaluator;
using yugawara::compiled_info;

using takatori::util::unsafe_downcast;
using takatori::util::fail;

write::write(
    write_kind kind,
    std::string_view storage_name,
    std::vector<details::write_tuple> keys,
    std::vector<details::write_tuple> values
) noexcept:
    kind_(kind),
    storage_name_(storage_name),
    keys_(std::move(keys)),
    values_(std::move(values))
{}

write::write(
    write_kind kind,
    std::string_view storage_name,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    memory::lifo_paged_memory_resource& resource,
    compiled_info const& info
) noexcept:
    write(
        kind,
        storage_name,
        create_tuples(idx, columns, tuples, info, resource, true),
        create_tuples(idx, columns, tuples, info, resource, false)
    )
{}

model::statement_kind write::kind() const noexcept {
    return model::statement_kind::write;
}

bool write::operator()(request_context& context) const {
    auto& tx = context.transaction();
    auto* db = tx->database();
    auto stg = db->get_storage(storage_name_);
    if(! stg) {
        fail();
    }

    // TODO is there the case insert_or_update?
    kvs::put_option opt = kind_ == write_kind::insert ?
        kvs::put_option::create :
        kvs::put_option::create_or_update;
    BOOST_ASSERT(keys_.size() == values_.size());  //NOLINT
    for(std::size_t i=0, n=keys_.size(); i<n; ++i) {
        auto& key = keys_[i];
        auto& value = values_[i];
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
    return true;
}

std::vector<details::write_tuple> write::create_tuples(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<tuple> const& tuples,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    bool key
) {
    std::vector<details::write_tuple> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, std::size_t> variable_indices{};
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto&& c = columns[i];
        variable_indices[c]=i;
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
    for(auto& tuple: tuples) {
        auto s = encode_tuple(tuple, fields, info, resource);
        ret.emplace_back(s);
    }
    return ret;
}

std::string write::encode_tuple(
    write::tuple const& t,
    std::vector<details::write_field> const& fields,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource
) {
    BOOST_ASSERT(fields.size() <= t.elements().size());  //NOLINT
    auto cp = resource.get_checkpoint();
    executor::process::impl::block_scope scope{};
    std::string buf{};  //TODO create own buffer class
    for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
        auto capacity = loop == 0 ? 0 : buf.capacity(); // capacity 0 makes stream empty write to calc. length
        kvs::stream s{buf.data(), capacity};
        for(auto&& f : fields) {
            evaluator eval{t.elements()[f.index_], info};
            auto res = eval(scope, &resource);

            if (f.nullable_) {
                kvs::encode_nullable(res, f.type_, f.spec_, s);
            } else {
                kvs::encode(res, f.type_, f.spec_, s);
            }
            resource.deallocate_after(cp);
        }
        if (loop == 0) {
            buf.resize(s.length());
        }
    }
    return buf;
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
