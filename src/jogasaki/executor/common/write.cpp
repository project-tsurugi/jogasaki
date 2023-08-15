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

#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/data/any.h>
#include <jogasaki/constants.h>
#include <jogasaki/error.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/index/utils.h>
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

using takatori::util::throw_exception;
using takatori::util::string_builder;
using data::any;

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

void abort_transaction(transaction_context& tx) {
    if (auto res = tx.abort(); res != status::ok) {
        throw_exception(std::logic_error{"abort failed unexpectedly"});
    }
}

bool write::operator()(request_context& context) const {  //NOLINT(readability-function-cognitive-complexity)
    auto& tx = context.transaction();
    BOOST_ASSERT(tx);  //NOLINT
    auto* db = tx->database();
    std::vector<details::write_target> targets{};
    if(auto res = create_targets(context, *idx_, wrt_->columns(), wrt_->tuples(),
            info_, *resource_, host_variables_, targets); res != status::ok) {
        // Errors from create_targets() are in SQL layer, so we need to abort tx manually.
        // An exception is sequence generation error which can abort tx,
        // even so, aborting again will do no harm since sharksfin tx manages is_active flag and omits aborting again.
        abort_transaction(*tx);
        context.status_code(res);
        return false;
    }
    for(auto&& e : targets) {
        auto stg = db->get_or_create_storage(e.storage_name_);
        if(! stg) {
            throw_exception(std::logic_error{""});
        }
        kvs::put_option opt = ((kind_ == write_kind::insert || kind_ == write_kind::insert_skip) && e.primary_) ?
            kvs::put_option::create :
            kvs::put_option::create_or_update;
        // TODO for insert_overwrite, update secondary first
        BOOST_ASSERT(e.keys_.size() == e.values_.size() || e.values_.empty());  //NOLINT
        for(std::size_t i=0, n=e.keys_.size(); i<n; ++i) {
            auto& key = e.keys_[i];
            details::write_tuple empty{};
            auto const* value = e.values_.empty() ? &empty : &e.values_[i];
            if(auto res = stg->put(
                    *tx,
                    {static_cast<char*>(key.data()), key.size()},
                    {static_cast<char*>(value->data()), value->size()},
                    opt
                ); res != status::ok) {
                if (opt == kvs::put_option::create && res == status::already_exists) {
                    if(kind_ == write_kind::insert) {
                        // integrity violation should be handled in SQL layer and forces transaction abort
                        // status::already_exists is an internal code, raise it as constraint violation
                        set_tx_error(
                            context,
                            error_code::unique_constraint_violation_exception,
                            string_builder{} <<
                                "Unique constraint violation occurred. Table:" << e.storage_name_ << string_builder::to_string,
                            status::err_unique_constraint_violation
                        );
                        abort_transaction(*tx);
                        return false;
                    }
                    // write_kind::insert_skip
                    // duplicated key is simply ignored

                    // currently this is for Load operation and assuming single tuple insert
                    // TODO skip tuples for secondary index and move to next tuple for primary
                    continue;
                }
                // TODO error handling for secondary index, multiple tuples
                if(res == status::err_serialization_failure) {
                    set_tx_error(
                        context,
                        error_code::cc_exception,
                        string_builder{} <<
                            "Serialization failed. " << string_builder::to_string,
                        res
                    );
                    return false;
                }
                set_tx_error(
                    context,
                    error_code::sql_service_exception,
                    string_builder{} <<
                        "Unexpected error occurred. status:" << res << string_builder::to_string,
                    res
                );
                return false;
            }
        }
    }
    return true;
}

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

status next_sequence_value(request_context& ctx, sequence_definition_id def_id, sequence_value& out) {
    BOOST_ASSERT(ctx.sequence_manager() != nullptr); //NOLINT
    auto& mgr = *ctx.sequence_manager();
    auto* seq = mgr.find_sequence(def_id);
    if(seq == nullptr) {
        throw_exception(std::logic_error{""});
    }
    auto ret = seq->next(*ctx.transaction()->object());
    try {
        mgr.notify_updates(*ctx.transaction()->object());
    } catch(executor::sequence::exception const& e) {
        return e.get_status();
    }
    out = ret;
    return status::ok;
}

void handle_encode_error(
    request_context& ctx,
    status st
) {
    if(st == status::err_data_corruption) {
        set_tx_error(
            ctx,
            error_code::data_corruption_exception,
            string_builder{} <<
                "Data inconsistency detected." << string_builder::to_string,
            st
        );
        return;
    }
    if(st == status::err_expression_evaluation_failure) {
        set_tx_error(
            ctx,
            error_code::value_evaluation_exception,
            string_builder{} <<
                "An error occurred in evaluating values. Encoding failed." << string_builder::to_string,
            st
        );
        return;
    }
    set_tx_error(
        ctx,
        error_code::sql_service_exception,
        string_builder{} <<
            "Unexpected error occurred." << string_builder::to_string,
        st
    );
}
// encode tuple into buf, and return result data length
status encode_tuple(  //NOLINT(readability-function-cognitive-complexity)
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

    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `buf`, extend it and retry
        kvs::writable_stream s{buf.data(), buf.capacity(), loop == 0};
        for(auto&& f : fields) {
            if (f.index_ == npos) {
                // value not specified for the field use default value or null
                switch(f.kind_) {
                    case process::impl::ops::default_value_kind::nothing:
                        if (! f.nullable_) {
                            set_tx_error(
                                ctx,
                                error_code::not_null_constraint_violation_exception,
                                string_builder{} <<
                                    "Null assigned for non-nullable field." << string_builder::to_string,
                                status::err_integrity_constraint_violation
                            );
                            return status::err_integrity_constraint_violation;
                        }
                        if(auto res = kvs::encode_nullable({}, f.type_, f.spec_, s); res != status::ok) {
                            handle_encode_error(ctx, res);
                            return res;
                        }
                        break;
                    case process::impl::ops::default_value_kind::immediate: {
                        auto& d = f.default_value_;
                        if(auto res = s.write(static_cast<char const*>(d.data()), d.size()); res != status::ok) {
                            return res;
                        }
                        break;
                    }
                    case process::impl::ops::default_value_kind::sequence:
                        // increment sequence - loop might increment the sequence twice
                        sequence_value v{};
                        if(auto res = next_sequence_value(ctx, f.def_id_, v); res != status::ok) {
                            return res;
                        }
                        any a{std::in_place_type<std::int64_t>, v};
                        if (f.nullable_) {
                            if(auto res = kvs::encode_nullable(a, f.type_, f.spec_, s); res != status::ok) {
                                handle_encode_error(ctx, res);
                                return res;
                            }
                        } else {
                            if(auto res = kvs::encode(a, f.type_, f.spec_, s); res != status::ok) {
                                handle_encode_error(ctx, res);
                                return res;
                            }
                        }
                        break;
                }
            } else {
                evaluator eval{t.elements()[f.index_], info, host_variables};
                process::impl::variable_table empty{};
                process::impl::expression::evaluator_context c{};
                auto res = eval(c, empty, &resource);
                if (res.error()) {
                    auto rc = status::err_expression_evaluation_failure;
                    set_tx_error(
                        ctx,
                        error_code::value_evaluation_exception,
                        string_builder{} <<
                            "An error occurred in evaluating values. error:" << res.to<process::impl::expression::error>() << string_builder::to_string,
                        rc
                    );
                    return rc;
                }
                if(! utils::convert_any(res, f.type_)) {
                    auto rc = status::err_expression_evaluation_failure;
                    set_tx_error(
                        ctx,
                        error_code::value_evaluation_exception,
                        string_builder{} <<
                            "An error occurred in evaluating values. type mismatch: expected " << f.type_ << ", value index is " << res.type_index() << string_builder::to_string,
                        rc
                    );
                    return rc;
                }
                if (f.nullable_) {
                    if(auto rc = kvs::encode_nullable(res, f.type_, f.spec_, s); rc != status::ok) {
                        handle_encode_error(ctx, rc);
                        return rc;
                    }
                } else {
                    if(! res) {
                        auto rc = status::err_integrity_constraint_violation;
                        set_tx_error(
                            ctx,
                            error_code::not_null_constraint_violation_exception,
                            string_builder{} <<
                                "Null assigned for non-nullable field." << string_builder::to_string,
                            rc
                        );
                        return rc;
                    }
                    if(auto rc = kvs::encode(res, f.type_, f.spec_, s); rc != status::ok) {
                        handle_encode_error(ctx, rc);
                        return rc;
                    }
                }
                cph.reset();
            }
        }
        if (primary_key_tuple != nullptr) {
            if(auto res = s.write(static_cast<char*>(primary_key_tuple->data()), primary_key_tuple->size());
                res != status::ok) {
                handle_encode_error(ctx, res);
                return res;
            }
        }
        length = s.size();
        bool fit = length <= buf.capacity();
        buf.resize(length);
        if (loop == 0) {
            if (fit) {
                break;
            }
            buf.resize(0); // set data size 0 and start from beginning
        }
    }
    return status::ok;
}

status create_generated_field(
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
            if(auto res = utils::encode_any(buf, t, nullable, spec, {src}); res != status::ok) {
                return res;
            }
            break;
        }
        case column_value_kind::sequence: {
            knd = process::impl::ops::default_value_kind::sequence;
            if (auto id = dv.element<column_value_kind::sequence>()->definition_id()) {
                def_id = *id;
            } else {
                throw_exception(std::logic_error{"sequence must be defined with definition_id"});
            }
        }
    }
    ret.emplace_back(
        index,
        t,
        spec,
        nullable,
        knd,
        std::move(buf),
        def_id
    );
    return status::ok;
}

status write::create_fields(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    bool key,
    std::vector<details::write_field>& out
) const {
    using reference = takatori::descriptor::variable::reference_type;
    yugawara::binding::factory bindings{};
    std::unordered_map<reference, std::size_t> variable_indices{};
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto&& c = columns[i];
        variable_indices[c.reference()] = i;
    }
    if (key) {
        out.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto kc = bindings(k.column());
            auto& type = k.column().type();
            auto t = utils::type_for(type);
            auto spec = k.direction() == takatori::relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            // pass storage spec with fields for write
            spec.storage(index::extract_storage_spec(type));
            bool nullable = k.column().criteria().nullity().nullable();
            if(variable_indices.count(kc.reference()) == 0) {
                // no column specified - use default value
                auto& dv = k.column().default_value();
                if(auto res = create_generated_field(out, npos, dv, type, nullable, spec); res != status::ok) {
                    return res;
                }
                continue;
            }
            out.emplace_back(
                variable_indices[kc.reference()],
                t,
                spec,
                nullable
            );
        }
    } else {
        out.reserve(idx.values().size());
        for(auto&& v : idx.values()) {
            auto b = bindings(v);

            auto& c = static_cast<yugawara::storage::column const&>(v);
            auto& type = c.type();
            auto t = utils::type_for(type);
            bool nullable = c.criteria().nullity().nullable();
            auto spec = kvs::spec_value;
            // pass storage spec with fields for write
            spec.storage(index::extract_storage_spec(type));
            if(variable_indices.count(b.reference()) == 0) {
                // no column specified - use default value
                auto& dv = c.default_value();
                if(auto res = create_generated_field(out, npos, dv, type, nullable, spec); res != status::ok) {
                    return res;
                }
                continue;
            }
            out.emplace_back(
                variable_indices[b.reference()],
                t,
                spec,
                nullable
            );
        }
    }
    return status::ok;
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
    std::vector<details::write_field> fields{};
    if(auto res = create_fields(idx, columns, key, fields); res != status::ok) {
        return res;
    }
    data::aligned_buffer buf{default_record_buffer_size};
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
    out.reserve(approx_index_count_per_table);
    auto& table = idx.table();
    auto primary = table.owner()->find_primary_index(table);
    BOOST_ASSERT(primary != nullptr); //NOLINT
    std::vector<details::write_tuple> ks{};
    if (auto res = create_tuples(ctx, *primary, columns, tuples, info, resource, host_variables, true, ks);
        res != status::ok) {
        handle_encode_error(ctx, res);
        return res;
    }
    std::vector<details::write_tuple> vs{};
    if (auto res = create_tuples(ctx, *primary, columns, tuples, info, resource, host_variables, false, vs);
        res != status::ok) {
        handle_encode_error(ctx, res);
        return res;
    }
    // first entry is primary index
    out.emplace_back(true, primary->simple_name(), std::move(ks), std::move(vs));

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
                false,
                entry->simple_name(),
                std::move(ts),
                std::vector<details::write_tuple>{}
            );
        }
    );
    if(ret_status != status::ok) {
        handle_encode_error(ctx, ret_status);
    }
    return ret_status;
}

details::write_tuple::write_tuple(std::string_view data) :
    buf_(data.size())
{
    std::memcpy(buf_.data(), data.data(), data.size());
    buf_.resize(data.size());
}

void* details::write_tuple::data() const noexcept {
    return buf_.data();
}

std::size_t details::write_tuple::size() const noexcept {
    return buf_.size();
}

details::write_tuple::operator std::string_view() const noexcept {
    return static_cast<std::string_view>(buf_);
}

}
