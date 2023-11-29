/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <jogasaki/executor/process/impl/ops/details/write_primary_target.h>
#include <jogasaki/executor/process/impl/ops/details/write_secondary_target.h>
#include <jogasaki/executor/process/impl/ops/details/write_primary_context.h>
#include <jogasaki/executor/process/impl/ops/details/write_secondary_context.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/coder.h>
#include <jogasaki/utils/convert_any.h>
#include <jogasaki/utils/storage_utils.h>

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using jogasaki::executor::process::impl::expression::evaluator;

using takatori::util::throw_exception;
using takatori::util::string_builder;
using data::any;

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

// fwd declaration
status next_sequence_value(request_context& ctx, sequence_definition_id def_id, sequence_value& out);
void handle_encode_error(
    request_context& ctx,
    status st
);
std::vector<details::write_field> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<write::column const> columns,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    bool key
);
write_primary_target create_primary_target(
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    std::vector<details::write_field> const& key_fields,
    std::vector<details::write_field> const& value_fields
);
std::vector<write_secondary_target> create_secondary_targets(
    yugawara::storage::index const& idx,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta
);

status create_record_from_tuple(  //NOLINT(readability-function-cognitive-complexity)
    request_context& ctx,
    write::tuple const& t,
    std::vector<details::write_field> const& fields,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    data::small_record_store& out
) {
    for (auto&& f: fields) {
        if (f.index_ == npos) {
            // value not specified for the field use default value or null
            switch (f.kind_) {
                case process::impl::ops::default_value_kind::nothing:
                    if (! f.nullable_) {
                        set_error(
                            ctx,
                            error_code::not_null_constraint_violation_exception,
                            string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                            status::err_integrity_constraint_violation);
                        return status::err_integrity_constraint_violation;
                    }
                    out.ref().set_null(f.nullity_offset_, true);
                    break;
                case process::impl::ops::default_value_kind::immediate: {
                    auto src = f.default_value_immediate_;
                    auto is_null = src.empty();
                    if (is_null && !f.nullable_) {
                        auto rc = status::err_integrity_constraint_violation;
                        set_error(
                            ctx,
                            error_code::not_null_constraint_violation_exception,
                            string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                            rc);
                        return rc;
                    }
                    out.ref().set_null(f.nullity_offset_, is_null);
                    if (f.nullable_) {
                        utils::copy_nullable_field(f.type_, out.ref(), f.offset_, f.nullity_offset_, src, std::addressof(resource));
                    } else {
                        utils::copy_field(f.type_, out.ref(), f.offset_, src, std::addressof(resource));
                    }
                    break;
                }
                case process::impl::ops::default_value_kind::sequence: {
                    // increment sequence - loop might increment the sequence twice
                    sequence_value v{};
                    if (auto res = next_sequence_value(ctx, f.def_id_, v); res != status::ok) {
                        handle_encode_error(ctx, res);
                        return res;
                    }
                    out.ref().set_null(f.nullity_offset_, false);
                    out.ref().set_value<std::int64_t>(f.offset_, v);
                    break;
                }
            }
        } else {
            evaluator eval{t.elements()[f.index_], info, host_variables};
            process::impl::variable_table empty{};
            process::impl::expression::evaluator_context c{};
            auto res = eval(c, empty, &resource);
            if (res.error()) {
                auto rc = status::err_expression_evaluation_failure;
                set_error(
                    ctx,
                    error_code::value_evaluation_exception,
                    string_builder{} << "An error occurred in evaluating values. error:" << res.to<process::impl::expression::error>() << string_builder::to_string,
                    rc);
                return rc;
            }
            if (!utils::convert_any(res, f.type_)) {
                auto rc = status::err_expression_evaluation_failure;
                set_error(
                    ctx,
                    error_code::value_evaluation_exception,
                    string_builder{} << "An error occurred in evaluating values. type mismatch: expected " << f.type_ << ", value index is " << res.type_index() << string_builder::to_string,
                    rc);
                return rc;
            }
            if (f.nullable_) {
                utils::copy_nullable_field(f.type_, out.ref(), f.offset_, f.nullity_offset_, res, std::addressof(resource));
            } else {
                if (!res) {
                    auto rc = status::err_integrity_constraint_violation;
                    set_error(
                        ctx,
                        error_code::not_null_constraint_violation_exception,
                        string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                        rc);
                    return rc;
                }
                utils::copy_field(f.type_, out.ref(), f.offset_, res, std::addressof(resource));
            }
        }
    }
    return status::ok;
}
write::write(
    write_kind kind,
    yugawara::storage::index const& idx,
    takatori::statement::write const& wrt,
    memory::lifo_paged_memory_resource& resource,
    compiled_info info,
    executor::process::impl::variable_table const* host_variables
) :
    kind_(kind),
    idx_(std::addressof(idx)),
    wrt_(std::addressof(wrt)),
    resource_(std::addressof(resource)),
    info_(std::move(info)),
    host_variables_(host_variables),
    key_meta_(index::create_meta(*idx_, true)),
    value_meta_(index::create_meta(*idx_, false)),
    key_fields_(create_fields(*idx_, wrt_->columns(), key_meta_, value_meta_, true)),
    value_fields_(create_fields(*idx_, wrt_->columns(), key_meta_, value_meta_, false)),
    primary_(create_primary_target(idx_->simple_name(), key_meta_, value_meta_, key_fields_, value_fields_)),
    secondaries_(create_secondary_targets(*idx_, key_meta_, value_meta_))
{}

model::statement_kind write::kind() const noexcept {
    return model::statement_kind::write;
}

void abort_transaction(transaction_context& tx) {
    if (auto res = tx.abort(); res != status::ok) {
        throw_exception(std::logic_error{"abort failed unexpectedly"});
    }
}

bool write::operator()(request_context& context) {
    auto res = process(context);
    if(! res) {
        // Ensure tx aborts on any error. Though tx might be already aborted due to kvs errors,
        // aborting again will do no harm since sharksfin tx manages is_active flag and omits aborting again.
        auto& tx = context.transaction();
        abort_transaction(*tx);
    }
    return res;
}

write_primary_target create_primary_target(
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    std::vector<details::write_field> const& key_fields,
    std::vector<details::write_field> const& value_fields
) {
    std::vector<index::field_info> input_key_fields{};
    input_key_fields.reserve(key_fields.size());
    for(auto&& f : key_fields) {
        input_key_fields.emplace_back(
            f.type_,
            true,
            f.offset_,
            f.nullity_offset_,
            f.nullable_,
            f.spec_
        );
    }
    std::vector<index::field_info> input_value_fields{};
    input_value_fields.reserve(value_fields.size());
    for(auto&& f : value_fields) {
        input_value_fields.emplace_back(
            f.type_,
            true,
            f.offset_,
            f.nullity_offset_,
            f.nullable_,
            f.spec_
        );
    }
    return {
        storage_name,
        std::move(key_meta),
        std::move(value_meta),
        input_key_fields,
        input_key_fields,
        std::move(input_value_fields),
        std::vector<process::impl::ops::details::update_field>{}
    };
}

std::vector<write_secondary_target> create_secondary_targets(
    yugawara::storage::index const& idx,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta
) {
    std::vector<write_secondary_target> ret{};
    auto cnt = 0;
    idx.table().owner()->each_table_index(idx.table(),
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ++cnt;
        }
    );
    ret.reserve(cnt);
    idx.table().owner()->each_table_index(idx.table(),
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ret.emplace_back(
                *entry,
                key_meta,
                value_meta
            );
        }
    );
    return ret;
}

std::vector<write_secondary_context> create_secondary_contexts(
    std::vector<write_secondary_target> const& targets,
    kvs::database& db,
    request_context& context
) {
    std::vector<write_secondary_context> ret{};
    ret.reserve(targets.size());
    for (auto&& e: targets) {
        ret.emplace_back(
            db.get_or_create_storage(e.storage_name()),
            std::addressof(context));
    }
    return ret;
}

bool write::put_primary(
    write_context& wctx,
    bool& skip_error,
    std::string_view& encoded_primary_key
) {
    skip_error = false;
    kvs::put_option opt = (kind_ == write_kind::insert || kind_ == write_kind::insert_skip) ?
        kvs::put_option::create :
        kvs::put_option::create_or_update;
    if(auto res = primary_.encode_and_put(
        wctx.primary_context_,
        *wctx.request_context_->transaction(),
        opt,
        wctx.key_store_.ref(),
        wctx.value_store_.ref(),
        encoded_primary_key
    ); res != status::ok) {
        if (opt == kvs::put_option::create && res == status::already_exists) {
            if (kind_ == write_kind::insert) {
                // integrity violation should be handled in SQL layer and forces transaction abort
                // status::already_exists is an internal code, raise it as constraint violation
                set_error(
                    *wctx.request_context_,
                    error_code::unique_constraint_violation_exception,
                    string_builder{} << "Unique constraint violation occurred. Table:" << primary_.storage_name() << string_builder::to_string,
                    status::err_unique_constraint_violation);
                return false;
            }
            // write_kind::insert_skip
            // duplicated key is simply ignored
            // simply set stats 0 in order to mark INSERT IF NOT EXISTS statement executed
            wctx.request_context_->enable_stats()->counter(counter_kind::inserted).count(0);

            // skip error and move to next tuple
            skip_error = true;
            return false;
        }
        handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
        return false;
    }
    auto kind = opt == kvs::put_option::create ? counter_kind::inserted : counter_kind::merged;
    wctx.request_context_->enable_stats()->counter(kind).count(1);
    return true;
}

write_context::write_context(request_context& context,
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,  //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    std::vector<write_secondary_target> const& secondaries,
    kvs::database& db,
    memory::lifo_paged_memory_resource* resource) :
    request_context_(std::addressof(context)),
    primary_context_(
        db.get_or_create_storage(storage_name),
        key_meta,
        value_meta,
        std::addressof(context)),
    secondary_contexts_(create_secondary_contexts(secondaries, db, context)),
    key_store_(key_meta, resource),
    value_store_(value_meta, resource) {}

bool write::put_secondaries(
    write_context& wctx,
    std::string_view encoded_primary_key
) {
    for(std::size_t i=0, n=secondaries_.size(); i < n; ++i) {
        auto& e = secondaries_[i];
        if(auto res = e.encode_and_put(
            wctx.secondary_contexts_[i],
            *wctx.request_context_->transaction(),
            wctx.key_store_.ref(),
            wctx.value_store_.ref(),
            encoded_primary_key
        ); res != status::ok) {
            handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
            return false;
        }
    }
    return true;
}

bool write::update_secondaries_before_upsert(
    write_context& wctx
) {
    std::string_view encoded_primary_key{};
    auto res = primary_.find_record(
        wctx.primary_context_,
        *wctx.request_context_->transaction(),
        wctx.key_store_.ref(),
        resource_,
        encoded_primary_key
    );
    if(res != status::ok && res != status::not_found) {
        handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
        return false;
    }

    data::aligned_buffer buf_i{};
    data::aligned_buffer buf_e{};
    bool found_primary = res != status::not_found;
    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        auto& e = secondaries_[i];
        auto& c = wctx.secondary_contexts_[i];
        if (found_primary) {
            // try update existing secondary entry
            std::string_view encoded_i{};
            if (auto res = e.encode_secondary_key(c, buf_i, wctx.key_store_.ref(), wctx.value_store_.ref(), encoded_primary_key, encoded_i); res != status::ok) {
                handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                return false;
            }
            std::string_view encoded_e{};
            if (auto res = e.encode_secondary_key(c, buf_e, wctx.primary_context_.extracted_key(), wctx.primary_context_.extracted_value(), encoded_primary_key, encoded_e); res != status::ok) {
                handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                return false;
            }
            if (encoded_e.size() != encoded_i.size() || std::memcmp(encoded_i.data(), encoded_e.data(), encoded_e.size()) != 0) {
                // secondary entry needs to be updated - first remove it
                if (auto res = e.remove_by_encoded_key(
                        c,
                        *wctx.request_context_->transaction(),
                        encoded_e); res != status::ok) {
                    handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                    return false;
                }
            }
        }
        if(auto res = e.encode_and_put(
            c,
            *wctx.request_context_->transaction(),
            wctx.key_store_.ref(),
            wctx.value_store_.ref(),
            encoded_primary_key
        ); res != status::ok) {
            handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
            return false;
        }
    }
    return true;
}

bool write::process(request_context& context) {  //NOLINT(readability-function-cognitive-complexity)
    auto& tx = context.transaction();
    BOOST_ASSERT(tx);  //NOLINT
    auto* db = tx->database();


    write_context wctx(context,
        idx_->simple_name(),
        key_meta_,
        value_meta_,
        secondaries_,
        *db,
        resource_);

    for(auto&& tuple: wrt_->tuples()) {
        utils::checkpoint_holder cph(resource_);
        if(auto res = create_record_from_tuple(
            context,
            tuple,
            key_fields_,
            info_,
            *resource_,
            host_variables_,
            wctx.key_store_
        ); res != status::ok) {
            return false;
        }
        if(auto res = create_record_from_tuple(
            context,
            tuple,
            value_fields_,
            info_,
            *resource_,
            host_variables_,
            wctx.value_store_
        ); res != status::ok) {
            return false;
        }

        if(kind_ == write_kind::insert_overwrite && ! secondaries_.empty()) {
            if(! update_secondaries_before_upsert(wctx)) {
                return false;
            }
        }

        std::string_view encoded_primary_key{};
        bool skip_error = false;
        if(! put_primary(wctx, skip_error, encoded_primary_key)) {
            if(skip_error) {
                continue;
            }
            return false;
        }

        if(kind_ == write_kind::insert_overwrite) {
            // updating secondaries is done already
            continue;
        }

        if(! put_secondaries(wctx, encoded_primary_key)) {
            return false;
        };
    }
    return true;
}

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
        set_error(
            ctx,
            error_code::data_corruption_exception,
            "Data inconsistency detected.",
            st
        );
        return;
    }
    if(st == status::err_expression_evaluation_failure) {
        set_error(
            ctx,
            error_code::value_evaluation_exception,
            "An error occurred in evaluating values. Encoding failed.",
            st
        );
        return;
    }
    if(st == status::err_insufficient_field_storage) {
        set_error(
            ctx,
            error_code::value_too_long_exception,
            "Insufficient storage to store field data.",
            st
        );
        return;
    }
    if(st == status::err_invalid_runtime_value) {
        set_error(
            ctx,
            error_code::invalid_runtime_value_exception,
            "detected invalid runtime value",
            st
        );
        return;
    }
    set_error(
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
                            set_error(
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
                            handle_encode_error(ctx, res);
                            return res;
                        }
                        break;
                    }
                    case process::impl::ops::default_value_kind::sequence:
                        // increment sequence - loop might increment the sequence twice
                        sequence_value v{};
                        if(auto res = next_sequence_value(ctx, f.def_id_, v); res != status::ok) {
                            handle_encode_error(ctx, res);
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
                    set_error(
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
                    set_error(
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
                        set_error(
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
    data::any src{};
    bool is_immediate = false;
    switch(dv.kind()) {
        case column_value_kind::nothing:
            break;
        case column_value_kind::immediate: {
            knd = process::impl::ops::default_value_kind::immediate;
            src = utils::as_any(
                *dv.element<column_value_kind::immediate>(),
                type,
                nullptr
            );
            is_immediate = true;
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
    auto& e = ret.emplace_back(
        index,
        t,
        spec,
        nullable,
        knd,
        std::move(buf),
        def_id
    );
    if(is_immediate) {
        e.default_value_immediate_ = src;
    }
}

std::vector<details::write_field> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<write::column const> columns,
    maybe_shared_ptr<meta::record_meta> key_meta,  //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    bool key
) {
    using reference = takatori::descriptor::variable::reference_type;
    yugawara::binding::factory bindings{};
    std::vector<details::write_field> out{};
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
                auto pos = out.size();
                create_generated_field(out, npos, dv, type, nullable, spec);
                auto& f = out[pos];
                f.nullity_offset_ = key_meta->nullity_offset(pos);
                f.offset_ = key_meta->value_offset(pos);
                continue;
            }
            auto pos = out.size();
            auto& f = out.emplace_back(
                variable_indices[kc.reference()],
                t,
                spec,
                nullable
            );
            f.nullity_offset_ = key_meta->nullity_offset(pos);
            f.offset_ = key_meta->value_offset(pos);
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
                auto pos = out.size();
                create_generated_field(out, npos, dv, type, nullable, spec);
                auto& f = out[pos];
                f.nullity_offset_ = value_meta->nullity_offset(pos);
                f.offset_ = value_meta->value_offset(pos);
                continue;
            }
            auto pos = out.size();
            auto& f = out.emplace_back(
                variable_indices[b.reference()],
                t,
                spec,
                nullable
            );
            f.nullity_offset_ = value_meta->nullity_offset(pos);
            f.offset_ = value_meta->value_offset(pos);
        }
    }
    return out;
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
    (void) idx;
    (void) columns;
    (void) key;
    // if(auto res = create_fields(idx, columns, key, fields); res != status::ok) {
        // return res;
    // }
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

    bool has_secondaries{false};
    status ret_status{status::ok};
    table.owner()->each_table_index(table,
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (ret_status != status::ok) return;
            if (entry == primary) {
                return;
            }
            has_secondaries = true;
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
    if(has_secondaries && kind_ == write_kind::insert_overwrite) {
        set_error(
            ctx,
            error_code::unsupported_runtime_feature_exception,
            string_builder{} <<
                "INSERT OR REPLACE statement is not supported yet for tables with secondary indices" << string_builder::to_string,
            status::err_unsupported
        );
        return status::err_unsupported;
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
