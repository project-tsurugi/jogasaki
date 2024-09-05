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
#include "insert_new_record.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <boost/assert.hpp>

#include <takatori/util/exception.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/conv/create_default_value.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/abort_transaction.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/handle_encode_errors.h>
#include <jogasaki/utils/handle_generic_error.h>

namespace jogasaki::executor::wrt {

using jogasaki::executor::process::impl::ops::write_kind;

using takatori::util::string_builder;

insert_new_record::insert_new_record(
    write_kind kind,
    primary_target primary,
    std::vector<secondary_target> secondaries
) :
    kind_(kind),
    primary_(std::move(primary)),
    secondaries_(std::move(secondaries))
{}

std::vector<secondary_context> create_secondary_contexts(
    std::vector<secondary_target> const& targets,
    kvs::database& db,
    request_context& context
) {
    std::vector<secondary_context> ret{};
    ret.reserve(targets.size());
    for (auto&& e: targets) {
        ret.emplace_back(
            db.get_or_create_storage(e.storage_name()),
            std::addressof(context));
    }
    return ret;
}

bool insert_new_record::try_insert_primary(
    write_context& wctx,
    bool& primary_already_exists,
    std::string_view& encoded_primary_key
) {
    primary_already_exists = false;
    if(auto res = primary_.encode_put(
        wctx.primary_context_,
        *wctx.request_context_->transaction(),
        kvs::put_option::create,
        wctx.key_store_.ref(),
        wctx.value_store_.ref(),
        encoded_primary_key
    ); res != status::ok) {
        if (res == status::already_exists) {
            primary_already_exists = true;
            return true;
        }
        handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
        return false;
    }
    wctx.request_context_->enable_stats()->counter(counter_kind::merged).count(1);
    return true;
}

bool insert_new_record::put_primary(
    write_context& wctx,
    bool& skip_error,
    std::string_view& encoded_primary_key
) {
    skip_error = false;
    kvs::put_option opt = (kind_ == write_kind::insert || kind_ == write_kind::insert_skip) ?
        kvs::put_option::create :
        kvs::put_option::create_or_update;
    if(auto res = primary_.encode_put(
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
                    string_builder{} << "Unique constraint violation occurred. Table:" << primary_.storage_name()
                                     << string_builder::to_string,
                    status::err_unique_constraint_violation
                );
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

write_context::write_context(
    request_context& context,
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,    //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    std::vector<secondary_target> const& secondaries,
    kvs::database& db,
    memory::lifo_paged_memory_resource* resource
) :
    request_context_(std::addressof(context)),
    primary_context_(db.get_or_create_storage(storage_name), key_meta, value_meta, std::addressof(context)),
    secondary_contexts_(create_secondary_contexts(secondaries, db, context)),
    key_store_(key_meta, resource),
    value_store_(value_meta, resource),
    resource_(resource)
{}

bool insert_new_record::put_secondaries(
    write_context& wctx,
    std::string_view encoded_primary_key
) {
    for(std::size_t i=0, n=secondaries_.size(); i < n; ++i) {
        auto& e = secondaries_[i];
        if(auto res = e.encode_put(
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

bool insert_new_record::update_secondaries_before_upsert(
    write_context& wctx,
    std::string_view encoded_primary_key,
    bool primary_already_exists
) {
    status res{};
    if(encoded_primary_key.empty()) {
        res = primary_.encode_find(
            wctx.primary_context_,
            *wctx.request_context_->transaction(),
            wctx.key_store_.ref(),
            wctx.resource_,
            wctx.primary_context_.extracted_key(),
            wctx.primary_context_.extracted_value(),
            encoded_primary_key
        );
    } else {
        res = primary_.find_by_encoded_key(
            wctx.primary_context_,
            *wctx.request_context_->transaction(),
            encoded_primary_key,
            wctx.resource_,
            wctx.primary_context_.extracted_key(),
            wctx.primary_context_.extracted_value()
        );
    }
    if(res != status::ok && res != status::not_found) {
        handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
        return false;
    }

    data::aligned_buffer buf_i{};
    data::aligned_buffer buf_e{};
    //TODO remove found_primary which is always true if dev_try_insert_on_upserting_secondary=true
    bool found_primary = res != status::not_found;
    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        auto& e = secondaries_[i];
        auto& c = wctx.secondary_contexts_[i];
        if (found_primary && primary_already_exists) {
            // try update existing secondary entry
            std::string_view encoded_i{};
            if(auto res = e.create_secondary_key(
                   c,
                   buf_i,
                   wctx.key_store_.ref(),
                   wctx.value_store_.ref(),
                   encoded_primary_key,
                   encoded_i
               );
               res != status::ok) {
                handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                return false;
            }
            std::string_view encoded_e{};
            if(auto res = e.create_secondary_key(
                   c,
                   buf_e,
                   wctx.primary_context_.extracted_key(),
                   wctx.primary_context_.extracted_value(),
                   encoded_primary_key,
                   encoded_e
               );
               res != status::ok) {
                handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                return false;
            }
            if(encoded_e.size() != encoded_i.size() ||
               std::memcmp(encoded_i.data(), encoded_e.data(), encoded_e.size()) != 0) {
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
        if(auto res = e.encode_put(
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

bool insert_new_record::process_record(
    request_context& context,
    write_context& wctx
) {
    (void) context;
    if(kind_ == write_kind::insert_overwrite && ! secondaries_.empty()) {
        bool primary_already_exists = true; // default true
        std::string_view encoded_primary_key{};
        if(wctx.request_context_->configuration()->try_insert_on_upserting_secondary()) {
            if(! try_insert_primary(wctx, primary_already_exists, encoded_primary_key)) {
                return false;
            }
        }
        if(! update_secondaries_before_upsert(wctx, encoded_primary_key, primary_already_exists)) {
            return false;
        }
        if(! primary_already_exists) {
            // there was no entry conflicting with insert, so there is nothing to update
            return true;
        }
    }
    // TODO consider to re-use `encoded_primary_key` above to optimize cost,
    // though value part encoding is still required
    std::string_view encoded_primary_key{};
    bool skip_error = false;
    if(! put_primary(wctx, skip_error, encoded_primary_key)) {
        return skip_error;
    }

    if(kind_ == write_kind::insert_overwrite) {
        // updating secondaries is done already
        return true;
    }

    if(! put_secondaries(wctx, encoded_primary_key)) {
        return false;
    };
    return true;
}

}  // namespace jogasaki::executor::common
