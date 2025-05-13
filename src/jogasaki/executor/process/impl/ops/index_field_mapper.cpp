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
#include "index_field_mapper.h"

#include <ostream>
#include <utility>
#include <glog/logging.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/abort_transaction.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/modify_status.h>

namespace jogasaki::executor::process::impl::ops {

details::secondary_index_field_info::secondary_index_field_info(
    meta::field_type type,
    bool source_nullable,
    kvs::coding_spec spec
) :
    type_(std::move(type)),
    source_nullable_(source_nullable),
    spec_(spec)
{}

index_field_mapper::index_field_mapper(
    bool use_secondary,
    std::vector<index::field_info> primary_key_fields,
    std::vector<index::field_info> primary_value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields
) :
    use_secondary_(use_secondary),
    primary_key_fields_(std::move(primary_key_fields)),
    primary_value_fields_(std::move(primary_value_fields)),
    secondary_key_fields_(std::move(secondary_key_fields))
{}

index_field_mapper::index_field_mapper(
    std::vector<index::field_info> primary_key_fields,
    std::vector<index::field_info> primary_value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields
) :
    index_field_mapper(
        true,
        std::move(primary_key_fields),
        std::move(primary_value_fields),
        std::move(secondary_key_fields)
    )
{}

index_field_mapper::index_field_mapper(
    std::vector<index::field_info> primary_key_fields,
    std::vector<index::field_info> primary_value_fields
) :
    index_field_mapper(
        false,
        std::move(primary_key_fields),
        std::move(primary_value_fields),
        {}
    )
{}
[[nodiscard]] bool index_field_mapper::use_secondary() const noexcept { return use_secondary_; }

status index_field_mapper::process_primary(
    std::string_view key,
    std::string_view value,
    accessor::record_ref target,
    kvs::storage& stg,
    transaction_context& tx,
    index_field_mapper::memory_resource* resource,
    request_context& req_context
) noexcept {
    (void)stg;
    (void)tx;
    (void)req_context;
    return populate_field_variables(key, value, target, resource);
}
status index_field_mapper::process_secondary(
    std::string_view key,
    std::string_view value,
    accessor::record_ref target,
    kvs::storage& stg,
    transaction_context& tx,
    index_field_mapper::memory_resource* resource,
    request_context& req_context
) noexcept {
    std::string_view k{key};
    std::string_view v{value};
    k = extract_primary_key(key);
    if (auto res = find_primary_index(k, stg, tx, v, req_context); res != status::ok) {
        return res;
    }
    return populate_field_variables(k, v, target, resource);
}
status index_field_mapper::process(
    std::string_view key,
    std::string_view value,
    accessor::record_ref target,
    kvs::storage& stg,
    transaction_context& tx,
    index_field_mapper::memory_resource* resource,
    request_context& req_context
) {
    std::string_view k{key};
    std::string_view v{value};
    if (use_secondary_) {
        k = extract_primary_key(key);
        if (auto res = find_primary_index(k, stg, tx, v, req_context); res != status::ok) {
            return res;
        }
    }
    return populate_field_variables(k, v, target, resource);
}

status index_field_mapper::consume_secondary_key_fields(
    std::vector<details::secondary_index_field_info> const& fields,
    kvs::readable_stream& stream
) {
    for(auto&& f : fields) {
        kvs::coding_context ctx{};
        if (f.source_nullable_) {
            if(auto res = kvs::consume_stream_nullable(stream, f.type_, f.spec_, ctx); res != status::ok) {
                return res;
            }
            continue;
        }
        if(auto res = kvs::consume_stream(stream, f.type_, f.spec_, ctx); res != status::ok) {
            return res;
        }
    }
    return status::ok;
}

std::string_view index_field_mapper::extract_primary_key(std::string_view key) {
    kvs::readable_stream keys{key.data(), key.size()};
    // consume key fields, then the rest is primary key
    consume_secondary_key_fields(secondary_key_fields_, keys);
    return keys.rest();
}

status index_field_mapper::find_primary_index(
    std::string_view key,
    kvs::storage& stg,
    transaction_context& tx,
    std::string_view& value_out,
    request_context& req_context
) {
    std::string_view v{};
    if(auto res = stg.content_get(tx, key, v); res != status::ok) {
        std::stringstream ss{};
        // we cannot use utils::modify_concurrent_operation_status here because missing entry is an inconsistency
        // between primary/secondary and should be treated as an error.
        if(res == status::concurrent_operation) {
            // concurrent operation blocks finding primary entry - retry might change the situation
            ss << "finding primary entry from secondary index entry failed due to concurrent operation";
            set_error(req_context, error_code::blocked_by_concurrent_operation_exception , ss.str(), res);
            utils::abort_transaction(tx);
        } else if (res == status::not_found) {
            // primary/secondary indices are not consistent
            ss << "missing primary index entry corresponding to the secondary index entry";
            res = status::err_inconsistent_index;
            set_error(req_context, error_code::secondary_index_corruption_exception, ss.str(), res);
            utils::abort_transaction(tx);
        } else {
            handle_kvs_errors(req_context, res);
            handle_generic_error(req_context, res, error_code::sql_execution_exception);
        }
        if(! ss.str().empty()) {
            VLOG_LP(log_error) << ss.str();
        }
        return res;
    }
    value_out = v;
    return status::ok;
}

status index_field_mapper::populate_field_variables(
    std::string_view key,
    std::string_view value,
    accessor::record_ref target,
    index_field_mapper::memory_resource* resource
) {
    kvs::readable_stream keys{key.data(), key.size()};
    kvs::readable_stream values{value.data(), value.size()};
    if(auto res = index::decode_fields(primary_key_fields_, keys, target, resource); res != status::ok) {
        return res;
    }
    if(auto res = index::decode_fields(primary_value_fields_, values, target, resource); res != status::ok) {
        return res;
    }
    return status::ok;
}

}


