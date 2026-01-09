/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "lob_processing.h"

#include <memory>
#include <string>
#include <type_traits>

#include <takatori/util/string_builder.h>

#include <jogasaki/data/any.h>
#include <jogasaki/datastore/assign_lob_id.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/lob/lob_reference.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/utils/assign_reference_tag.h>

namespace jogasaki::executor::expr {

using takatori::util::string_builder;

namespace {

constexpr std::uint64_t BLOB_CLOB_PADDING = 0xFFFFFFFFFFFFFFFFULL;

template <typename T>
std::enable_if_t<std::is_same_v<T, lob::clob_reference> || std::is_same_v<T, lob::blob_reference>, data::any>
validate_reference_tag(T in, data_relay_grpc::blob_relay::blob_session& session, evaluator_context& ctx) {
    auto create_error = [&]() {
        ctx.set_error_info(create_error_info(
            error_code::permission_error,
            string_builder{} << "invalid reference tag in the large object function return value object_id:"
                             << in.object_id() << string_builder::to_string,
            status::err_illegal_operation
            )
        );
        return data::any{std::in_place_type<class error>, error_kind::error_info_provided};
    };
    auto tag = static_cast<lob::lob_reference&>(in).reference_tag();
    if(! tag) {
        // normally this should not happen
        return create_error();
    }
    lob::lob_reference_tag_type computed{};
    if (in.provider() == lob::lob_data_provider::datastore) {
        auto t = utils::assign_reference_tag(
            ctx.transaction()->surrogate_id(),
            in.object_id()
        );
        if(! t) {
            // normally this should not happen
            return create_error();
        }
        computed = t.value();
    } else if(in.provider() == lob::lob_data_provider::relay_service_session) {
        computed = session.compute_tag(in.object_id());
    }
    if (computed != tag.value()) {
        VLOG_LP(log_debug) << "validating reference tag failed computed_tag:" << computed << " blob_ref:" << in;
        return create_error();
    }
    VLOG_LP(log_debug) << "validating reference tag successful blob_ref:" << in;
    return {};
}

template <typename T>
std::enable_if_t<std::is_same_v<T, lob::clob_reference> || std::is_same_v<T, lob::blob_reference>, data::any>
post_process_lob(data::any in, evaluator_context& ctx) {
    // check if the returned blob/clob references belong to the current blob session
    // and if it's on the blob session, register the data to limestone
    // before the session is disposed (at the end of task).
    assert_with_exception(ctx.blob_session() != nullptr, "fail");
    auto s = ctx.blob_session()->get_or_create();
    if (! s) {
        // should not happen normally
        ctx.add_error({error_kind::unknown, "missing blob session"});
        return data::any{std::in_place_type<class error>, error_kind::unknown};
    }
    auto var = in.to<T>();
    if (auto a = validate_reference_tag(var, *s, ctx); a.error()) {
        return a;
    }
    if (var.provider() != lob::lob_data_provider::relay_service_session) {
        // stored on datastore, return as it is
        return in;
    }
    auto obj = s->find(var.object_id());
    if (! obj) {
        // should not happen normally
        ctx.add_error({error_kind::unknown, "missing entry in the blob session"});
        return data::any{std::in_place_type<class error>, error_kind::unknown};
    }
    // create `provided` lob reference and register to limestone
    lob::lob_locator loc{obj->string(), true};
    lob::lob_reference ref{loc};
    lob::lob_id_type id{};
    std::shared_ptr<jogasaki::error::error_info> error{};
    if (auto st = datastore::assign_lob_id(ref, ctx.transaction(), id, error); st != status::ok) {
        ctx.set_error_info(std::move(error));
        return data::any{std::in_place_type<class error>, error_kind::error_info_provided};
    }
    return {std::in_place_type<T>, T{id, lob::lob_data_provider::datastore}};
}

template <typename T>
std::enable_if_t<std::is_same_v<T, lob::clob_reference> || std::is_same_v<T, lob::blob_reference>, data::any>
pre_process_lob(T var, evaluator_context& ctx) {
    // assign lob reference tag before passing lob reference to functions
    if (global::config_pool()->udf_pass_mock_tag()) {
        var.reference_tag(BLOB_CLOB_PADDING);
        return {std::in_place_type<T>, var};
    }
    auto tag = utils::assign_reference_tag(
        ctx.transaction()->surrogate_id(),
        var.object_id()
    );
    if (! tag.has_value()) {
        ctx.add_error(
            {error_kind::unknown,
             string_builder{} << "unexpected error occurred during generating reference tag tx_id:"
                              << ctx.transaction()->surrogate_id() << " object_id:" << var.object_id()
                              << string_builder::to_string}
        );
        return data::any{std::in_place_type<class error>, error_kind::unknown};
    }
    var.reference_tag(tag);
    return {std::in_place_type<T>, var};
}

}  // namespace

data::any pre_process_if_lob(data::any in, evaluator_context& ctx) {
    if(in.type_index() == data::any::index<runtime_t<meta::field_type_kind::blob>>) {
        auto var = in.to<runtime_t<meta::field_type_kind::blob>>();
        return pre_process_lob<runtime_t<meta::field_type_kind::blob>>(var, ctx);
    }
    if(in.type_index() == data::any::index<runtime_t<meta::field_type_kind::clob>>) {
        auto var = in.to<runtime_t<meta::field_type_kind::clob>>();
        return pre_process_lob<runtime_t<meta::field_type_kind::clob>>(var, ctx);
    }
    return in;
}

data::any post_process_if_lob(data::any in, evaluator_context& ctx) {
    if(in.type_index() == data::any::index<runtime_t<meta::field_type_kind::blob>>) {
        return post_process_lob<runtime_t<meta::field_type_kind::blob>>(in, ctx);
    }
    if(in.type_index() == data::any::index<runtime_t<meta::field_type_kind::clob>>) {
        return post_process_lob<runtime_t<meta::field_type_kind::clob>>(in, ctx);
    }
    return in;
}

}  // namespace jogasaki::executor::expr
