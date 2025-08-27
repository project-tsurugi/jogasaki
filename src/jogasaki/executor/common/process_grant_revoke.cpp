/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "process_grant_revoke.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <boost/assert.hpp>

#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/table.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include "acquire_table_lock.h"

namespace jogasaki::executor::common {

using takatori::util::string_builder;

static auth::action_set from(std::vector<takatori::statement::details::table_privilege_action> const& actions) {
    using kind = takatori::statement::details::table_privilege_action::action_kind_type;
    auth::action_set ret{};
    for (auto&& action : actions) {
        switch (action.action_kind()) {
            case kind::control: ret.add_action(auth::action_kind::control); break;
            case kind::select: ret.add_action(auth::action_kind::select); break;
            case kind::insert: ret.add_action(auth::action_kind::insert); break;
            case kind::update: ret.add_action(auth::action_kind::update); break;
            case kind::delete_: ret.add_action(auth::action_kind::delete_); break;
        }
    }
    return ret;
}

static std::string_view to_string_view(tateyama::api::server::user_type type) {
    switch (type) {
        case tateyama::api::server::user_type::administrator: return "administrator";
        case tateyama::api::server::user_type::standard: return "standard";
    }
    std::abort();
}

bool process_grant_revoke(  //NOLINT(readability-function-cognitive-complexity)
    bool grant,
    request_context& context,
    std::vector<takatori::statement::details::table_privilege_element> const& elements
) {
    BOOST_ASSERT(context.storage_provider());  //NOLINT

    // pre-condition check: verify that all target tables exist and acquire locks successfully

    auto& smgr = *global::storage_manager();
    auto& provider = *context.storage_provider();
    for(auto&& e : elements) {
        auto c = yugawara::binding::extract_shared<yugawara::storage::table>(e.table());
        if (! c) {
            set_error(
                context,
                error_code::target_not_found_exception,
                "target table not found",
                status::err_not_found
            );
            return false;
        }
        auto table = provider.find_table(c->simple_name());
        if (! table) {
            set_error(
                context,
                error_code::target_not_found_exception,
                string_builder{} << "table \"" << c->simple_name() << "\" not found" << string_builder::to_string,
                status::err_not_found
            );
            return false;
        }

        // on any error during this for loop, the tx will be aborted in caller and the acquired lock will be released
        storage::storage_entry tid{};
        if(! acquire_table_lock(context, c->simple_name(), tid)) {
            return false;
        }

        // check permission
        // only the following users can grant/revoke privileges
        // - admin users
        // - user with control privilege
        // - user with alter privilege (not supported yet)
        if (context.req_info().request_source()) {
            auto name = context.req_info().request_source()->session_info().username();
            if(auto type = context.req_info().request_source()->session_info().user_type();
               type != tateyama::api::server::user_type::administrator) {
                auto sc = smgr.find_entry(tid);
                assert_with_exception(sc != nullptr, c->simple_name());  // must exist as we locked successfully above

                // TODO change to alter when supported
                if (name.has_value() && sc->allows_user_actions(name.value(), auth::action_set{auth::action_kind::control})) {
                    if (grant) {
                        // if grant, the privileges must be subset of user's own privileges
                        // TODO uncomment below to enable check when alter is supported
                        // if(! sc->allows(name.value(), from(e.default_privileges()))) {
                        //     return false;
                        // }
                        // for(auto&& tae : e.authorization_entries()) {
                        //     if(! sc->allows(name.value(), from(tae.privileges()))) {
                        //         return false;
                        //     }
                        // }
                    }
                    continue;
                }

                VLOG_LP(log_error) << "insufficient authorization user:\"" << (name.has_value() ? name.value() : "")
                                   << "\" user_type:" << to_string_view(type);
                set_error(
                    context,
                    error_code::permission_error,
                    "insufficient authorization for the requested operation",
                    status::err_illegal_operation
                );
                return false;
            }
        }
    }

    // pre-condition checked, let's make changes
    // after this point, we expect no error occurs normally; if any error occurs, that is unexpected internal error

    for(auto&& tpe : elements) {
        auto& c = *yugawara::binding::extract_shared<yugawara::storage::table>(tpe.table());

        auto se = smgr.find_by_name(c.simple_name());
        assert_with_exception(se.has_value(), c.simple_name());
        auto sc = smgr.find_entry(se.value());
        assert_with_exception(sc != nullptr, c.simple_name());

        if (grant) {
            sc->public_actions().add_actions(from(tpe.default_privileges()));
            for(auto&& tae : tpe.authorization_entries()) {
                sc->authorized_actions().add_user_actions(tae.authorization_identifier(), from(tae.privileges()));
            }
        } else {
            sc->public_actions().remove_actions(from(tpe.default_privileges()));
            for(auto&& tae : tpe.authorization_entries()) {
                sc->authorized_actions().remove_user_actions(tae.authorization_identifier(), from(tae.privileges()));
            }
        }

        auto primary_index = provider.find_index(c.simple_name());
        assert_with_exception(primary_index != nullptr, c.simple_name());
        std::string storage{};
        if(auto err = recovery::create_storage_option(
               *primary_index,
               storage,
               utils::metadata_serializer_option{
                   false,
                   std::addressof(sc->authorized_actions()),
                   std::addressof(sc->public_actions())
               }
           )) {
            // error should not happen normally
            set_error_info(context, err);
            return false;
        }

        // create_table calls recovery::deserialize_storage_option_into_provider here, but grant does not affect storage provider, so skip here

        sharksfin::StorageOptions options{};
        options.payload(std::move(storage));
        auto stg = context.database()->get_or_create_storage(c.simple_name());
        if(! stg) {
            // should not happen normally
            set_error(
                context,
                error_code::target_not_found_exception,
                string_builder{} << "Storage \"" << c.simple_name() << "\" not found" << string_builder::to_string,
                status::err_not_found
            );
            return false;
        }
        if(auto res = stg->set_options(options);res != status::ok) {
            // should not happen normally
            // though this calls sharksfin, updating storage metadata should almost always succeed
            set_error(
                context,
                error_code::sql_execution_exception,
                string_builder{} << "failed to modify storage metadata. status:" << res << string_builder::to_string,
                status::err_unknown
            );
            return false;
        }
    }
    return true;
}

}
