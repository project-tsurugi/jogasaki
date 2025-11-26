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

#include <takatori/statement/details/table_authorization_entry.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/table.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/auth/fill_action_set.h>
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

static bool check_grant_revoke_preconditions(  //NOLINT(readability-function-cognitive-complexity)
    bool grant,
    request_context& context,
    std::vector<takatori::statement::details::table_privilege_element> const& elements,
    std::optional<std::string_view> const& current_user
) {
    auto& smgr = *global::storage_manager();
    auto& provider = *context.storage_provider();
    for(auto&& tpe : elements) {
        auto c = yugawara::binding::extract_shared<yugawara::storage::table>(tpe.table());
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

        for(auto&& tae: tpe.authorization_entries()) {
            if(tae.user_kind() == takatori::statement::authorization_user_kind::current_user) {
                // `current_user` is allowed only when the user name is available
                if (! current_user.has_value()) {
                    set_error(
                        context,
                        error_code::value_evaluation_exception,
                        "current_user value is not available",
                        status::err_expression_evaluation_failure
                    );
                    return false;
                }
            }
            if(tae.user_kind() == takatori::statement::authorization_user_kind::all_users) {
                // `*` is allowed only when the user name is available
                if (! current_user.has_value()) {
                    set_error(
                        context,
                        error_code::value_evaluation_exception,
                        "cannot revoke table privileges from all users when authentication mechanism is disabled",
                        status::err_expression_evaluation_failure
                    );
                    return false;
                }
                // `*` is allowed for REVOKE ALL PRIVILEGES only
                for(auto&& tpa : tae.privileges()) {
                    if(tpa.action_kind() != takatori::statement::details::table_privilege_action::action_kind_type::control) {
                        set_error(
                            context,
                            error_code::unsupported_runtime_feature_exception,
                            "to revoke table privileges from all users, the privilege must be ALL PRIVILEGES",
                            status::err_unsupported
                        );
                        return false;
                    }
                }
            }
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
        if (! context.req_info().request_source() || context.req_info().request_source()->session_info().user_type() ==
            tateyama::api::server::user_type::administrator) {
            // if request_source is null, this is in testcase
            continue;
        }
        auto sc = smgr.find_entry(tid);
        assert_with_exception(sc != nullptr, c->simple_name());  // must exist as we locked successfully above

        // TODO change to alter when supported
        if (current_user.has_value() &&
            sc->allows_user_actions(current_user.value(), auth::action_set{auth::action_kind::control})) {

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

        VLOG_LP(log_error) << "insufficient authorization user:\"" << (current_user.has_value() ? current_user.value() : "")
                           << "\" user_type:" << to_string_view(tateyama::api::server::user_type::standard);
        set_error(
            context,
            error_code::permission_error,
            "insufficient authorization for the requested operation",
            status::err_illegal_operation
        );
        return false;
    }
    return true;
}

static std::string get_grantee(
    takatori::statement::details::table_authorization_entry const& tae,
    std::optional<std::string_view> const& current_user
) {
    std::string ret{};
    switch(tae.user_kind()) {
        case takatori::statement::authorization_user_kind::specified:
            ret = tae.authorization_identifier() ;
            break;
        case takatori::statement::authorization_user_kind::current_user:
            assert_with_exception(current_user.has_value(), tae.authorization_identifier());  // already checked in check_grant_revoke_preconditions //NOLINT
            ret = current_user.value();
            break;
        case takatori::statement::authorization_user_kind::all_users:
            // no-op here. `*` is allowed only for revoke. There is no single grantee for this case and it is separately handled.
            break;
    }
    return ret;
}

static std::pair<auth::action_set, auth::authorized_users_action_set> calculate_public_and_authorized_actions(
    takatori::statement::details::table_privilege_element const& tpe,
    storage::impl::storage_control& sc,
    bool grant,
    std::optional<std::string_view> const& current_user
) {
    auth::action_set public_actions = sc.public_actions();
    auth::authorized_users_action_set authorized_actions = sc.authorized_actions();

    if (grant) {
        public_actions.add_actions(from(tpe.default_privileges()));
        for(auto&& tae : tpe.authorization_entries()) {
            auto grantee = get_grantee(tae, current_user);
            authorized_actions.add_user_actions(grantee, from(tae.privileges()));
        }
        return {public_actions, authorized_actions};
    }

    // revoke
    public_actions.remove_actions(from(tpe.default_privileges()));
    for(auto&& tae : tpe.authorization_entries()) {
        if(tae.user_kind() == takatori::statement::authorization_user_kind::all_users) {
            assert_with_exception(current_user.has_value(), tae.authorization_identifier());  // already checked in check_grant_revoke_preconditions //NOLINT
            public_actions.clear();
            for(auto it = authorized_actions.begin(), end = authorized_actions.end(); it != end; ) {
                if (it->first != current_user.value()) {
                    it = authorized_actions.erase(it);
                    continue;
                }
                ++it;
            }
            continue;
        }
        // for specified or current_user
        auto grantee = get_grantee(tae, current_user);
        authorized_actions.remove_user_actions(grantee, from(tae.privileges()));
    }
    return {public_actions, authorized_actions};
}

static bool serialize_and_save(
    request_context& context,
    std::string_view table_name,
    storage::impl::storage_control& sc,
    auth::action_set const& public_actions,
    auth::authorized_users_action_set const& authorized_actions
) {
    auto& provider = *context.storage_provider();
    auto primary_index = provider.find_index(table_name);
    assert_with_exception(primary_index != nullptr, table_name);
    std::string storage{};
    if(auto err = recovery::create_storage_option(
           *primary_index,
           storage,
           utils::metadata_serializer_option{
               false,
               std::addressof(authorized_actions),
               std::addressof(public_actions)
           }
       )) {
        // error should not happen normally
        set_error_info(context, err);
        return false;
    }

    proto::metadata::storage::IndexDefinition idef{};
    std::uint64_t v{};
    if(auto err = recovery::validate_extract(storage, idef, v)) {
        // should not happen normally
        // we just created the option above
        set_error_info(context, err);
        return false;
    }
    sc.authorized_actions().clear();
    from_authorization_list(idef.table_definition(), sc.authorized_actions());
    sc.public_actions().clear();
    auth::from_default_privilege(idef.table_definition(), sc.public_actions());

    sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    auto stg = context.database()->get_storage(table_name);
    if(! stg) {
        // should not happen normally
        set_error(
            context,
            error_code::target_not_found_exception,
            string_builder{} << "Storage \"" << table_name << "\" not found" << string_builder::to_string,
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
    return true;
}

static bool reflect_grant_revoke(
    bool grant,
    request_context& context,
    std::vector<takatori::statement::details::table_privilege_element> const& elements,
    std::optional<std::string_view> const& current_user
) {
    auto& smgr = *global::storage_manager();
    for(auto&& tpe : elements) {
        auto& c = *yugawara::binding::extract_shared<yugawara::storage::table>(tpe.table());

        auto se = smgr.find_by_name(c.simple_name());
        assert_with_exception(se.has_value(), c.simple_name());
        auto sc = smgr.find_entry(se.value());
        assert_with_exception(sc != nullptr, c.simple_name());

        // to avoid discrepancies between in-memory auth. metadata (in storage entry) and
        // durable one (in storage option), we first copy the in-memory metadata, serialize to create storage option,
        // and deserialize into the in-memory one again

        auto [public_actions, authorized_actions] =
            calculate_public_and_authorized_actions(tpe, *sc, grant, current_user);
        if(! serialize_and_save(context, c.simple_name(), *sc, public_actions, authorized_actions)) {
            return false;
        }
    }
    return true;
}

bool process_grant_revoke(  //NOLINT(readability-function-cognitive-complexity)
    bool grant,
    request_context& context,
    std::vector<takatori::statement::details::table_privilege_element> const& elements
) {
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    std::optional<std::string_view> current_user = context.req_info().request_source()
        ? context.req_info().request_source()->session_info().username()
        : std::nullopt;

    // pre-condition check
    // - all target tables exist
    // - syntax elements are correct (e.g. current_user is allowed only when user name is available)
    // - acquire locks successfully
    // - current user has sufficient permission
    if(! check_grant_revoke_preconditions(grant, context, elements, current_user)) {
        return false;
    }

    // pre-condition checked, let's make changes
    // after this point, we expect no error occurs normally; if any error occurs, that is unexpected internal error
    return reflect_grant_revoke(grant, context, elements, current_user);
}

}
