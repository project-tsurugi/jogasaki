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
#include "drop_table.h"

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>

#include <jogasaki/constants.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/string_manipulation.h>

namespace jogasaki::executor::common {

using takatori::util::string_builder;

drop_table::drop_table(takatori::statement::drop_table& ct) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind drop_table::kind() const noexcept {
    return model::statement_kind::drop_table;
}

bool remove_generated_sequences(
    request_context& context,
    std::vector<std::string>& generated_sequences,
    std::string_view sequence_name
) {
    auto& provider = *context.storage_provider();
    generated_sequences.emplace_back(sequence_name);
    if(auto s = provider.find_sequence(sequence_name)) {
        if(s->definition_id().has_value()) {
            auto def_id = s->definition_id().value();
            try {
                if(! context.sequence_manager()->remove_sequence(def_id, context.transaction()->object().get())) {
                    VLOG_LP(log_error) << "sequence '" << sequence_name << "' not found";
                    context.status_code(status::err_not_found);
                    return false;
                }
            } catch(executor::sequence::exception const& e) {
                VLOG_LP(log_error) << "removing sequence '" << sequence_name << "' not found";
                context.status_code(e.get_status(), e.what());
                return false;
            }
        }
    }
    return true;
}

bool drop_auto_generated_sequences(
    request_context& context,
    yugawara::storage::table const& t,
    std::vector<std::string>& generated_sequences
) {
    for(auto&& col : t.columns()) {
        // normally, sequence referenced in default value should not be dropped. Only the exception is one auto-generated for primary key.
        if(utils::is_prefix(col.simple_name(), generated_pkey_column_prefix)) {
            if(! remove_generated_sequences(context, generated_sequences, col.simple_name())) {
                return false;
            }
            continue;
        }
        if(col.default_value().kind() == yugawara::storage::column_value_kind::sequence) {
            auto& dv = col.default_value().element<::yugawara::storage::column_value_kind::sequence>();
            // currently any sequence in the default value is auto-generated, but check the prefix just in case
            if(utils::is_prefix(dv->simple_name(), generated_sequence_name_prefix)) {
                if(! remove_generated_sequences(context, generated_sequences, dv->simple_name())) {
                    return false;
                }
            }
        }
    }
    return true;
}

bool drop_table::operator()(request_context& context) const {
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    auto& provider = *context.storage_provider();
    auto& c = yugawara::binding::extract<yugawara::storage::table>(ct_->target());
    auto t = provider.find_table(c.simple_name());
    if(t == nullptr) {
        set_error(
            context,
            error_code::target_not_found_exception,
            string_builder{} << "Table \"" << c.simple_name() << "\" not found." << string_builder::to_string,
            status::err_not_found
        );
        return false;
    }

    std::vector<std::string> generated_sequences{};
    // drop auto-generated sequences first because accessing system table causes cc error
    if(! drop_auto_generated_sequences(context, *t, generated_sequences)) {
        return false;
    }

    // drop secondary indices
    std::vector<std::string> indices{};
    provider.each_table_index(*t, [&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const& entry) {
        if(c.simple_name() == entry->simple_name()) {
            // skip primary
            return;
        }
        indices.emplace_back(id);
    });

    for(auto&& n : indices) {
        if(auto stg = context.database()->get_storage(n)) {
            if(auto res = stg->delete_storage(); res != status::ok && res != status::not_found) {
                handle_generic_error(context, status::err_unknown, error_code::sql_execution_exception);
                return false;
            }
        }
    }

    if(auto stg = context.database()->get_storage(c.simple_name())) {
        if(auto res = stg->delete_storage(); res != status::ok && res != status::not_found) {
            handle_generic_error(context, status::err_unknown, error_code::sql_execution_exception);
            return false;
        }
    }

    // kvs storages are deleted successfully
    // Going forward, try to clean up metadata as much as possible even if there is some inconsistency/missing parts.

    // drop auto-generated sequences
    for(auto&& n : generated_sequences) {
        // Normally, sequence referenced in default value should not be dropped.
        // The exception is ones auto-generated for primary key or generated identity columns.
        provider.remove_sequence(n);
    }

    for(auto&& n : indices) {
        if(! provider.remove_index(n)) {
            VLOG_LP(log_warning) << "secondary index '" << n << "' not found";
        }
    }

    // drop primary index
    if(! provider.remove_index(c.simple_name())) {
        VLOG_LP(log_warning) << "primary index for table '" << c.simple_name() << "' not found";
    }

    // drop table
    if(! provider.remove_relation(c.simple_name())) {
        VLOG_LP(log_warning) << "table '" << c.simple_name() << "' not found";
    }
    return true;
}
}
