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
#include "function_registry.h"

#include <sstream>
#include <utility>

#include <glog/logging.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/executor/function/builtin_scalar_functions.h>
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/function/udf_functions.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/udf/log/logging_prefix.h>
#include <jogasaki/udf/udf_loader.h>

namespace jogasaki::executor::function {

void function_registry::initialize(configuration const& cfg) {
    clear();
    add_builtin_scalar_functions(
        *regular_provider_,
        scalar_functions_
    );
    loader_ = std::make_unique<plugin::udf::udf_loader>();
    auto results = loader_->load(std::string(cfg.plugin_directory()));
    for (auto const& result : results) {
        auto const status = result.status();
        auto const outcome = plugin::udf::classify(status);

        std::ostringstream oss;

        oss << jogasaki::udf::log::prefix << plugin::udf::to_string_view(outcome)
            << " status=" << plugin::udf::to_string_view(status) << " file=" << result.file()
            << " detail=" << result.detail();

        auto const message = oss.str();

        switch (outcome) {
            case plugin::udf::load_outcome::ok: LOG_LP(INFO) << message; break;
            case plugin::udf::load_outcome::skipped: LOG_LP(WARNING) << message; break;
            case plugin::udf::load_outcome::fail: LOG_LP(ERROR) << message; break;
            default: LOG_LP(ERROR) << message; break;
        }
    }
    for (auto& plugin : loader_->get_plugins()) {
        plugins_.emplace_back(std::move(std::get<0>(plugin)), std::move(std::get<1>(plugin)),
            std::move(std::get<2>(plugin)));
    }
    add_udf_functions(*regular_provider_, scalar_functions_,
        table_functions_, plugins_);
    incremental::add_builtin_aggregate_functions(
        *aggregate_provider_,
        incremental_functions_
    );
    add_builtin_aggregate_functions(
        *aggregate_provider_,
        aggregate_functions_
    );
}

void function_registry::clear() {
    auto regular = std::make_shared<yugawara::function::configurable_provider>();
    auto aggregate = std::make_shared<yugawara::aggregate::configurable_provider>();
    scalar_functions_.clear();
    table_functions_.clear();
    aggregate_functions_.clear();
    incremental_functions_.clear();
    regular_provider_ = std::move(regular);
    aggregate_provider_ = std::move(aggregate);
    plugins_.clear();
    loader_.reset();
}

void function_registry::regular_provider(std::shared_ptr<yugawara::function::configurable_provider> value) {
    regular_provider_ = std::move(value);
}
}
