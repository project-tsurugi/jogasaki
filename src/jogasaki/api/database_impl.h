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
#include <jogasaki/api/database.h>

#include <string_view>
#include <glog/logging.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/util/fail.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/variable/configurable_provider.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/aggregate/configurable_provider.h>

#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_impl.h>
#include <jogasaki/request_context.h>
#include <jogasaki/channel.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/function/functions.h>

namespace jogasaki::api {

using takatori::util::fail;
namespace storage = yugawara::storage;
namespace variable = yugawara::variable;
namespace aggregate = yugawara::aggregate;
namespace function = yugawara::function;

class database::impl {
public:
    impl() : impl(std::make_shared<configuration>()) {}
    explicit impl(std::shared_ptr<configuration> cfg) : cfg_(std::move(cfg)), scheduler_{cfg_} {
        executor::add_builtin_tables(*tables_);
        executor::function::add_builtin_aggregate_functions(*aggregate_functions_);
    }
    std::unique_ptr<result_set> execute(std::string_view sql);
    bool start();
    bool stop();

    [[nodiscard]] static database::impl* get_impl(database& arg) noexcept {
        return arg.impl_.get();
    }

    [[nodiscard]] std::shared_ptr<kvs::database> const& kvs_db() const noexcept {
        return kvs_db_;
    }

    [[nodiscard]] std::shared_ptr<yugawara::storage::configurable_provider> const& tables() const noexcept {
        return tables_;
    }

    [[nodiscard]] std::shared_ptr<yugawara::aggregate::configurable_provider> const& aggregate_functions() const noexcept {
        return aggregate_functions_;
    }
private:
    std::shared_ptr<configuration> cfg_{};
    scheduler::dag_controller scheduler_{};
    std::shared_ptr<yugawara::storage::configurable_provider> tables_{std::make_shared<yugawara::storage::configurable_provider>()};
    std::shared_ptr<yugawara::aggregate::configurable_provider> aggregate_functions_{std::make_shared<yugawara::aggregate::configurable_provider>()};
    std::shared_ptr<kvs::database> kvs_db_{};
};

}
