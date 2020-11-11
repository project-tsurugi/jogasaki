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
#include <takatori/util/fail.h>

#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_impl.h>
#include <jogasaki/request_context.h>
#include <jogasaki/channel.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/common/graph.h>

namespace jogasaki::api {

using configurable_provider = ::yugawara::storage::configurable_provider;
using takatori::util::fail;

class database::impl {
public:
    impl() : impl(std::make_shared<configuration>()) {}
    explicit impl(std::shared_ptr<configuration> cfg) : cfg_(std::move(cfg)), scheduler_{cfg_} {
        add_default_table_defs(storage_provider_.get());
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
private:
    std::shared_ptr<configuration> cfg_{};
    scheduler::dag_controller scheduler_{};
    std::shared_ptr<configurable_provider> storage_provider_{std::make_shared<configurable_provider>()};
    std::shared_ptr<kvs::database> kvs_db_{};

    void add_default_table_defs(configurable_provider* provider) {
        namespace type = ::takatori::type;

        std::shared_ptr<::yugawara::storage::table> t0 = provider->add_table("T0", {
            "T0",
            {
                { "C0", type::int8() },
                { "C1", type::float8 () },
            },
        });
        std::shared_ptr<::yugawara::storage::index> i0 = provider->add_index("I0", {
            t0,
            "I0",
            {
                t0->columns()[0],
            },
            {},
            {
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            },
        });
    }
};

}
