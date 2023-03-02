/*
 * Copyright 2018-2022 tsurugi project.
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
#pragma once

#include <functional>
#include <memory>
#include <type_traits>

#include <tateyama/framework/service.h>
#include <tateyama/framework/repository.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/component_ids.h>

#include <jogasaki/api/database.h>

namespace jogasaki::api::impl {
class service;
}

namespace jogasaki::api::service {

using tateyama::api::server::request;
using tateyama::api::server::response;
namespace framework = tateyama::framework;

/**
 * @brief sql service bridge for tateyama framework
 * @details This object bridges sql engine as a service component in tateyama framework.
 * This object should be responsible only for life-cycle management.
 */
class bridge : public framework::service {
public:
    static constexpr id_type tag = framework::service_id_sql;

    /**
     * @brief create new object
     */
    bridge();

    bridge(bridge const& other) = delete;
    bridge& operator=(bridge const& other) = delete;
    bridge(bridge&& other) noexcept = delete;
    bridge& operator=(bridge&& other) noexcept = delete;

    /**
     * @brief accessor to the service id
     */
    [[nodiscard]] id_type id() const noexcept override;

    /**
     * @brief setup the component (the state will be `ready`)
     */
    bool setup(framework::environment& env) override;

    /**
     * @brief start the component (the state will be `activated`)
     */
    bool start(framework::environment& env) override;

    /**
     * @brief shutdown the component (the state will be `deactivated`)
     */
    bool shutdown(framework::environment&) override;

    /**
     * @brief interface to exchange request and response
     */
    bool operator()(
        std::shared_ptr<request> req,
        std::shared_ptr<response> res) override;

    /**
     * @brief destructor the object
     */
    ~bridge() override;

    /**
     * @brief accessor to the database
     */
    [[nodiscard]] jogasaki::api::database* database() const noexcept;

private:
    std::unique_ptr<jogasaki::api::impl::service> core_;  // to use incomplete object, do not add {} after var. name.
    bool deactivated_{false};
    bool quiescent_or_maintenance_{false};
};

}

