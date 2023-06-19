/*
 * Copyright 2018-2023 tsurugi project.
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

#include <tateyama/framework/service.h>
#include <tateyama/framework/repository.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/component_ids.h>

#include <jogasaki/api/kvsservice/store.h>

namespace jogasaki::api::kvsservice {

using tateyama::api::server::request;
using tateyama::api::server::response;
namespace framework = tateyama::framework;

/**
 * @brief kvs resource bridge for tateyama framework
 * @details This object bridges kvs engine as a resource component in tateyama framework.
 * This object should be responsible only for life-cycle management.
 */
class resource : public framework::resource {
public:
    static constexpr id_type tag = framework::resource_id_remote_kvs;

    /**
     * @brief human readable label of this component
     */
    static constexpr std::string_view component_label = "remote_kvs_resource";

    /**
     * @brief create new object
     */
    resource();

    resource(resource const& other) = delete;
    resource& operator=(resource const& other) = delete;
    resource(resource&& other) noexcept = delete;
    resource& operator=(resource&& other) noexcept = delete;

    /**
     * @brief accessor to the resource id
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
    bool shutdown(framework::environment& env) override;

    /**
     * @brief destructor the object
     */
    ~resource() override;

    /**
     * @brief accessor to the database
     */
    [[nodiscard]] jogasaki::api::kvsservice::store* store() const noexcept;

    /**
     * @see tateyama::framework::component::label()
     */
    [[nodiscard]] std::string_view label() const noexcept override;

private:
    std::unique_ptr<jogasaki::api::kvsservice::store> store_;  // to use incomplete object, do not add {} after var. name.
};

}
