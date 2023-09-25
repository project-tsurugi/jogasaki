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
#include <jogasaki/api/resource/bridge.h>

#include <functional>
#include <memory>
#include <type_traits>

#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/service.h>
#include <tateyama/framework/repository.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/component_ids.h>
#include <tateyama/framework/transactional_kvs_resource.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/impl/service.h>

namespace jogasaki::api::resource {

namespace framework = tateyama::framework;

bridge::bridge() = default;

framework::component::id_type bridge::id() const noexcept {
    return tag;
}

bool bridge::setup(framework::environment& env) {
    // on maintenance/quiescent mode, sql resource exists, but does nothing.
    if(env.mode() == framework::boot_mode::maintenance_standalone ||
        env.mode() == framework::boot_mode::maintenance_server ||
        env.mode() == framework::boot_mode::quiescent_server) {
        return true;
    }
    if (db_) return true;
    auto kvs = env.resource_repository().find<framework::transactional_kvs_resource>();
    if(! kvs) {
        LOG_LP(ERROR) << "failed to find transactional kvs";
        return false;
    }
    auto cfg = convert_config(*env.configuration());
    if(! cfg) {
        return false;
    }
    db_ = jogasaki::api::create_database(cfg, kvs->core_object());
    return true;
}

bool bridge::start(framework::environment& env) {
    // on maintenance/quiescent mode, sql resource exists, but does nothing.
    if(env.mode() == framework::boot_mode::maintenance_standalone ||
        env.mode() == framework::boot_mode::maintenance_server ||
        env.mode() == framework::boot_mode::quiescent_server) {
        return true;
    }
    auto ret = db_->start() == status::ok;
    started_ = ret;
    return ret;
}

bool bridge::shutdown(framework::environment&) {
    if(started_) {
        if(db_->stop() == status::ok) {
            started_ = false;
            return true;
        }
    }
    return true;
}

bridge::~bridge() {
    if(db_ && started_) {
        db_->stop();
    }
    VLOG(log_info) << "/:tateyama:lifecycle:component:<dtor> " << component_label;
}

jogasaki::api::database* bridge::database() const noexcept {
    return db_.get();
}

std::string_view bridge::label() const noexcept {
    return component_label;
}

template <class ...Args>
bool validate_enum_strings(std::string_view name, std::string_view value, std::int32_t& out, Args...args) {
    std::vector<std::string> allowed{args...};
    std::int32_t idx = 0;
    for(auto&& e : allowed) {
        if(e == value) {
            out = idx;
            return true;
        }
        ++idx;
    }
    std::stringstream ss{};
    for(auto&& e : allowed) {
        ss << e << " ";
    }
    LOG_LP(ERROR) << "invalid configuration - enum value \"" << value << "\" specified for parameter \"" <<
            name << "\". Acceptable values are " << ss.str();
    return false;
}

std::shared_ptr<jogasaki::configuration> convert_config_internal(tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
    auto ret = std::make_shared<jogasaki::configuration>();

    auto jogasaki_config = cfg.get_section("sql");
    if (jogasaki_config == nullptr) {
        return ret;
    }

    if (auto v = jogasaki_config->get<std::size_t>("thread_pool_size")) {
        if(v.has_value()) {
            ret->thread_pool_size(v.value());
        }
    }
    if (auto v = jogasaki_config->get<bool>("prepare_test_tables")) {
        ret->prepare_test_tables(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("prepare_benchmark_tables")) {
        ret->prepare_benchmark_tables(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("prepare_analytics_benchmark_tables")) {
        ret->prepare_analytics_benchmark_tables(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("default_partitions")) {
        ret->default_partitions(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("stealing_enabled")) {
        ret->stealing_enabled(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("enable_index_join")) {
        ret->enable_index_join(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("use_preferred_worker_for_current_thread")) {
        ret->use_preferred_worker_for_current_thread(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("stealing_wait")) {
        ret->stealing_wait(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("task_polling_wait")) {
        ret->task_polling_wait(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("tasked_write")) {
        ret->tasked_write(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("enable_hybrid_scheduler")) {
        ret->enable_hybrid_scheduler(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("lightweight_job_level")) {
        ret->lightweight_job_level(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("busy_worker")) {
        ret->busy_worker(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("watcher_interval")) {
        ret->watcher_interval(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("worker_try_count")) {
        ret->worker_try_count(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("worker_suspend_timeout")) {
        ret->worker_suspend_timeout(v.value());
    }

    constexpr std::string_view KEY_COMMIT_RESPONSE{"commit_response"};
    if (auto v = jogasaki_config->get<std::string>(KEY_COMMIT_RESPONSE)) {
        std::int32_t idx{};
        if(! validate_enum_strings(KEY_COMMIT_RESPONSE, v.value(), idx, "ACCEPTED", "AVAILABLE", "STORED", "PROPAGATED")) {
            return {};
        }
        ret->default_commit_response(static_cast<commit_response_kind>(idx));
    }
    return ret;
}

std::shared_ptr<jogasaki::configuration> convert_config(tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
    try {
        return convert_config_internal(cfg);
    } catch (std::exception const& e) {
        // error should have been logged already
        return {};
    }
}

}

