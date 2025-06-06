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
#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <vector>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>

#include <takatori/datetime/conversion.h>

#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/repository.h>
#include <tateyama/framework/transactional_kvs_resource.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/convert_offset_string.h>

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

std::string toupper(std::string_view src) {
    std::string ret(src.size(), '\0');
    std::transform(src.cbegin(), src.cend(), ret.begin(), ::toupper);
    return ret;
}

template <class ...Args>
bool validate_enum_strings(std::string_view name, std::string_view value, std::int32_t& out, Args...args) {
    std::vector<std::string> allowed{args...};
    std::int32_t idx = 0;
    auto v = toupper(value);
    for(auto&& e : allowed) {
        if(toupper(e) == v) {
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

bool process_sql_config(std::shared_ptr<jogasaki::configuration>& ret, tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
    // sql section
    auto jogasaki_config = cfg.get_section("sql");
    if (jogasaki_config == nullptr) {
        return true;
    }

    if (auto v = jogasaki_config->get<std::size_t>("thread_pool_size")) {
        ret->thread_pool_size(v.value());
    } else {
        constexpr double default_worker_coefficient = 0.8;
        auto physical_cores = boost::thread::physical_concurrency();
        constexpr std::size_t max_default_worker = 32;
        auto workers = std::min(static_cast<std::size_t>(default_worker_coefficient * physical_cores), max_default_worker);
        if(workers == 0) {
            workers = 1;
        }
        ret->thread_pool_size(workers);
    }
    if (auto v = jogasaki_config->get<bool>("prepare_analytics_benchmark_tables")) {
        ret->prepare_analytics_benchmark_tables(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("max_result_set_writers")) {
        const std::size_t writers = v.value();
        if (writers > 256) {
            LOG_LP(ERROR) << "Too large max_result_set_writers (" << writers
                          << ") given. It must be equal to or less than 256.";
            return false;
        }
        ret->max_result_set_writers(writers);
    }
    if (auto v = jogasaki_config->get<std::size_t>("default_partitions")) {
        const std::size_t partitions = v.value();
        if (partitions > ret->max_result_set_writers()) {
            LOG_LP(ERROR) << "Too large default_partitions (" << partitions
                          << ") given. It must be equal to or less than max_result_set_writers ("
                          << ret->max_result_set_writers() << ").";
            return false;
        }
        ret->default_partitions(partitions);
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
    if (auto v = jogasaki_config->get<bool>("lowercase_regular_identifiers")) {
        ret->lowercase_regular_identifiers(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_enable_blob_cast")) {
        ret->enable_blob_cast(v.value());
    }

    constexpr std::string_view KEY_COMMIT_RESPONSE{"commit_response"};
    if (auto v = jogasaki_config->get<std::string>(KEY_COMMIT_RESPONSE)) {
        std::int32_t idx{};
        if(! validate_enum_strings(KEY_COMMIT_RESPONSE, v.value(), idx, "ACCEPTED", "AVAILABLE", "STORED", "PROPAGATED")) {
            ret = {};
            return false;
        }
        ret->default_commit_response(static_cast<commit_response_kind>(idx));
    }

    if (auto v = jogasaki_config->get<bool>("dev_update_skips_deletion")) {
        ret->update_skips_deletion(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_profile_commits")) {
        ret->profile_commits(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_return_os_pages")) {
        ret->return_os_pages(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_omit_task_when_idle")) {
        ret->omit_task_when_idle(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("plan_recording")) {
        ret->plan_recording(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_try_insert_on_upserting_secondary")) {
        ret->try_insert_on_upserting_secondary(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_support_boolean")) {
        ret->support_boolean(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_support_smallint")) {
        ret->support_smallint(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_scan_concurrent_operation_as_not_found")) {
        ret->scan_concurrent_operation_as_not_found(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_point_read_concurrent_operation_as_not_found")) {
        ret->point_read_concurrent_operation_as_not_found(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("scan_block_size")) {
        ret->scan_block_size(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("scan_yield_interval")) {
        ret->scan_yield_interval(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("dev_thousandths_ratio_check_local_first")) {
        ret->thousandths_ratio_check_local_first(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_direct_commit_callback")) {
        ret->direct_commit_callback(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("scan_default_parallel")) {
        const std::size_t parallel = v.value();
        if (parallel > ret->max_result_set_writers()) {
            LOG_LP(ERROR) << "Too large scan_default_parallel (" << parallel
                          << ") given. It must be equalt to or less than max_result_set_writers ("
                          << ret->max_result_set_writers() << ").";
            return false;
        }
        ret->scan_default_parallel(parallel);
    }
    if (auto v = jogasaki_config->get<bool>("dev_inplace_teardown")) {
        ret->inplace_teardown(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_inplace_dag_schedule")) {
        ret->inplace_dag_schedule(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("enable_join_scan")) {
        ret->enable_join_scan(v.value());
    }
    constexpr std::string_view KEY_RTX_KEY_DISTRIBUTION{"dev_rtx_key_distribution"};
    if (auto v = jogasaki_config->get<std::string>(KEY_RTX_KEY_DISTRIBUTION)) {
        std::int32_t idx{};
        if(! validate_enum_strings(KEY_RTX_KEY_DISTRIBUTION, v.value(), idx, "simple", "uniform", "sampling")) {
            ret = {};
            return false;
        }
        ret->key_distribution(static_cast<key_distribution_kind>(idx));
    }
    if (auto v = jogasaki_config->get<std::size_t>("dev_initial_core")) {
        ret->initial_core(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_core_affinity")) {
        ret->core_affinity(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_assign_numa_nodes_uniformly")) {
        ret->assign_numa_nodes_uniformly(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("dev_force_numa_node")) {
        ret->force_numa_node(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_enable_session_store")) {
        ret->enable_session_store(v.value());
    }
    return true;
}

bool process_session_config(std::shared_ptr<jogasaki::configuration>& ret, tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
    // session section
    auto session_config = cfg.get_section("session");
    if (session_config == nullptr) {
        return true;
    }

    if (auto v = session_config->get<std::string>("zone_offset")) {
        std::int32_t offset_ms{};
        if(! utils::convert_offset_string(v.value(), offset_ms)) {
            ret = {};
            return false;
        }
        ret->zone_offset(offset_ms);
    }
    return true;
}

std::shared_ptr<jogasaki::configuration> convert_config_internal(tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
    auto ret = std::make_shared<jogasaki::configuration>();
    if(! process_sql_config(ret, cfg)) {
        return {};
    };
    if(! process_session_config(ret, cfg)) {
        return {};
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

} // namespace  jogasaki::api::resource

