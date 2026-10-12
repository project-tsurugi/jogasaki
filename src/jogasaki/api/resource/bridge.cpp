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
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <vector>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>

#include <takatori/datetime/conversion.h>

#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/repository.h>
#include <tateyama/framework/transactional_kvs_resource.h>
#include <tateyama/grpc/blob_relay/service_adapter.h>
#include <tateyama/grpc/distributed_query/service_adapter.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/dist/local_catalog_handler.h>
#include <jogasaki/dist/remote_process_handler.h>
#include <jogasaki/executor/global.h>
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
    cfg_ = convert_config(*env.configuration());
    return cfg_ != nullptr;
}

static void borrow_relay_service(framework::environment& env) {
    // Borrow relay service instance and make it available as global::relay_service().
    // Whether the BLOB relay service is available depends on the server configuration, but that is
    // out of jogasaki's knowledge. So simply store whatever is given here (possibly nullptr) and
    // check its availability lazily when the BLOB relay service is really needed.

    auto proxy = env.resource_repository().find<::tateyama::grpc::blob_relay::service_adapter>();
    if (! proxy) {
        LOG_LP(INFO) << "data relay service is not available";
        global::relay_service(nullptr);
        return;
    }
    auto relay_service = proxy->blob_relay_service();
    if (! relay_service) {
        LOG_LP(INFO) << "data relay service is not available";
    }
    global::relay_service(std::move(relay_service));
}

bool bridge::start(framework::environment& env) {
    // on maintenance/quiescent mode, sql resource exists, but does nothing.
    if(env.mode() == framework::boot_mode::maintenance_standalone ||
        env.mode() == framework::boot_mode::maintenance_server ||
        env.mode() == framework::boot_mode::quiescent_server) {
        return true;
    }

    borrow_relay_service(env);

    if (! db_) {
        auto kvs = env.resource_repository().find<framework::transactional_kvs_resource>();
        if(! kvs) {
            LOG_LP(ERROR) << "failed to find transactional kvs";
            return false;
        }
        db_ = jogasaki::api::create_database(cfg_, kvs->core_object());
    }

    auto ret = db_->start() == status::ok;
    started_ = ret;
    auto remote = env.configuration()->get_section("distributed_query_remote");
    if (ret && remote != nullptr && remote->get<bool>("enabled").value_or(false)) {
        auto adapter = env.resource_repository().find<tateyama::grpc::distributed_query::service_adapter>();
        if (!adapter) {
            LOG_LP(ERROR) << "distributed query remote service adapter is unavailable";
            db_->stop();
            started_ = false;
            return false;
        }
        remote_service_adapter_ = adapter;
        catalog_handler_ = std::make_shared<dist::local_catalog_handler>(*db_);
        adapter->set_remote_catalog_handler(catalog_handler_);
#ifdef ENABLE_DISTRIBUTED_QUERY_FIXTURES
        remote_process_handler_ = std::make_shared<dist::remote_process_handler>(*db_);
        adapter->set_remote_process_handler(remote_process_handler_);
#endif
    }
    return ret;
}

bool bridge::shutdown(framework::environment&) {
    unregister_remote_handlers();
    if(started_) {
        if(db_->stop() == status::ok) {
            started_ = false;
        }
    }
    global::relay_service(nullptr);
    return true;
}

void bridge::unregister_remote_handlers() {
    if (remote_process_handler_ || catalog_handler_) {
        auto adapter = remote_service_adapter_.lock();
        if (adapter && remote_process_handler_) adapter->set_remote_process_handler({});
        if (adapter && catalog_handler_) adapter->set_remote_catalog_handler({});
        remote_process_handler_.reset();
        catalog_handler_.reset();
    }
    remote_service_adapter_.reset();
}

bridge::~bridge() {
    unregister_remote_handlers();
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

static std::string toupper(std::string_view src) {
    std::string ret(src.size(), '\0');
    std::transform(src.cbegin(), src.cend(), ret.begin(), ::toupper);
    return ret;
}

template <class ...Args>
static bool validate_enum_strings(std::string_view name, std::string_view value, std::int32_t& out, Args...args) {
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

static bool process_sql_config(std::shared_ptr<jogasaki::configuration>& ret, tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
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
        if (writers > 256 || writers == 0) {
            LOG_LP(ERROR) << "Invalid max_result_set_writers (" << writers
                          << ") It must be greater than zero and less than or equal to 256.";
            return false;
        }
        ret->max_result_set_writers(writers);
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
        ret->scan_default_parallel(v.value());
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
    if (auto v = jogasaki_config->get<bool>("dev_log_msg_user_data")) {
        ret->log_msg_user_data(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_enable_disjunction_range_hinting")) {
        ret->enable_disjunction_range_hinting(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("dev_apply_max_polls")) {
        ret->apply_max_polls(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_enable_maintenance_thread")) {
        ret->enable_maintenance_thread(v.value());
    }
    if (auto v = jogasaki_config->get<std::size_t>("dev_maintenance_interval_ms")) {
        ret->maintenance_interval_ms(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("dev_enable_truncate")) {
        ret->enable_truncate(v.value());
    }
    return true;
}

static bool process_session_config(std::shared_ptr<jogasaki::configuration>& ret, tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
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

static bool process_udf_config(std::shared_ptr<jogasaki::configuration>& ret, tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
    // udf section
    auto jogasaki_config = cfg.get_section("udf");
    if (jogasaki_config == nullptr) {
        return true;
    }
    if (auto v = jogasaki_config->get<std::filesystem::path>("plugin_directory")) {
        if(v->empty()) {
            LOG_LP(ERROR) << "plugin_directory` in the UDF section must not be empty";
            return false;
        }
        ret->plugin_directory(v->string());
    }
    if (auto v = jogasaki_config->get<std::string>("endpoint")) {
        ret->endpoint(v.value());
    }
    if (auto v = jogasaki_config->get_vector<bool>("secure", "|")) {
        ret->secure_values(v.value());
    }
    if (auto v = jogasaki_config->get<size_t>("timeout")) {
        ret->udf_client_timeout(v.value());
    }
    return true;
}

static bool process_grpc_server_config(std::shared_ptr<jogasaki::configuration>& ret, tateyama::api::configuration::whole& cfg) {
    // grpc_server section
    auto jogasaki_config = cfg.get_section("grpc_server");
    if (jogasaki_config == nullptr) {
        return true;
    }
    if (auto v = jogasaki_config->get<std::string>("endpoint")) {
        ret->grpc_server_endpoint(v.value());
    }
    if (auto v = jogasaki_config->get<bool>("secure")) {
        ret->grpc_server_secure(v.value());
    }
    return true;
}

[[nodiscard]] static bool has_prefix(std::string_view value, std::string_view prefix) noexcept {
    return value.size() >= prefix.size() && value.substr(0, prefix.size()) == prefix;
}

[[nodiscard]] static bool validate_remote_server_uri(std::string_view uri) {
    constexpr std::string_view dns_prefix{"dns:///"};
    constexpr std::string_view unix_prefix{"unix://"};
    if (has_prefix(uri, dns_prefix)) {
        return uri.size() > dns_prefix.size();
    }
    if (has_prefix(uri, unix_prefix)) {
        auto const path = uri.substr(unix_prefix.size());
        return ! path.empty() && path.front() == '/';
    }
    return false;
}

[[nodiscard]] static std::optional<std::filesystem::path> resolve_root_certificate_path(
    tateyama::api::configuration::whole& cfg,
    std::string_view value)
{
    std::filesystem::path path{std::string{value}};
    if (path.is_absolute()) {
        return path;
    }
    if (auto base = cfg.base_path(); base) {
        return *base / path;
    }
    LOG_LP(ERROR) << "invalid configuration - distributed_query_client.root_cert contains relative path \""
                  << value << "\" but the configuration base path is empty";
    return std::nullopt;
}

[[nodiscard]] static std::optional<std::string> read_root_certificate(
    tateyama::api::configuration::whole& cfg,
    std::string_view value)
{
    auto path = resolve_root_certificate_path(cfg, value);
    if (! path) {
        return std::nullopt;
    }
    std::ifstream input{*path, std::ios::binary};
    if (! input) {
        LOG_LP(ERROR) << "invalid configuration - distributed_query_client.root_cert file is not readable: "
                      << path->string();
        return std::nullopt;
    }
    std::string contents{
        std::istreambuf_iterator<char>{input},
        std::istreambuf_iterator<char>{}
    };
    if (!boost::algorithm::contains(contents, "-----BEGIN CERTIFICATE-----") ||
        !boost::algorithm::contains(contents, "-----END CERTIFICATE-----")) {
        LOG_LP(ERROR) << "invalid configuration - distributed_query_client.root_cert file does not contain PEM certificate: "
                      << path->string();
        return std::nullopt;
    }
    return contents;
}

static bool process_distributed_query_catalog_config(
    std::shared_ptr<jogasaki::configuration>& ret,
    tateyama::api::configuration::whole& cfg)
{
    auto section = cfg.get_section("distributed_query_client");
    if (!section) { return true; }
    dist::remote_catalog_options options{};
    auto source = section->get<std::string>("catalog_source").value_or("disabled");
    if (source == "disabled") { options.source = dist::remote_catalog_source::disabled; }
    else if (source == "rpc") { options.source = dist::remote_catalog_source::rpc; }
    else if (source == "fixture") { options.source = dist::remote_catalog_source::fixture; }
    else {
        LOG_LP(ERROR) << "invalid distributed_query_client.catalog_source: " << source;
        return false;
    }
    auto fixture = section->get<std::string>("fixture_path").value_or("");
    if (options.source == dist::remote_catalog_source::fixture) {
        if (!dist::distributed_query_fixtures_available() || fixture.empty()) {
            LOG_LP(ERROR) << "fixture catalogs require ENABLE_DISTRIBUTED_QUERY_FIXTURES and fixture_path";
            return false;
        }
        std::filesystem::path path{fixture};
        if (!path.is_absolute()) {
            auto base = cfg.base_path();
            if (!base) {
                LOG_LP(ERROR) << "relative fixture_path requires configuration base path";
                return false;
            }
            path = *base / path;
        }
        options.fixture_path = path.string();
    } else if (!fixture.empty()) {
        LOG_LP(ERROR) << "fixture_path requires catalog_source=fixture";
        return false;
    }
    ret->distributed_query_catalog(std::move(options));
    return true;
}

static bool process_distributed_query_client_config(
    std::shared_ptr<jogasaki::configuration>& ret,
    tateyama::api::configuration::whole& cfg)
{
    auto section = cfg.get_section("distributed_query_client");
    if (section == nullptr) {
        return true;
    }

    auto names = section->get_vector<std::string>("name", "|");
    auto uris = section->get_vector<std::string>("uri", "|");
    if (! names && ! uris) {
        return true;
    }
    if (! names || ! uris) {
        LOG_LP(ERROR) << "invalid configuration - distributed_query_client.name and uri must be specified together";
        return false;
    }
    if (names->size() == 1 && uris->size() == 1 && names->front().empty() && uris->front().empty()) {
        return true;
    }
    if (names->size() != uris->size()) {
        LOG_LP(ERROR) << "invalid configuration - distributed_query_client.name and uri count mismatch";
        return false;
    }

    auto secure_values = section->get_vector<bool>("secure", "|").value_or(std::vector<bool>(names->size(), false));
    if (secure_values.size() != names->size()) {
        LOG_LP(ERROR) << "invalid configuration - distributed_query_client.secure count mismatch";
        return false;
    }

    auto root_cert_values = section->get_vector<std::string>("root_cert", "|")
        .value_or(std::vector<std::string>(names->size(), "grpc_default"));
    if (root_cert_values.size() != names->size()) {
        LOG_LP(ERROR) << "invalid configuration - distributed_query_client.root_cert count mismatch";
        return false;
    }

    std::vector<dist::remote_server_configuration> remote_servers{};
    remote_servers.reserve(names->size());
    for (std::size_t i = 0; i < names->size(); ++i) {
        if ((*names)[i].empty()) {
            LOG_LP(ERROR) << "invalid configuration - distributed_query_client.name contains empty element";
            return false;
        }
        if ((*uris)[i].empty() || ! validate_remote_server_uri((*uris)[i])) {
            LOG_LP(ERROR) << "invalid configuration - distributed_query_client.uri contains unsupported value: "
                          << (*uris)[i];
            return false;
        }

        dist::remote_server_configuration remote_server{
            (*names)[i],
            (*uris)[i],
            secure_values[i],
            dist::root_certificate_source::none,
            {},
        };
        if (remote_server.secure) {
            if (boost::algorithm::iequals(root_cert_values[i], "grpc_default")) {
                remote_server.root_certificate = dist::root_certificate_source::grpc_default_roots;
            } else if (auto pem = read_root_certificate(cfg, root_cert_values[i]); pem) {
                remote_server.root_certificate = dist::root_certificate_source::pem;
                remote_server.pem_root_certs = std::move(*pem);
            } else {
                return false;
            }
        }
        remote_servers.emplace_back(std::move(remote_server));
    }
    ret->distributed_query_remote_servers(std::move(remote_servers));
    return true;
}

static std::shared_ptr<jogasaki::configuration> convert_config_internal(tateyama::api::configuration::whole& cfg) {  //NOLINT(readability-function-cognitive-complexity)
    auto ret = std::make_shared<jogasaki::configuration>();
    if(! process_sql_config(ret, cfg)) {
        return {};
    };
    if(! process_session_config(ret, cfg)) {
        return {};
    }
    if(! process_udf_config(ret, cfg)) {
        return {};
    }
    if(! process_grpc_server_config(ret, cfg)) {
        return {};
    }
    auto coordinator = cfg.get_section("distributed_query_coordinator");
    bool coordinator_enabled = coordinator != nullptr && coordinator->get<bool>("enabled").value_or(false);
    ret->distributed_query_coordinator_enabled(coordinator_enabled);
    LOG_LP(INFO) << "distributed_query_coordinator.enabled=" << coordinator_enabled;
    if (coordinator_enabled) {
        if (! process_distributed_query_catalog_config(ret, cfg) ||
            ! process_distributed_query_client_config(ret, cfg)) {
            return {};
        }
        if (ret->distributed_query_remote_servers().empty()) {
            LOG_LP(ERROR) << "distributed_query_coordinator.enabled=true requires at least one remote server";
            return {};
        }
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

