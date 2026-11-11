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
#include "remote_process_task.h"

#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <iterator>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <glog/logging.h>
#include <grpcpp/client_context.h>

#include <takatori/util/downcast.h>
#include <tateyama/proto/distributed_query/remote_catalog.grpc.pb.h>

#include <jogasaki/callback.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/dist/remote_execution_block_proto.h>
#include <jogasaki/dist/remote_execution_result.h>
#include <jogasaki/dist/remote_record_codec.h>
#include <jogasaki/dist/remote_result_queue.h>
#include <jogasaki/dist/remote_server_channel_manager.h>
#include <jogasaki/dist/remote_type_codec.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/common/task_completion.h>
#include <jogasaki/executor/exchange/flow.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/step.h>

#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/conditional_task.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/field_types.h>

#include "remote_sql_error.h"
#include "remote_rpc_error.h"

namespace jogasaki::dist {
namespace {
namespace wire = tateyama::grpc::proto;
} // namespace

static std::optional<std::chrono::milliseconds> timeout() {
    std::size_t milliseconds{};
    if (auto* text = std::getenv("JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS")) {
        auto* end = text;
        std::advance(end, std::char_traits<char>::length(text));
        std::size_t value{};
        auto [position, error] = std::from_chars(text, end, value);
        if (position == end && error == std::errc{} && value > 0 && value <= 600000) {
            milliseconds = value;
        }
    }
    if (milliseconds == 0) {
        return {};
    }
    return std::chrono::milliseconds(milliseconds);
}


namespace {
// Only transport I/O runs here. Request state and result writing stay on scheduler workers.
class rpc_state {
public:
    rpc_state() = default;
    rpc_state(rpc_state const&) = delete;
    rpc_state& operator=(rpc_state const&) = delete;
    rpc_state(rpc_state&&) = delete;
    rpc_state& operator=(rpc_state&&) = delete;

    void cancel() {
        context.TryCancel();
        results.cancel();
    }
    ~rpc_state() {
        cancel();
        if (transport.joinable()) {
            transport.join();
        }
    }
    void start(std::shared_ptr<grpc::Channel> channel, std::string payload, std::size_t job, std::string server) {
        if (auto limit = timeout()) {
            context.set_deadline(std::chrono::system_clock::now() + *limit);
        }
        static std::atomic_uint64_t sequence{};
        execution_id_ = std::to_string(job) + "-" + std::to_string(++sequence);
        transport = std::thread{[this, channel = std::move(channel), payload = std::move(payload), job, server = std::move(server)] {
            receive(channel, payload, job, server);
        }};
    }
    bool ready() { return !results.empty() || done.load(std::memory_order_acquire); }
    [[nodiscard]] std::deque<std::string> take_results() { return results.take(); }
    bool completed() const noexcept { return done.load(std::memory_order_acquire); }
    bool has_results() { return !results.empty(); }
    enum class local_failure { none, request_size, response_shape, exception };
    local_failure failure() const noexcept { return failure_; }
    std::string const& diagnostic() const noexcept { return diagnostic_; }
    std::string const& execution_id() const noexcept { return execution_id_; }
    grpc::Status const& status() const noexcept { return final_status; }
    bool has_rpc_status() const noexcept { return has_rpc_status_; }

private:
    grpc::ClientContext context{};
    remote_result_queue results{};
    grpc::Status final_status{};
    bool has_rpc_status_{};
    local_failure failure_{};
    std::string diagnostic_{};
    std::string execution_id_{};
    std::atomic_bool done{};
    std::thread transport{};
    void receive(std::shared_ptr<grpc::Channel> const& channel, std::string const& payload, std::size_t job,
                 std::string const& server) {
        auto started = std::chrono::steady_clock::now();
        std::size_t request_bytes{};
        std::size_t response_bytes{};
        std::size_t response_count{};
        std::size_t row_count{};
        try {
            wire::RemoteProcessRequest request{};
            if (!request.mutable_execution_block()->ParseFromString(payload)) {
                throw std::invalid_argument("invalid shared execution block");
            }
            auto* metadata = request.mutable_metadata();
            metadata->mutable_protocol_version()->set_major(1);
            metadata->mutable_protocol_version()->set_minor(0);
            metadata->set_coordinator_job_id(std::to_string(job));
            metadata->set_remote_execution_id(execution_id_);
            metadata->set_attempt_number(1);
            request_bytes = request.ByteSizeLong();
            if (request_bytes > std::size_t{64} * 1024U * 1024U) {
                failure_ = local_failure::request_size;
                diagnostic_ = "serialized request bytes:" + std::to_string(request_bytes);
            } else {
                auto stub = wire::DistributedQueryService::NewStub(channel);
                LOG_LP(INFO) << "distributed query rpc:ExecuteRemoteProcess event:start"
                             << " job_id:" << job << " execution_id:" << execution_id_
                             << " server:" << server << " attempt:1 protocol_version:1.0"
                             << " request_bytes:" << request_bytes;
                auto reader = stub->ExecuteRemoteProcess(&context, request);
                wire::RemoteProcessResult response{};
                std::uint64_t expected{};
                bool invalid{};
                while (reader->Read(&response)) {
                    auto bytes = response.ByteSizeLong();
                    ++response_count;
                    response_bytes += bytes;
                    row_count += static_cast<std::size_t>(response.record_list_size());
                    if (response.sequence_number() != expected++ || bytes > std::size_t{64} * 1024U * 1024U) {
                        invalid = true;
                        context.TryCancel();
                        break;
                    }
                    if (!results.push(response.SerializeAsString())) {
                        // Never leave unread messages behind a live call before Finish().
                        context.TryCancel();
                        break;
                    }
                }
                final_status = reader->Finish();
                has_rpc_status_ = true;
                if (invalid) {
                    failure_ = local_failure::response_shape;
                    diagnostic_ = "invalid remote response sequence or size";
                }
            }
        } catch (std::exception const& e) {
            context.TryCancel();
            failure_ = local_failure::exception;
            diagnostic_ = e.what();
        } catch (...) {
            context.TryCancel();
            failure_ = local_failure::exception;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started);
        LOG_LP(INFO) << "distributed query rpc:ExecuteRemoteProcess event:finish"
                     << " job_id:" << job << " execution_id:" << execution_id_
                     << " server:" << server << " attempt:1 protocol_version:1.0"
                     << " request_bytes:" << request_bytes << " response_bytes:" << response_bytes
                     << " response_count:" << response_count << " row_count:" << row_count
                     << " elapsed_us:" << elapsed.count()
                     << " grpc_status:" << (has_rpc_status_ ? std::to_string(final_status.error_code()) : "none");
        done.store(true, std::memory_order_release);
    }
};
class remote_process_task final : public executor::common::task,
                                  public std::enable_shared_from_this<remote_process_task> {
public:
    remote_process_task(request_context& context, executor::process::step& step,
                        executor::process::processor_info const& info, std::string server)
        : executor::common::task(&context, &step), server_(std::move(server)),
          meta_(step.io_info()->output_at(0).meta()), state_(std::make_shared<rpc_state>()) {

        if (server_.empty() || !context.remote_catalogs()) {
            throw std::invalid_argument("remote process requires a retained catalog snapshot and destination server");
        }
        auto block = create_remote_execution_block(info.relations(), &info.compiled_info(), meta_.get(),
                                                   &step.io_info()->output_at(0).column_order(), context.remote_catalogs().get(), server_);
        if (!block) {
            throw std::invalid_argument("remote process requires a supported graph and storage definition IDs");
        }
        payload_ = serialize_remote_execution_block_proto(*block);
        cancel_kind_ = info.details().has_find_operator() ? request_cancel_kind::find : request_cancel_kind::scan;
        auto& exchanges = *step.io_exchange_map();
        auto& downstream = dynamic_cast<executor::exchange::flow&>(
            exchanges.output_at(0)->data_flow_object(context));
        downstream.setup_partitions(1);
        task_context_ = std::make_shared<executor::process::impl::task_context>(
            context, 0, exchanges, nullptr, nullptr, std::vector<std::size_t>{downstream.sink_count() - 1});
    }
    model::task_result operator()() override {
        try {
            return execute();
        } catch (std::exception const& e) {
            LOG_LP(INFO) << "distributed query execution_id:" << state_->execution_id()
                         << " server:" << server_ << " internal_code:REMOTE_TRANSPORT_EXCEPTION reason:" << e.what();
            return fail(status::err_unknown, "[distributed-query] unexpected remote transport error",
                        error_code::internal_exception);
        }
    }
    model::task_result execute() {
        if (!started_) {
            auto* channels = context()->remote_server_channels();
            auto channel = channels ? channels->find_channel(server_) : nullptr;
            if (!channel) {
                return fail_local(status::err_not_found, "REMOTE_CHANNEL_NOT_FOUND",
                                  "[distributed-query] remote server channel not found");
            }
            auto job = context()->job()->id();
            try {
                state_->start(std::move(channel), std::move(payload_), job, server_);
            } catch (std::exception const& e) {
                return fail_local(status::err_resource_limit_reached, "REMOTE_TRANSPORT_START_FAILED",
                                  "[distributed-query] failed to start remote process transport", e.what());
            }
            started_ = true;
        }
        for (auto const& payload : state_->take_results()) {
            if (auto result = write_result(payload)) {
                return *result;
            }
        }
        if (state_->completed()) {
            // The transport may publish one final chunk between the swap and done check.
            if (state_->has_results()) {
                return wait();
            }
            return complete_rpc();
        }
        return wait();
    }
    [[nodiscard]] model::task_transaction_kind transaction_capability() override {
        return model::task_transaction_kind::in_transaction;
    }

private:
    [[nodiscard]] std::optional<model::task_result> write_result(std::string const& payload) {
        std::string error{};
        wire::RemoteProcessResult result{};
        if (!result.ParseFromString(payload)) {
            return invalid_result("invalid remote result envelope");
        }
        // The output schema comes from the request's execution block, not from a result envelope.
        // Validate the complete batch before publishing any row to the downstream exchange.
        data::small_record_store checked{meta_};
        for (auto const& row : result.record_list()) {
            if (!decode_remote_record(row, *meta_, checked.ref(), error)) {
                return invalid_result(error);
            }
        }
        for (auto const& row : result.record_list()) {
            data::small_record_store record{meta_};
            if (!decode_remote_record(row, *meta_, record.ref(), error)) {
                return invalid_result(error);
            }
            if (!writer_) {
                writer_ = task_context_->downstream_writer(0);
            }
            if (!writer_) {
                return fail_local(status::err_io_error, "REMOTE_DOWNSTREAM_FAILED",
                                  "[distributed-query] remote downstream writer unavailable");
            }
            if (!writer_->write(record.ref())) {
                return fail_local(status::err_io_error, "REMOTE_DOWNSTREAM_FAILED",
                                  "[distributed-query] remote downstream write failed");
            }
        }
        return std::nullopt;
    }
    model::task_result invalid_result(std::string_view reason) {
        LOG_LP(INFO) << "distributed query execution_id:" << state_->execution_id()
                     << " server:" << server_ << " internal_code:INVALID_REMOTE_RESULT reason:" << reason;
        return fail(status::err_invalid_state,
                    "[distributed-query] failed to decode remote result record", error_code::internal_exception);
    }
    model::task_result complete_rpc() {
        if (state_->has_rpc_status()) {
            LOG_LP(INFO) << "distributed query execution_id:" << state_->execution_id()
                         << " server:" << server_ << " rpc:ExecuteRemoteProcess grpc_status:"
                         << static_cast<int>(state_->status().error_code())
                         << " grpc_message:" << state_->status().error_message();
        }
        using local_failure = rpc_state::local_failure;
        auto diagnose = [&](std::string_view code) {
            LOG_LP(INFO) << "distributed query execution_id:" << state_->execution_id()
                         << " server:" << server_ << " internal_code:" << code
                         << " reason:" << state_->diagnostic();
        };
        switch (state_->failure()) {
            case local_failure::request_size:
                diagnose("REMOTE_REQUEST_TOO_LARGE");
                return fail(status::err_resource_limit_reached,
                            "[distributed-query] remote request exceeds RPC message size limit");
            case local_failure::response_shape:
                diagnose("INVALID_REMOTE_RESULT");
                return fail(status::err_invalid_state,
                            "[distributed-query] invalid remote response sequence or size", error_code::internal_exception);
            case local_failure::exception:
                diagnose("REMOTE_TRANSPORT_EXCEPTION");
                return fail(status::err_unknown,
                            "[distributed-query] unexpected remote transport error", error_code::internal_exception);
            case local_failure::none:
                break;
        }
        // Rows of earlier messages may already be downstream; a failed status fails the whole request.
        if (!state_->status().ok()) {
            auto failure = classify_remote_rpc_error(state_->status().error_code());
            bool protocol_error{};
            if (auto remote =
                    deserialize_remote_sql_error(state_->status().error_details(), state_->status().error_code(),
                                                 state_->status().error_message(), protocol_error)) {
                state_->cancel();
                if (auto existing = context()->error_info(); existing && *existing) {
                    return finish();
                }
                if (!proto::sql::error::Code_IsValid(remote->code())) {
                    LOG_LP(INFO) << "distributed query execution_id:" << state_->execution_id()
                                 << " server:" << server_ << " unknown_sql_code:" << static_cast<int>(remote->code())
                                 << " message:" << remote->detail() << " supplemental_text:" << remote->supplemental_text();
                }
                error::set_error_info(*context(), restore_remote_sql_error(*remote));
                return finish();
            }
            if (protocol_error) {
                return fail(status::err_invalid_state, "[distributed-query] remote SQL status code or message mismatch",
                            error_code::internal_exception);
            }
            if (state_->status().error_code() == grpc::StatusCode::CANCELLED &&
                client_cancelled_.load(std::memory_order_acquire)) {
                return fail(status::request_canceled, "remote process cancelled");
            }
            return fail(failure.result, remote_rpc_message("ExecuteRemoteProcess", failure), failure.code);
        }
        return finish();
    }
    model::task_result finish() {
        if (finished_) {
            return model::task_result::complete;
        }
        if (writer_) {
            auto* writer = std::exchange(writer_, nullptr);
            auto report = [&](std::string_view reason) {
                LOG_LP(INFO) << "distributed query execution_id:" << state_->execution_id()
                             << " server:" << server_ << " internal_code:REMOTE_DOWNSTREAM_FAILED reason:" << reason;
                if (auto existing = context()->error_info(); !existing || !*existing) {
                    error::set_error_info(*context(), create_error_info(error_code::sql_execution_exception,
                        "[distributed-query] remote downstream write failed", status::err_io_error));
                }
            };
            try {
                writer->flush();
            } catch (std::exception const& e) {
                report(e.what());
            }
            try {
                writer->release();
            } catch (std::exception const& e) {
                report(e.what());
            }
        }
        task_context_->deactivate_writer(0);
        finished_ = true;
        executor::common::complete_dag_task(*context(), step()->id(), id());
        return model::task_result::complete;
    }
    model::task_result wait() {
        auto self = shared_from_this();
        context()->scheduler()->schedule_conditional_task(scheduler::conditional_task{
            context(),
            [self] {
                auto response = self->context()->req_info().response_source();
                if (utils::request_cancel_enabled(self->cancel_kind_) && response && response->check_cancel()) {
                    self->client_cancelled_.store(true, std::memory_order_release);
                    self->state_->cancel();
                } else if (self->context()->status_code() != status::ok) {
                    self->state_->cancel();
                }
                return self->state_->ready();
            },
            [self] {
                self->context()->scheduler()->schedule_task(scheduler::flat_task{
                    scheduler::task_enum_tag<scheduler::flat_task_kind::wrapped>, self->context(), self});
            }});
        return model::task_result::complete;
    }
    model::task_result fail_local(status result, std::string_view diagnostic, std::string const& message,
                                  std::string_view reason = {}) {
        LOG_LP(INFO) << "distributed query execution_id:" << state_->execution_id()
                     << " server:" << server_ << " internal_code:" << diagnostic
                     << " status:" << result << " reason:" << reason;
        return fail(result, message);
    }
    model::task_result fail(status status, const std::string& message,
                            error_code code = error_code::sql_execution_exception) {
        state_->cancel();
        if (auto existing = context()->error_info(); existing && *existing) {
            return finish();
        }
        if (status == status::request_canceled) {
            utils::cancel_request_impl(*context(), __FILE__, "remote process cancelled");
        } else {
            error::set_error_info(*context(), create_error_info(code, message, status));
        }
        return finish();
    }
    std::string server_{};
    std::string payload_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::shared_ptr<executor::process::impl::task_context> task_context_{};
    executor::io::record_writer* writer_{};
    std::shared_ptr<rpc_state> state_{};
    bool started_{};
    bool finished_{};
    request_cancel_kind cancel_kind_{request_cancel_kind::scan};
    std::atomic_bool client_cancelled_{};
};
class failed_remote_process_task final : public executor::common::task {
public:
    failed_remote_process_task(request_context& context, executor::process::step& step, std::string message, error_code code, status result)
        : executor::common::task(&context, &step), message_(std::move(message)), code_(code), result_(result) {}
    model::task_result operator()() override {
        if (auto existing = context()->error_info(); !existing || !*existing) {
            error::set_error_info(*context(), create_error_info(code_, message_, result_));
        }
        executor::common::complete_dag_task(*context(), step()->id(), id());
        return model::task_result::complete;
    }
    [[nodiscard]] model::task_transaction_kind transaction_capability() override {
        return model::task_transaction_kind::in_transaction;
    }

private:
    std::string message_{};
    error_code code_;
    status result_;
};
} // namespace
std::shared_ptr<model::task> make_remote_process_task(request_context& context, executor::process::step& step,
                                                      executor::process::processor_info const& info,
                                                      std::string server) {
    if (!context.transaction() || !context.transaction()->option() ||
        !context.transaction()->option()->readonly()) {
        LOG_LP(INFO) << "distributed query internal_code:RTX_REQUIRED";
        return std::make_shared<failed_remote_process_task>(
            context, step, "[distributed-query] execution requires an RTX",
            error_code::restricted_operation_exception, status::err_illegal_operation);
    }
    if (server.empty() || !context.remote_catalogs()) {
        LOG_LP(INFO) << "distributed query internal_code:REMOTE_METADATA_MISSING";
        return std::make_shared<failed_remote_process_task>(
            context, step, "[distributed-query] required remote execution metadata is missing",
            error_code::internal_exception, status::err_invalid_state);
    }
    try {
        if (step.io_info()->output_count() != 1 || step.io_exchange_map()->output_count() != 1) {
            throw std::invalid_argument("remote process requires exactly one output");
        }
        return std::make_shared<remote_process_task>(context, step, info, std::move(server));
    } catch (std::invalid_argument const& e) {
        LOG_LP(INFO) << "distributed query server:" << server
                     << " internal_code:UNSUPPORTED_REMOTE_PLAN reason:" << e.what();
        return std::make_shared<failed_remote_process_task>(
            context, step, "[distributed-query] unsupported remote execution plan",
            error_code::unsupported_runtime_feature_exception, status::err_unsupported);
    } catch (std::exception const& e) {
        LOG_LP(INFO) << "distributed query internal_code:REMOTE_TRANSPORT_EXCEPTION reason:" << e.what();
        return std::make_shared<failed_remote_process_task>(
            context, step, "[distributed-query] unexpected remote transport error",
            error_code::internal_exception, status::err_unknown);
    }
}
} // namespace jogasaki::dist
