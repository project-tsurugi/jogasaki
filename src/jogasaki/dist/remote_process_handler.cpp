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
#include "remote_process_handler.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>

#include <takatori/plan/forward.h>
#include <takatori/relation/step/offer.h>
#include <takatori/scalar/immediate.h>
#include <takatori/type/primitive.h>
#include <takatori/util/downcast.h>
#include <takatori/value/primitive.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/dist/remote_execution_block_decoder.h>
#include <jogasaki/dist/remote_execution_graph_builder.h>
#include <jogasaki/dist/remote_execution_graph_compiler.h>
#include <jogasaki/dist/remote_execution_result.h>
#include <jogasaki/dist/remote_record_stream.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/impl/process_executor.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/process_preparation.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/utils/field_types.h>

#include "remote_record_codec.h"
#include "remote_sql_error.h"
#include "remote_type_codec.h"

namespace jogasaki::dist {

namespace {

namespace scalar = takatori::scalar;
namespace type = takatori::type;
namespace value = takatori::value;
namespace dq = tateyama::grpc::distributed_query;
using remote_error = dq::remote_process_error_kind;

#ifdef ENABLE_DISTRIBUTED_QUERY_FIXTURES

class transaction_abort_guard {
public:
    explicit transaction_abort_guard(std::shared_ptr<transaction_context> transaction) noexcept
        : transaction_(std::move(transaction)) {}

    ~transaction_abort_guard() {
        if (!transaction_) {
            return;
        }
        if (direct_) {
            (void)transaction_->abort_transaction();
            return;
        }
        executor::abort_transaction(std::move(transaction_));
    }

    transaction_abort_guard(transaction_abort_guard const&) = delete;
    transaction_abort_guard& operator=(transaction_abort_guard const&) = delete;
    transaction_abort_guard(transaction_abort_guard&&) = delete;
    transaction_abort_guard& operator=(transaction_abort_guard&&) = delete;

    void release() noexcept { transaction_.reset(); }

    // Abort without the task count check, for an interrupted job whose count may never drop.
    void abort_directly() noexcept { direct_ = true; }

private:
    std::shared_ptr<transaction_context> transaction_{};
    bool direct_{};
};

// Expose transport cancellation through the executor's existing response check.
class remote_cancellation_response final : public tateyama::api::server::response {
public:
    explicit remote_cancellation_response(std::function<bool()> check) : check_(std::move(check)) {}
    bool check_cancel() const override {
        if (cancelled_.load(std::memory_order_acquire)) {
            return true;
        }
        if (check_ && check_()) {
            cancelled_.store(true, std::memory_order_release);
        }
        return cancelled_.load(std::memory_order_acquire);
    }
    void session_id(std::size_t) override {}
    void error(tateyama::proto::diagnostics::Record const&) override {}
    tateyama::status body_head(std::string_view) override { return tateyama::status::ok; }
    tateyama::status body(std::string_view) override { return tateyama::status::ok; }
    tateyama::status acquire_channel(std::string_view, std::shared_ptr<tateyama::api::server::data_channel>&,
                                     std::size_t) override {
        return tateyama::status::operation_denied;
    }
    tateyama::status release_channel(tateyama::api::server::data_channel&) override {
        return tateyama::status::operation_denied;
    }
    tateyama::status add_blob(std::unique_ptr<tateyama::api::server::blob_info>) override {
        return tateyama::status::operation_denied;
    }

private:
    std::function<bool()> check_{};
    mutable std::atomic_bool cancelled_{};
};

// Encode at the output boundary instead of accumulating an unbounded forward partition.
class bounded_remote_result {
public:
    bounded_remote_result(remote_execution_result const& result, meta::record_meta const& meta,
                          dq::remote_process_handler::result_sink sink,
                          dq::remote_process_handler::record_sink const& records,
                          std::function<bool()> const& cancelled)
        : header_(records ? std::string{} : serialize_remote_execution_result(result)), payload_(header_), meta_(meta), sink_(std::move(sink)) {
        if (records) {
            records_ = std::make_unique<remote_record_stream_writer>([records](auto const& batch) {
                std::vector<std::string> rows{batch.record_list().begin(), batch.record_list().end()};
                return records(rows) ? status::ok : status::request_canceled;
            }, remote_message_limit, cancelled);
        }
    }
    bool write(accessor::record_ref record) {
        // Runs inside an executor task; no exception may escape to the scheduler.
        try {
            return write_impl(record);
        } catch (...) {
            std::scoped_lock<std::mutex> lock{mutex_};
            internal_error_ = true;
            failed_ = true;
            return false;
        }
    }
    bool finish() {
        try {
            std::scoped_lock<std::mutex> lock{mutex_};
            return !overflow_ && !failed_ && (records_ ? update_status(records_->finish()) : (!sink_ || flush_locked()));
        } catch (...) {
            std::scoped_lock<std::mutex> lock{mutex_};
            internal_error_ = true;
            failed_ = true;
            return false;
        }
    }
    [[nodiscard]] bool overflow() const noexcept { return overflow_; }
    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] bool internal_error() const noexcept { return internal_error_; }
    std::string release() { return sink_ || records_ ? std::string{} : std::move(payload_); }

private:
    bool update_status(status result) {
        overflow_ = overflow_ || result == status::err_resource_limit_reached;
        failed_ = failed_ || result != status::ok;
        return result == status::ok;
    }
    bool write_impl(accessor::record_ref record) {
        auto row = encode_remote_record(record, meta_);
        std::scoped_lock<std::mutex> lock{mutex_};
        if (records_) {
            if (!update_status(records_->append(row))) return false;
            record_bytes_ += row.size();
            if (record_bytes_ >= std::size_t{1024} * 1024U) {
                record_bytes_ = 0;
                return update_status(records_->flush());
            }
            return true;
        }
        using wire = google::protobuf::internal::WireFormatLite;
        constexpr auto field = proto::distributed_query::RemoteExecutionResult::kEncodedRowsFieldNumber;
        auto overhead = static_cast<std::size_t>(wire::TagSize(field, wire::TYPE_BYTES)) +
                        google::protobuf::io::CodedOutputStream::VarintSize32(static_cast<std::uint32_t>(row.size()));
        // Reserve the outer sequence/tag/length envelope. Never split an encoded record.
        auto limit = std::size_t{64} * 1024U * 1024U - (sink_ ? 32U : 0U);
        if (overflow_ || failed_) {
            return false;
        }
        if (header_.size() > limit || row.size() > limit - header_.size() ||
            overhead > limit - header_.size() - row.size()) {
            overflow_ = true;
            return false;
        }
        if (sink_ && payload_.size() > header_.size() &&
            (payload_.size() >= std::size_t{1024} * 1024U || row.size() + overhead > limit - payload_.size())) {
            if (!flush_locked()) {
                return false;
            }
        }
        if (payload_.size() > limit || row.size() + overhead > limit - payload_.size()) {
            overflow_ = true;
            return false;
        }
        google::protobuf::io::StringOutputStream stream{&payload_};
        google::protobuf::io::CodedOutputStream out{&stream};
        wire::WriteBytes(field, row, &out);
        return true;
    }
    bool flush_locked() {
        try {
            if (!sink_(payload_)) {
                failed_ = true;
                return false;
            }
        } catch (...) {
            internal_error_ = true;
            failed_ = true;
            return false;
        }
        payload_ = header_;
        return true;
    }
    std::unique_ptr<remote_record_stream_writer> records_{};
    std::size_t record_bytes_{};
    std::string header_{};
    std::string payload_{};
    meta::record_meta const& meta_;
    dq::remote_process_handler::result_sink sink_{};
    std::mutex mutex_{};
    bool overflow_{};
    bool failed_{};
    bool internal_error_{};
};
class remote_result_writer final : public executor::io::record_writer {
public:
    remote_result_writer(bounded_remote_result& result, executor::io::record_writer& writer)
        : result_(std::addressof(result)), writer_(std::addressof(writer)) {}
    bool write(accessor::record_ref record) override { return result_->write(record); }
    void flush() override {}
    void release() override {
        if (!released_) {
            released_ = true;
            writer_->release();
        }
    }

private:
    bounded_remote_result* result_{};
    executor::io::record_writer* writer_{};
    bool released_{};
};
class remote_result_context final : public executor::process::impl::task_context {
public:
    remote_result_context(std::shared_ptr<executor::process::impl::task_context> source,
                          executor::process::io_exchange_map const& exchanges, bounded_remote_result& result)
        : task_context(*source->req_context(), source->partition(), exchanges,
                       std::shared_ptr<executor::process::impl::scan_range>{
                           source, const_cast<executor::process::impl::scan_range*>(
                                       dynamic_cast<executor::process::impl::scan_range const*>(source->range()))},
                       source->channel(), {source->partition()}),
          source_(std::move(source)), writer_(result, *source_->downstream_writer(0)) {
        // Preserve the working area prepared by flow::create_task_context().
        work_context(source_->release_work());
    }
    executor::io::record_writer* downstream_writer(writer_index) override { return &writer_; }
    void deactivate_writer(writer_index index) override { source_->deactivate_writer(index); }

private:
    std::shared_ptr<executor::process::impl::task_context> source_{};
    remote_result_writer writer_;
};

} // namespace

[[nodiscard]] static std::optional<std::int64_t> parse_int8(std::string text) {
    if (auto begin = text.rfind('('); begin != std::string::npos && !text.empty() && text.back() == ')') {
        text = text.substr(begin + 1, text.size() - begin - 2);
    }
    char* end{};
    errno = 0;
    auto result = std::strtoll(text.c_str(), std::addressof(end), 10);
    if (errno == ERANGE || end == text.c_str() || *end != '\0') {
        return {};
    }
    return result;
}

namespace {

class catalog_graph_resolver final : public remote_execution_graph_resolver {
public:
    catalog_graph_resolver(api::database& database, remote_execution_block const& block)
        : database_(&database), relation_id_(block.operators.back().offer->external_relation_id) {
        std::vector<takatori::descriptor::variable> columns{};
        columns.reserve(block.result_schema.size());
        for (auto const& column : block.result_schema) {
            columns.emplace_back(bindings_.exchange_column("remote_result_" + std::to_string(column.column_id)));
            column_positions_.emplace(column.column_id, columns.size() - 1);
        }
        output_ = std::make_unique<takatori::plan::forward>(std::move(columns));
    }

    std::optional<takatori::descriptor::relation> resolve_storage(std::string_view table_name,
                                                                  std::string_view index_name) const override {
        auto index = find_index(table_name, index_name);
        if (!index) {
            return {};
        }
        return bindings_.index(*index);
    }

    std::optional<takatori::descriptor::variable> resolve_storage_column(std::string_view table_name,
                                                                         std::string_view index_name,
                                                                         std::string_view column_name) const override {
        auto index = find_index(table_name, index_name);
        if (!index) {
            return {};
        }
        for (auto const& column : index->table().columns()) {
            if (column.simple_name() == column_name) {
                if (column.type().kind() == type::type_kind::blob || column.type().kind() == type::type_kind::clob ||
                    column.type().kind() == type::type_kind::boolean || column.type().kind() == type::type_kind::int1 ||
                    column.type().kind() == type::type_kind::int2 || column.type().kind() == type::type_kind::bit ||
                    column.type().kind() == type::type_kind::datetime_interval) {
                    unsupported_column_ = true;
                    return {};
                }
                return bindings_.table_column(column);
            }
        }
        return {};
    }

    std::optional<takatori::descriptor::variable> resolve_storage_key_column(std::string_view table_name,
                                                                             std::string_view index_name,
                                                                             std::size_t position) const override {
        auto index = find_index(table_name, index_name);
        if (!index || position >= index->keys().size()) {
            return {};
        }
        return bindings_.table_column(index->keys()[position].column());
    }

    std::optional<takatori::descriptor::relation> resolve_external_relation(std::uint64_t relation_id) const override {
        if (relation_id != relation_id_ || !output_) {
            return {};
        }
        return bindings_.exchange(*output_);
    }

    std::optional<takatori::descriptor::variable> resolve_external_column(std::uint64_t relation_id,
                                                                          std::uint64_t column_id) const override {
        if (relation_id != relation_id_ || !output_) {
            return {};
        }
        auto position = column_positions_.find(column_id);
        if (position == column_positions_.end()) {
            return {};
        }
        return output_->columns()[position->second];
    }

    std::unique_ptr<scalar::expression> resolve_immediate(remote_scalar_expression const& expression) const override {
        if (!expression.type_payload.empty()) {
            auto type = decode_remote_type(expression.type_payload);
            if (!type) {
                return {};
            }
            auto value = decode_remote_literal(expression.literal_payload, *type);
            if (!value) {
                return {};
            }
            return std::make_unique<scalar::immediate>(std::move(value), std::move(type));
        }
        if (expression.value_kind != "int8" || expression.type != "int8()") {
            return {};
        }
        auto parsed = parse_int8(expression.value);
        if (!parsed) {
            return {};
        }
        return std::make_unique<scalar::immediate>(value::int8{*parsed}, type::int8{});
    }

    std::shared_ptr<type::data const> resolve_type(std::string_view name) const override {
        if (name != "int8()") {
            return {};
        }
        return std::make_shared<type::int8>();
    }

    [[nodiscard]] bool unsupported_column() const noexcept { return unsupported_column_; }

    [[nodiscard]] takatori::plan::forward const& output() const noexcept { return *output_; }

private:
    [[nodiscard]] std::shared_ptr<yugawara::storage::index const> find_index(std::string_view table_name,
                                                                             std::string_view index_name) const {
        auto index = database_->find_index(index_name);
        if (!index || index->table().simple_name() != table_name) {
            return {};
        }
        return index;
    }

    api::database* database_;
    mutable yugawara::binding::factory bindings_{};
    // Development adapter for Yugawara versions without external_relation_info.
    // The surrogate is worker-local and never serialized into Portable IR.
    // Replace this exchange binding with the native descriptor when available.
    std::unique_ptr<takatori::plan::forward> output_{};
    std::uint64_t relation_id_{};
    std::unordered_map<std::uint64_t, std::size_t> column_positions_{};
    mutable bool unsupported_column_{};
};

} // namespace

[[nodiscard]] static std::optional<dq::remote_process_result>
validate_output_types(remote_execution_block const& block) {
    // Reject output kinds before constructing any internal record layout.
    for (auto const& column : block.result_schema) {
        auto t = column.type_payload.empty() && column.type == "int8()" ? std::make_shared<type::int8>()
                                                                        : decode_remote_type(column.type_payload);
        if (!t) {
            return dq::remote_process_result{false, {}, "invalid remote output type", remote_error::invalid_request};
        }
        using k = type::type_kind;
        switch (t->kind()) {
            case k::boolean:
            case k::int4:
            case k::int8:
            case k::float4:
            case k::float8:
            case k::decimal:
            case k::character:
            case k::octet:
            case k::date:
            case k::time_of_day:
            case k::time_point:
                break;
            default:
                return dq::remote_process_result{
                    false, {}, "unsupported remote output type", remote_error::unsupported};
        }
    }
    return std::nullopt;
}

[[nodiscard]] static takatori::relation::step::offer const* output_offer(takatori::relation::graph_type const& graph) {
    for (auto const& expression : graph) {
        if (expression.kind() == takatori::relation::expression_kind::offer) {
            return std::addressof(takatori::util::unsafe_downcast<takatori::relation::step::offer>(expression));
        }
    }
    return nullptr;
}

static dq::remote_process_result
execute_remote_graph(api::impl::database& database, remote_execution_block const& block,
                     takatori::relation::graph_type const& graph, remote_execution_graph_compile_result const& compiled,
                     executor::exchange::forward::step&& forward, std::function<bool()> const& is_cancelled,
                     dq::remote_process_handler::result_sink const& sink,
                     dq::remote_process_handler::record_sink const& records) {
    std::shared_ptr<transaction_context> transaction{};
    api::transaction_option option{transaction_type_kind::rtx};
    option.scan_parallel(1);
    if (auto st = database.create_transaction_internal(transaction, option); st != status::ok) {
        return {false, {}, "failed to create remote RTX", remote_error::unavailable};
    }
    transaction_abort_guard abort_guard{transaction};

    while (!transaction->is_ready()) {
        if (is_cancelled && is_cancelled()) {
            return {false, {}, "remote RTX start was cancelled", remote_error::cancelled};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    transaction->start_time(transaction_context::clock::now());
    transaction->state(transaction_state_kind::active);

    auto cancellation = std::make_shared<remote_cancellation_response>(is_cancelled);
    // Run the remote job on the RPC calling thread. The result sink may block on transport
    // backpressure, and it must not occupy the shared worker pool used by local SQL and by the
    // coordinator side that drains this stream (loopback or mutual routing would wait cyclically).
    // Declared before the request so that it outlives the request and job contexts borrowing it.
    scheduler::serial_task_scheduler rpc_scheduler{};
    auto request = api::impl::create_request_context(
        database, transaction, nullptr, std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        request_info{0, {}, cancellation}, false, nullptr, std::addressof(rpc_scheduler));

    request->remote_worker(true);
    auto* offer = output_offer(graph);
    executor::process::relation_io_map::output_entity_type outputs{};
    outputs.emplace(offer, 0);
    auto relation_map = std::make_shared<executor::process::relation_io_map>(
        executor::process::relation_io_map::input_entity_type{}, std::move(outputs));
    executor::common::graph execution_graph{};
    auto& process_step = execution_graph.emplace<executor::process::step>(
        executor::process::prepare_process(compiled.processor_info, std::move(relation_map)));
    auto& forward_step = execution_graph.emplace<executor::exchange::forward::step>(std::move(forward));
    process_step >> forward_step;

    auto exchange_map = std::make_shared<executor::process::io_exchange_map>();
    exchange_map->add_output(0, std::addressof(forward_step));
    executor::process::bind_process_io(process_step, std::move(exchange_map));

    remote_execution_result remote_result{};
    remote_result.columns = block.result_schema;
    for (auto& column : remote_result.columns) {
        if (column.type_payload.empty()) {
            column.type_payload = encode_remote_type(type::int8{});
        }
    }
    bounded_remote_result result{remote_result, *forward_step.output_meta(), sink, records, is_cancelled};
    process_step.executor_factory(std::make_shared<executor::process::abstract::process_executor_factory>(
        [&process_step, &result](std::shared_ptr<executor::process::abstract::processor> processor,
                                 std::vector<std::shared_ptr<executor::process::abstract::task_context>> contexts) {
            for (auto& context : contexts) {
                auto original = std::dynamic_pointer_cast<executor::process::impl::task_context>(context);
                context = std::make_shared<remote_result_context>(std::move(original), *process_step.io_exchange_map(),
                                                                  result);
            }
            return executor::process::impl::default_process_executor_factory()(std::move(processor),
                                                                               std::move(contexts));
        }));

    auto job = request->job();
    job->callback([] {});
    auto job_id = job->id();
    try {
        rpc_scheduler.schedule_task(scheduler::flat_task{scheduler::task_enum_tag<scheduler::flat_task_kind::bootstrap>,
                                                         request.get(), execution_graph});
        // The serial scheduler runs every task of the job here, including teardown.
        rpc_scheduler.wait_for_progress(job_id);
    } catch (...) {
        // Discard tasks left in this thread's queues before the request and graph they refer to are destroyed.
        // An interrupted task may have left the transaction task count raised, which would defer
        // executor::abort_transaction() forever; the guard aborts the RTX directly after the graph,
        // request and scheduler are destroyed, since nothing else can run the job anymore.
        rpc_scheduler.stop();
        abort_guard.abort_directly();
        return {false, {}, "remote process execution failed with an internal exception", remote_error::internal};
    }

    if (cancellation->check_cancel() || request->status_code() == status::request_canceled) {
        return {false, {}, "remote process execution was cancelled", remote_error::cancelled};
    }

    if (result.overflow()) {
        return {false, {}, "remote result exceeds 64 MiB", remote_error::resource_exhausted};
    }
    if (result.internal_error()) {
        return {false, {}, "remote result encoding or sink failed", remote_error::internal};
    }
    if (result.failed()) {
        return {false, {}, "remote result sink was closed", remote_error::cancelled};
    }
    if (request->status_code() != status::ok) {
        if (auto info = request->error_info()) {
            return {
                false, {}, std::string{info->message()}, remote_error::sql_error, serialize_remote_sql_error(*info)};
        }
        return {false, {}, "failed to execute remote process task", remote_error::internal};
    }

    if (!result.finish()) {
        if (result.internal_error()) {
            return {false, {}, "remote result encoding or sink failed", remote_error::internal};
        }
        return {false, {}, "remote result sink was closed", remote_error::cancelled};
    }
    if (auto st = transaction->commit(); st != status::ok) {
        return {false, {}, "failed to commit remote RTX", remote_error::internal};
    }
    abort_guard.release();

    auto encoded = result.release();
    if (encoded.size() > std::size_t{64} * 1024U * 1024U) {
        return {false, {}, "remote result exceeds 64 MiB", remote_error::resource_exhausted};
    }
    return {true, std::move(encoded), {}};
}

namespace {

#endif

} // namespace

remote_process_handler::remote_process_handler(api::database& database) noexcept
    : database_(std::addressof(database)) {}

dq::remote_process_result remote_process_handler::operator()(std::string_view payload) { return execute(payload, {}); }

dq::remote_process_result remote_process_handler::execute(std::string_view payload,
                                                          cancellation_check const& is_cancelled) {
    return execute_impl(payload, is_cancelled, {});
}

dq::remote_process_result remote_process_handler::execute_records(std::string_view payload,
    cancellation_check const& is_cancelled, record_sink const& sink) {
    if (!sink) return {false, {}, "remote record sink is required", remote_error::invalid_request};
    return execute_impl(payload, is_cancelled, {}, sink);
}

dq::remote_process_result remote_process_handler::execute_stream(std::string_view payload,
                                                                 cancellation_check const& is_cancelled,
                                                                 result_sink const& sink) {
    if (!sink) {
        return {false, {}, "remote result sink is required", remote_error::invalid_request};
    }
    return execute_impl(payload, is_cancelled, sink);
}

dq::remote_process_result remote_process_handler::execute_impl(std::string_view payload,
                                                               cancellation_check const& is_cancelled,
                                                               result_sink const& sink,
                                                               record_sink const& records) {
#ifndef ENABLE_DISTRIBUTED_QUERY_FIXTURES
    (void)payload;
    (void)is_cancelled;
    (void)sink;
    (void)records;
    return {false,
            {},
            "remote execution with the external relation development adapter is disabled",
            remote_error::unsupported};
#else
    if (is_cancelled && is_cancelled()) {
        return {false, {}, "remote execution was cancelled before decoding", remote_error::cancelled};
    }
    auto decoded = decode_remote_execution_block(payload);
    if (!decoded) {
        return {false,
                {},
                std::move(decoded.error),
                decoded.unsupported_type ? remote_error::unsupported : remote_error::invalid_request};
    }

    if (auto error = validate_output_types(*decoded.block)) {
        return std::move(*error);
    }
    catalog_graph_resolver resolver{*database_, *decoded.block};
    auto graph = build_remote_execution_graph(*decoded.block, resolver);
    if (!graph) {
        return {false,
                {},
                std::move(graph.error),
                resolver.unsupported_column() ? remote_error::unsupported : remote_error::not_found};
    }

    auto compiled = compile_remote_execution_graph(*graph.graph);
    if (!compiled) {
        return {false,
                {},
                std::move(compiled.error),
                compiled.unsupported ? remote_error::unsupported : remote_error::invalid_request};
    }

    auto forward = plan::impl::create(resolver.output(), *compiled.compiled_info);
    if (!remote_output_supported(*forward.output_meta())) {
        return {false, {}, "unsupported remote output type", remote_error::unsupported};
    }
    if (decoded.block->result_schema.size() != forward.output_meta()->field_count()) {
        return {false, {}, "remote result schema width mismatch", remote_error::invalid_request};
    }
    for (std::size_t i = 0; i < decoded.block->result_schema.size(); ++i) {
        auto const& column = decoded.block->result_schema[i];
        auto expected = column.type_payload.empty() && column.type == "int8()"
                            ? std::make_shared<type::int8>()
                            : decode_remote_type(column.type_payload);
        if (!expected || utils::type_for(*expected) != forward.output_meta()->at(i)) {
            return {false, {}, "remote result schema type mismatch", remote_error::invalid_request};
        }
    }
    return execute_remote_graph(api::impl::get_impl(*database_), *decoded.block, *graph.graph, compiled,
                                std::move(forward), is_cancelled, sink, records);
#endif
}

} // namespace jogasaki::dist
