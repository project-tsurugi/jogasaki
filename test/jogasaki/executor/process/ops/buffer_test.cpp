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
#include <cstddef>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/relation/buffer.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>

#include <jogasaki/executor/process/impl/ops/buffer.h>
#include <jogasaki/executor/process/impl/ops/buffer_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::process::impl::ops {

using namespace testing;

namespace relation = ::takatori::relation;

namespace {

/**
 * @brief downstream stub that returns a fixed sequence of statuses, then ok.
 */
class fixed_status_operator : public record_operator {
public:
    explicit fixed_status_operator(std::vector<operation_status_kind> statuses) :
        statuses_(std::move(statuses))
    {}

    operation_status process_record(abstract::task_context*) override {
        if (call_count_ < statuses_.size()) {
            return statuses_[call_count_++];
        }
        return operation_status_kind::ok;
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::unknown;
    }

    void finish(abstract::task_context*) override {
        finished_ = true;
    }

    std::size_t call_count_{};  //NOLINT
    bool finished_{};           //NOLINT

private:
    std::vector<operation_status_kind> statuses_{};
};

}  // namespace

class buffer_test : public test_root, public operator_test_utils {
public:
    /**
     * @brief Build the minimal process graph needed for processor_info.
     *
     * Graph: take_flat(0 cols) → buffer(n_outs) → offer0, …, offer_{n_outs-1}.
     * Operators and exchange nodes carry no columns; the test focuses on
     * the fan-out dispatching logic, not variable handling.
     *
     * @param n_outs number of output ports for the buffer node
     * @return reference to the emplaced buffer node
     */
    relation::buffer& build_graph(std::size_t n_outs) {
        auto& up = add_take(0);
        auto& buf = emplace_operator<relation::buffer>(n_outs);
        up.output() >> buf.input();
        for (std::size_t i = 0; i < n_outs; ++i) {
            auto& off = add_offer({});
            buf.output_ports()[i] >> off.input();
        }
        create_processor_info(nullptr, false);
        return buf;
    }

    /**
     * @brief Initialize task_ctx_ with a minimal work_context.
     *
     * operator_count=1 reserves one context slot (index 0) for the buffer.
     * block_count=0 allocates no variable tables (variables are not exercised
     * in these tests).
     */
    void setup_task_ctx() {
        task_ctx_.work_context(std::make_unique<impl::work_context>(
            &request_context_, 1, 0, nullptr, nullptr, nullptr, nullptr, false, false
        ));
    }

    mock::task_context task_ctx_{};  //NOLINT
};

TEST_F(buffer_test, two_downstreams_both_called) {
    auto& buf_node = build_graph(2);
    auto block_idx = processor_info_->block_indices().at(&buf_node);
    setup_task_ctx();

    std::size_t count0{};
    std::size_t count1{};
    std::vector<std::unique_ptr<operator_base>> downs;
    downs.push_back(std::make_unique<verifier>([&count0]() { ++count0; }));
    downs.push_back(std::make_unique<verifier>([&count1]() { ++count1; }));

    buffer op{0, *processor_info_, block_idx, std::move(downs)};
    auto st = op.process_record(&task_ctx_);

    EXPECT_EQ(st.kind(), operation_status_kind::ok);
    EXPECT_EQ(count0, 1U);
    EXPECT_EQ(count1, 1U);
}

TEST_F(buffer_test, three_downstreams_all_called) {
    auto& buf_node = build_graph(3);
    auto block_idx = processor_info_->block_indices().at(&buf_node);
    setup_task_ctx();

    std::size_t count{};
    std::vector<std::unique_ptr<operator_base>> downs;
    downs.push_back(std::make_unique<verifier>([&count]() { ++count; }));
    downs.push_back(std::make_unique<verifier>([&count]() { ++count; }));
    downs.push_back(std::make_unique<verifier>([&count]() { ++count; }));

    buffer op{0, *processor_info_, block_idx, std::move(downs)};
    auto st = op.process_record(&task_ctx_);

    EXPECT_EQ(st.kind(), operation_status_kind::ok);
    EXPECT_EQ(count, 3U);
}

TEST_F(buffer_test, abort_on_second_skips_third) {
    auto& buf_node = build_graph(3);
    auto block_idx = processor_info_->block_indices().at(&buf_node);
    setup_task_ctx();

    std::size_t count0{};
    std::size_t count2{};
    std::vector<std::unique_ptr<operator_base>> downs;
    downs.push_back(std::make_unique<verifier>([&count0]() { ++count0; }));
    downs.push_back(std::make_unique<fixed_status_operator>(
        std::vector<operation_status_kind>{operation_status_kind::aborted}
    ));
    downs.push_back(std::make_unique<verifier>([&count2]() { ++count2; }));

    buffer op{0, *processor_info_, block_idx, std::move(downs)};
    auto st = op.process_record(&task_ctx_);

    EXPECT_EQ(st.kind(), operation_status_kind::aborted);
    EXPECT_EQ(count0, 1U);
    EXPECT_EQ(count2, 0U);
}

TEST_F(buffer_test, yield_from_second_resumes_correctly) {
    auto& buf_node = build_graph(3);
    auto block_idx = processor_info_->block_indices().at(&buf_node);
    setup_task_ctx();

    std::vector<std::unique_ptr<operator_base>> downs;
    downs.reserve(3);
    auto& d0 = downs.emplace_back(std::make_unique<fixed_status_operator>(
        std::vector<operation_status_kind>{operation_status_kind::ok}
    ));
    auto& d1 = downs.emplace_back(std::make_unique<fixed_status_operator>(
        std::vector<operation_status_kind>{operation_status_kind::yield, operation_status_kind::ok}
    ));
    auto& d2 = downs.emplace_back(std::make_unique<fixed_status_operator>(
        std::vector<operation_status_kind>{operation_status_kind::ok}
    ));
    auto* first  = static_cast<fixed_status_operator*>(d0.get());
    auto* second = static_cast<fixed_status_operator*>(d1.get());
    auto* third  = static_cast<fixed_status_operator*>(d2.get());

    buffer op{0, *processor_info_, block_idx, std::move(downs)};

    // first: first→ok, second→yield → buffer yields
    auto st1 = op.process_record(&task_ctx_);
    EXPECT_EQ(st1.kind(), operation_status_kind::yield);
    EXPECT_EQ(first->call_count_,  1U);
    EXPECT_EQ(second->call_count_, 1U);
    EXPECT_EQ(third->call_count_,  0U);

    // resume: starts from second → second→ok, third→ok → buffer ok
    auto st2 = op.process_record(&task_ctx_);
    EXPECT_EQ(st2.kind(), operation_status_kind::ok);
    EXPECT_EQ(first->call_count_,  1U);  // first NOT called again
    EXPECT_EQ(second->call_count_, 2U);
    EXPECT_EQ(third->call_count_,  1U);
}

TEST_F(buffer_test, finish_propagates_to_all_downstreams) {
    auto& buf_node = build_graph(3);
    auto block_idx = processor_info_->block_indices().at(&buf_node);
    setup_task_ctx();

    std::vector<std::unique_ptr<operator_base>> downs;
    downs.reserve(3);
    auto& d0 = downs.emplace_back(std::make_unique<fixed_status_operator>(
        std::vector<operation_status_kind>{}
    ));
    auto& d1 = downs.emplace_back(std::make_unique<fixed_status_operator>(
        std::vector<operation_status_kind>{}
    ));
    auto& d2 = downs.emplace_back(std::make_unique<fixed_status_operator>(
        std::vector<operation_status_kind>{}
    ));
    auto* first  = static_cast<fixed_status_operator*>(d0.get());
    auto* second = static_cast<fixed_status_operator*>(d1.get());
    auto* third  = static_cast<fixed_status_operator*>(d2.get());

    buffer op{0, *processor_info_, block_idx, std::move(downs)};
    op.process_record(&task_ctx_);
    op.finish(&task_ctx_);

    EXPECT_TRUE(first->finished_);
    EXPECT_TRUE(second->finished_);
    EXPECT_TRUE(third->finished_);
}

}  // namespace jogasaki::executor::process::impl::ops
