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
#pragma once

#include <atomic>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/transaction.h>

namespace jogasaki {
class request_context;

namespace scheduler {

using takatori::util::maybe_shared_ptr;

/**
 * @brief context object for the job
 * @details this class represents context information in the scope of the job scheduling
 */
class cache_align load_context {
public:
    constexpr static std::size_t undefined_index = static_cast<std::size_t>(-1);

    using job_completion_callback = std::function<void(void)>;

    /**
     * @brief create default context object
     */
    load_context() = default;

    ~load_context() = default;
    load_context(load_context const& other) = delete;
    load_context& operator=(load_context const& other) = delete;
    load_context(load_context&& other) noexcept = delete;
    load_context& operator=(load_context&& other) noexcept = delete;

    load_context(
        request_context* rctx,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::database* db,
        api::impl::transaction* tx
    ) noexcept :
        rctx_(rctx),
        prepared_(prepared),
        parameters_(std::move(parameters)),
        db_(db),
        tx_(tx)
    {}

    constexpr static std::size_t bulk_size = 100;

    bool operator()() {
        if (running_statements_ < bulk_size) {
            // read records, assign host variables, submit tasks
            std::unique_ptr<api::executable_statement> statement{};
            db_->resolve(prepared_, parameters_, statement);
            auto shared = shared_from_this();
            ++count_;
            tx_->execute_async(std::shared_ptr{std::move(statement)},
                [&, shared](status st, std::string_view msg){
                    (void)shared;
                    (void)st;
                    (void)msg;
                    if(count_ < 10) {
                        auto& ts = *context_->scheduler();
//                    ts.register_job(context_->job());
                        ts.schedule_task(scheduler::flat_task{
                            scheduler::task_enum_tag<scheduler::flat_task_kind::wrapped>,
                            context_,
                            std::make_shared<executor::common::load_task>(
                                context_,
                                prepared_,
                                parameters_,
                                db_,
                                tx_
                            )
                        });
                    }
                }
            );
        }
//        if (remaing record)
    }

private:
    request_context* rctx_{};
    std::atomic_size_t running_statements_{};

    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};

    api::database* db_{};
    api::impl::transaction* tx_{};
    std::size_t count_{0};
};

}

}

