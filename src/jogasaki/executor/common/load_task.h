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
#include <ostream>
#include <string_view>

#include <jogasaki/model/task.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/utils/interference_size.h>
#include "step.h"

namespace jogasaki::executor::common {

class load_task : public model::task, private std::enable_shared_from_this<load_task> {
public:
    load_task() = default;

    explicit load_task(
        request_context* context,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::database* db,
        api::impl::transaction* tx
    ) :
        id_(++id_src),
        context_(context),
        prepared_(prepared),
        parameters_(std::move(parameters)),
        db_(db),
        tx_(tx)
    {}

    [[nodiscard]] identity_type id() const override {
        return id_;
    }

    [[nodiscard]] request_context* context() const {
        return context_;
    }

    [[nodiscard]] bool has_transactional_io() override {
        return true;
    }

    [[nodiscard]] model::task_result operator()() override {
        // read records from file
        return model::task_result::complete;
    }
protected:
    std::ostream& write_to(std::ostream& out) const override {
        using namespace std::string_view_literals;
        return out << "task[id="sv << std::to_string(static_cast<identity_type>(id_)) << "]"sv;
    }

private:
    cache_align static inline std::atomic_size_t id_src = 10000;
    identity_type id_{};
    request_context* context_{};

    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};

    api::database* db_{};
    api::impl::transaction* tx_{};
    std::size_t count_{0};
};

}



