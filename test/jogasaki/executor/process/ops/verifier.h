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

#include <functional>
#include <jogasaki/executor/process/impl/ops/operator_base.h>

namespace jogasaki::executor::process::impl::ops {

class verifier : public record_operator {
public:
    verifier() = default;
    void body(std::function<void(void)> f) {
        f_ = std::move(f);
    }
    void process_record(abstract::task_context* context) override {
        f_();
    }
    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::unknown;
    }
    std::function<void(void)> f_{}; //NOLINT
};

class group_verifier : public group_operator {
public:
    group_verifier() = default;
    void body(std::function<void(bool first)> f) {
        f_ = std::move(f);
    }
    void process_group(abstract::task_context* context, bool first) override {
        f_(first);
    }
    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::unknown;
    }
    std::function<void(bool first)> f_{}; //NOLINT
};

template<class Iterator>
class cogroup_verifier : public cogroup_operator<Iterator> {
public:
    cogroup_verifier() = default;
    void body(std::function<void(cogroup<Iterator>& c)> f) {
        f_ = std::move(f);
    }
    void process_cogroup(abstract::task_context* context, cogroup<Iterator>& c) override {
        f_(c);
    }
    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::unknown;
    }
    std::function<void(cogroup<Iterator>& c)> f_{}; //NOLINT
};

}