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

#include <unordered_map>

#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief process input information corresponding to input port
 * @details the input can be either record-based or group-based, which the upstream exchange kind defines.
 */
class process_input {
public:
    process_input(
        maybe_shared_ptr<meta::record_meta> meta,
        meta::variable_order column_order
    ) :
        record_meta_(std::move(meta)),
        column_order_(std::move(column_order))
    {}
    process_input(
        maybe_shared_ptr<meta::group_meta> meta,
        meta::variable_order column_order
    ) :
        group_meta_(std::move(meta)),
        column_order_(std::move(column_order)),
        for_group_(true)
    {}
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept {
        return record_meta_;
    }
    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& group_meta() const noexcept {
        return group_meta_;
    }
    [[nodiscard]] meta::variable_order const& column_order() const noexcept {
        return column_order_;
    }
    [[nodiscard]] bool is_group_input() const noexcept {
        return for_group_;
    }
private:
    maybe_shared_ptr<meta::record_meta> record_meta_{};
    maybe_shared_ptr<meta::group_meta> group_meta_{};
    meta::variable_order column_order_{};
    bool for_group_{false};
};

/**
 * @brief process output information corresponding to output port
 * @details the output is always record based
 */
class process_output {
public:
    process_output(
        maybe_shared_ptr<meta::record_meta> meta,
        meta::variable_order column_order
    ) :
        meta_(std::move(meta)),
        column_order_(std::move(column_order))
    {}
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }
    [[nodiscard]] meta::variable_order const& column_order() const noexcept {
        return column_order_;
    }
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    meta::variable_order column_order_{};
};

/**
 * @brief process external output information corresponding to emit or write operator
 * @details the output is always record based
 */
class process_external_output {
    maybe_shared_ptr<meta::record_meta> meta_{};
    meta::variable_order column_order_{};
public:
    process_external_output(
        maybe_shared_ptr<meta::record_meta> meta,
        meta::variable_order column_order
    ) :
        meta_(std::move(meta)),
        column_order_(std::move(column_order))
    {}
    [[nodiscard]] meta::record_meta const& meta() const noexcept {
        return *meta_;
    }
    [[nodiscard]] meta::variable_order const& column_order() const noexcept {
        return column_order_;
    }
};

class process_io {
public:
    using input_entity_type = std::vector<process_input>;
    using output_entity_type = std::vector<process_output>;
    using external_output_entity_type = std::vector<process_external_output>;

    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create new empty instance
     */
    process_io() = default;

    /**
     * @brief create new instance
     */
    process_io(
        input_entity_type inputs,
        output_entity_type outputs,
        external_output_entity_type external_outputs
    ) :
        inputs_(std::move(inputs)),
        outputs_(std::move(outputs)),
        external_outputs_(std::move(external_outputs))
    {}

    [[nodiscard]] process_input const& input_at(std::size_t index) const {
        return inputs_.at(index);
    }

    [[nodiscard]] process_output const& output_at(std::size_t index) const {
        return outputs_.at(index);
    }

    [[nodiscard]] process_external_output const& external_output_at(std::size_t index) const {
        return external_outputs_.at(index);
    }

    [[nodiscard]] std::size_t input_count() const noexcept {
        return inputs_.size();
    }

    [[nodiscard]] std::size_t output_count() const noexcept {
        return outputs_.size();
    }

    [[nodiscard]] std::size_t external_output_count() const noexcept {
        return external_outputs_.size();
    }

private:
    input_entity_type inputs_{};
    output_entity_type outputs_{};
    external_output_entity_type external_outputs_{};
};

}


