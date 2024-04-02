/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <ostream>
#include <string_view>
#include <type_traits>

#include <takatori/util/enum_set.h>

#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/executor/diagnostic_record.h>

#include "error.h"

namespace jogasaki::executor::process::impl::expression {

/**
 * @brief cast loss policy
 */
enum class loss_precision_policy : std::size_t {
    ///@brief ignore the loss of precision
    ignore,

    ///@brief round down the value
    floor,

    ///@brief round up the value
    ceil,

    ///@brief fill null value when precision is lost
    unknown,

    ///@brief warn and continue when precision is lost
    warn,

    ///@brief raise error when precision is lost
    error,

    ///@brief implicit cast policy (almost always same as error)
    implicit,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(loss_precision_policy value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case loss_precision_policy::ignore: return "ignore"sv;
        case loss_precision_policy::floor: return "floor"sv;
        case loss_precision_policy::ceil: return "ceil"sv;
        case loss_precision_policy::unknown: return "unknown"sv;
        case loss_precision_policy::warn: return "warn"sv;
        case loss_precision_policy::error: return "error"sv;
        case loss_precision_policy::implicit: return "implicit"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, loss_precision_policy value) {
    return out << to_string_view(value);
}

/**
 * @brief range error policy
 */
enum class range_error_policy : std::size_t {
    ignore,
    wrap,
    warning,
    error,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(range_error_policy value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case range_error_policy::ignore: return "ignore"sv;
        case range_error_policy::wrap: return "wrap"sv;
        case range_error_policy::warning: return "warning"sv;
        case range_error_policy::error: return "error"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, range_error_policy value) {
    return out << to_string_view(value);
}

/**
 * @brief class representing evaluation error
 */
class evaluator_context {
public:
    using error_type = diagnostic_record<error_kind>;

    using memory_resource = memory::paged_memory_resource;

    /**
     * @brief create new object
     */
    explicit evaluator_context(
        memory_resource* resource
    ) :
        resource_(resource)
    {}

    /**
     * @brief accessor for cast loss policy
     */
    [[nodiscard]] loss_precision_policy get_loss_precision_policy() const noexcept {
        return loss_precision_policy_;
    }

    /**
     * @brief setter for cast loss policy
     */
    evaluator_context& set_loss_precision_policy(loss_precision_policy arg) noexcept {
        loss_precision_policy_ = arg;
        return *this;
    }

    /**
     * @brief accessor for range error policy
     */
    [[nodiscard]] range_error_policy get_range_error_policy() const noexcept {
        return range_error_policy_;
    }

    /**
     * @brief setter for range error policy
     */
    evaluator_context& set_range_error_policy(range_error_policy arg) noexcept {
        range_error_policy_ = arg;
        return *this;
    }

    /**
     * @brief create new error record and returns the reference, which is available until next call of this method
     */
    error_type& add_error(error_type arg) {
        return errors_.emplace_back(std::move(arg));
    }

    /**
     * @brief accessor for errors
     */
    [[nodiscard]] std::vector<error_type> const& errors() const noexcept {
        return errors_;
    }

    /**
     * @brief accessor for memory resource
     */
    [[nodiscard]] memory_resource* resource() const noexcept {
        return resource_;
    }

    /**
     * @brief accessor whether the precision is lost
     */
    [[nodiscard]] bool lost_precision() const noexcept {
        return lost_precision_;
    }

    /**
     * @brief set whether the precision is lost
     */
    void lost_precision(bool arg) noexcept {
        lost_precision_ = arg;
    }

private:
    memory_resource* resource_{};
    loss_precision_policy loss_precision_policy_{loss_precision_policy::ignore};
    range_error_policy range_error_policy_{range_error_policy::ignore};
    std::vector<error_type> errors_{};
    bool lost_precision_{};
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
inline std::ostream& operator<<(std::ostream& out, evaluator_context const& value) {
    out <<
        "evaluator_context(" << value.get_loss_precision_policy() <<
        ", " << value.get_range_error_policy();
    for(auto&& e : value.errors()) {
        out << ", " << e;
    }
    out << ")";
    return out;
}

std::pair<std::string, std::string> create_conversion_error_message(evaluator_context const& ctx);

}  // namespace jogasaki::executor::process::impl::expression
