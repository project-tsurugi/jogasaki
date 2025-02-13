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
#pragma once

#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/plan/executable_statement.h>

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
/**
 * @brief executable statement implementation
 * @details this object holds plan::executable_statement together with memory resource, that is
 * used for variable length data during compilation.
 */
class executable_statement : public api::executable_statement {
public:
    executable_statement() = default;

    executable_statement(
        std::shared_ptr<plan::executable_statement> body,
        std::shared_ptr<memory::lifo_paged_memory_resource> resource,
        maybe_shared_ptr<api::parameter_set const> parameters
    );

    /**
     * @brief accessor to the wrapped object
     * @return plan::executable_statement holding compiled result and jogasaki artifacts
     */
    [[nodiscard]] std::shared_ptr<plan::executable_statement> const& body() const noexcept;

    /**
     * @brief accessor to the compile-time memory resource
     * @return resource used in the compile-time processing
     */
    [[nodiscard]] std::shared_ptr<memory::lifo_paged_memory_resource> const& resource() const noexcept;

    [[nodiscard]] api::record_meta const* meta() const noexcept override {
        return meta_.get();
    }
private:
    std::shared_ptr<plan::executable_statement> body_{};
    std::shared_ptr<memory::lifo_paged_memory_resource> resource_{};
    std::unique_ptr<impl::record_meta> meta_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{}; // to own parameter set by the end of statement execution
};

/**
 * @brief accessor to the impl of api::executable_statement
 * @return reference to the impl object
 */
inline api::impl::executable_statement& get_impl(api::executable_statement& es) {
    return unsafe_downcast<api::impl::executable_statement>(es);
}

/**
 * @brief accessor to the impl of api::executable_statement
 * @return reference to the const impl object
 */
inline api::impl::executable_statement const& get_impl(api::executable_statement const& es) {
    return unsafe_downcast<api::impl::executable_statement const>(es);
}

} // namespace jogasaki::api::impl
