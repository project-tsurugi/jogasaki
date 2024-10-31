/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <jogasaki/executor/process/abstract/range.h>
#include <jogasaki/executor/process/impl/bound.h>

namespace jogasaki::executor::process::impl {

class range : public abstract::range {
  public:
    explicit range(std::unique_ptr<bound> begin = nullptr, std::unique_ptr<bound> end = nullptr,
        bool is_empty = true) noexcept;

    ~range() override                        = default;
    range(range const& other)                = delete;
    range& operator=(range const& other)     = delete;
    range(range&& other) noexcept            = default;
    range& operator=(range&& other) noexcept = default;
    [[nodiscard]] bound const* begin() const noexcept;
    [[nodiscard]] bound const* end() const noexcept;
    [[nodiscard]] bool is_empty() const noexcept;
    /**
     * @brief Support for debugging, callable in GDB
     * @param out The output stream to which the buffer's internal state will be written.
     * @param indent The indentation level for formatting the output, default is 0.
     * @param status The result of the bound.
     */
    void dump(std::ostream& out, int indent = 0) const noexcept;

  private:
    std::unique_ptr<bound> begin_;
    std::unique_ptr<bound> end_;
    bool is_empty_;
};

} // namespace jogasaki::executor::process::impl