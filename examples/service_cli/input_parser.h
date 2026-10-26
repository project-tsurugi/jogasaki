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
#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace tateyama::service_cli {

/**
 * @brief item parsed from the input
 */
struct input_item {
    enum class kind {
        /// @brief statement (sql, transaction control, or special statement starting with backslash)
        statement,

        /// @brief `--\sleep <seconds>`
        sleep,

        /// @brief `--\wait [<session>]`
        wait,

        /// @brief `--\expect_error`
        expect_error,

        /// @brief unknown `--\` directive
        unknown_directive,
    };

    kind kind_{};  //NOLINT

    /// @brief the target session name
    std::string session_{};  //NOLINT

    /// @brief statement text (without the delimiter), or directive argument
    std::string text_{};  //NOLINT

    /// @brief seconds for sleep
    double seconds_{};  //NOLINT

    /// @brief the line number (1-origin) where the item begins
    std::size_t line_{};  //NOLINT
};

/**
 * @brief parser for the input compatible with tgsql scripts and tgsqlmux extensions
 * @details the input is fed line by line. This parser
 * - splits statements by `;` (outside quotes and comments) as tgsql does,
 * - treats the line starting with backslash as a special statement terminated by the line end
 *   (except `\prepare` and `\execute` that are terminated by `;`),
 * - handles `NAME:` prefix to switch the target session as tgsqlmux does,
 * - handles directives starting with `--\` (e.g. `--\sleep`), which are comments for tgsql.
 * Incomplete statement text is kept for each session separately.
 */
class input_parser {
public:
    /**
     * @brief create new object
     * @param initial_session the name of the initial target session
     */
    explicit input_parser(std::string initial_session = "T0");

    /**
     * @brief feed one line (without line terminator)
     * @return items completed by this line
     */
    std::vector<input_item> feed(std::string_view line);

    /**
     * @brief notify the end of input
     * @return items completed by the end of input (statements without delimiter)
     */
    std::vector<input_item> finish();

    /**
     * @brief returns the current target session name
     */
    [[nodiscard]] std::string const& current_session() const noexcept {
        return current_;
    }

    /**
     * @brief returns whether the session has incomplete statement
     */
    [[nodiscard]] bool has_partial(std::string const& session) const;

private:
    enum class state {
        normal,
        single_quote,
        double_quote,
        block_comment,
    };

    struct buffer {
        std::string text_{};
        state state_{state::normal};
        std::size_t line_{};
        bool special_{};
    };

    std::string current_{};
    std::map<std::string, buffer> buffers_{};
    std::size_t line_{};

    void scan(std::string const& session, std::string_view line, std::vector<input_item>& out);
    bool parse_directive(std::string const& session, std::string_view line, std::vector<input_item>& out);
    static void emit(std::string const& session, buffer& buf, std::vector<input_item>& out);
};

}  // namespace tateyama::service_cli
