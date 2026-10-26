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
#include "input_parser.h"

#include <cctype>
#include <cstdlib>
#include <utility>

#include "text_utils.h"

namespace tateyama::service_cli {

input_parser::input_parser(std::string initial_session) :
    current_(std::move(initial_session))
{}

bool input_parser::has_partial(std::string const& session) const {
    auto it = buffers_.find(session);
    if (it == buffers_.end()) {
        return false;
    }
    return ! trim(it->second.text_).empty() || it->second.state_ != state::normal;
}

namespace {

bool is_session_name(std::string_view s) {
    if (s.empty()) {
        return false;
    }
    for(auto c : s) {
        if (std::isalnum(static_cast<unsigned char>(c)) == 0) {
            return false;
        }
    }
    return true;
}

}  // namespace

std::vector<input_item> input_parser::feed(std::string_view line) {
    ++line_;
    std::vector<input_item> ret{};
    if (line_ == 1 && starts_with(line, "#!")) {
        return ret;
    }
    auto trimmed = trim(line);

    // tgsqlmux session prefix `NAME:` or `NAME: <statement>`
    auto session = current_;
    if (auto pos = trimmed.find(':'); pos != std::string_view::npos && is_session_name(trimmed.substr(0, pos))) {
        auto rest = trimmed.substr(pos + 1);
        if (trim(rest).empty()) {
            current_ = std::string{trimmed.substr(0, pos)};
            return ret;
        }
        if (rest[0] == ' ' || rest[0] == '\t') {
            session = std::string{trimmed.substr(0, pos)};
            line = rest;
            trimmed = trim(rest);
        }
    }
    auto& buf = buffers_[session];
    bool empty = trim(buf.text_).empty() && buf.state_ == state::normal;

    // directive `--\xxx` - this is a comment for tgsql
    if (starts_with(trimmed, "--\\")) {
        if (parse_directive(session, trimmed.substr(3), ret)) {
            return ret;
        }
    }

    // special statement starting with backslash
    if (empty && starts_with(trimmed, "\\")) {
        auto name = to_lower(first_word(trimmed.substr(1)));
        if (name != "prepare" && name != "execute") {
            buf.text_ = std::string{trimmed};
            if (! buf.text_.empty() && buf.text_.back() == ';') {
                buf.text_.pop_back();
            }
            buf.line_ = line_;
            emit(session, buf, ret);
            return ret;
        }
    }
    scan(session, line, ret);
    return ret;
}

bool input_parser::parse_directive(std::string const& session, std::string_view line, std::vector<input_item>& out) {
    auto name = to_lower(first_word(line));
    auto arg = trim(line.substr(name.size()));
    input_item item{};
    item.session_ = session;
    item.line_ = line_;
    item.text_ = std::string{arg};
    if (name == "sleep") {
        item.kind_ = input_item::kind::sleep;
        item.seconds_ = std::strtod(item.text_.c_str(), nullptr);
    } else if (name == "wait") {
        item.kind_ = input_item::kind::wait;
    } else if (name == "expect_error") {
        item.kind_ = input_item::kind::expect_error;
    } else {
        item.kind_ = input_item::kind::unknown_directive;
        item.text_ = std::string{line};
    }
    out.emplace_back(std::move(item));
    return true;
}

void input_parser::emit(std::string const& session, buffer& buf, std::vector<input_item>& out) {
    auto text = trim(buf.text_);
    if (! text.empty()) {
        input_item item{};
        item.kind_ = input_item::kind::statement;
        item.session_ = session;
        item.text_ = std::string{text};
        item.line_ = buf.line_;
        out.emplace_back(std::move(item));
    }
    buf.text_.clear();
    buf.state_ = state::normal;
    buf.line_ = 0;
}

void input_parser::scan(std::string const& session, std::string_view line, std::vector<input_item>& out) {
    auto& buf = buffers_[session];
    std::size_t i = 0;
    auto n = line.size();
    auto append = [&](char c) {
        if (buf.line_ == 0 && std::isspace(static_cast<unsigned char>(c)) == 0) {
            buf.line_ = line_;
        }
        buf.text_.push_back(c);
    };
    while(i < n) {
        char c = line[i];
        char next = i + 1 < n ? line[i + 1] : '\0';
        switch(buf.state_) {
            case state::normal:
                if (c == '\'') {
                    buf.state_ = state::single_quote;
                    append(c);
                } else if (c == '"') {
                    buf.state_ = state::double_quote;
                    append(c);
                } else if ((c == '-' && next == '-') || (c == '/' && next == '/')) {
                    // comment till the end of line
                    i = n;
                    continue;
                } else if (c == '/' && next == '*') {
                    buf.state_ = state::block_comment;
                    append(' ');
                    ++i;
                } else if (c == ';') {
                    emit(session, buf, out);
                } else {
                    append(c);
                }
                break;
            case state::single_quote:
            case state::double_quote: {
                char quote = buf.state_ == state::single_quote ? '\'' : '"';
                append(c);
                if (c == '\\' && i + 1 < n) {
                    append(next);
                    ++i;
                } else if (c == quote) {
                    buf.state_ = state::normal;
                }
                break;
            }
            case state::block_comment:
                if (c == '*' && next == '/') {
                    buf.state_ = state::normal;
                    ++i;
                }
                break;
        }
        ++i;
    }
    if (! buf.text_.empty()) {
        buf.text_.push_back('\n');
    }
}

std::vector<input_item> input_parser::finish() {
    std::vector<input_item> ret{};
    for(auto&& [session, buf] : buffers_) {
        emit(session, buf, ret);
    }
    return ret;
}

}  // namespace tateyama::service_cli
