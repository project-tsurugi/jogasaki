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
#include "statement.h"

#include <cctype>
#include <unordered_map>

#include "text_utils.h"

namespace tateyama::service_cli {

namespace sql = jogasaki::proto::sql;
using jogasaki::common_cli::begin_options;

namespace {

struct token {
    enum class kind {
        word,
        quoted,
        symbol,
    };
    kind kind_{};
    std::string text_{};

    [[nodiscard]] bool is(std::string_view keyword) const {
        return kind_ == kind::word && to_upper(text_) == keyword;
    }
    [[nodiscard]] bool is_symbol(char c) const {
        return kind_ == kind::symbol && text_.size() == 1 && text_[0] == c;
    }
};

// tokenize words, quoted strings and symbols. Quoted strings are unquoted.
std::vector<token> tokenize(std::string_view s) {
    std::vector<token> ret{};
    std::size_t i = 0;
    while(i < s.size()) {
        char c = s[i];
        if (std::isspace(static_cast<unsigned char>(c)) != 0) {
            ++i;
            continue;
        }
        if (c == '\'' || c == '"') {
            std::string text{};
            ++i;
            while(i < s.size() && s[i] != c) {
                if (s[i] == '\\' && i + 1 < s.size()) {
                    ++i;
                }
                text.push_back(s[i]);
                ++i;
            }
            ++i;
            ret.push_back({token::kind::quoted, std::move(text)});
            continue;
        }
        if (std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' || c == '.') {
            std::size_t b = i;
            while(i < s.size() && (std::isalnum(static_cast<unsigned char>(s[i])) != 0 || s[i] == '_' || s[i] == '.')) {
                ++i;
            }
            ret.push_back({token::kind::word, std::string{s.substr(b, i - b)}});
            continue;
        }
        ret.push_back({token::kind::symbol, std::string(1, c)});
        ++i;
    }
    return ret;
}

class cursor {
public:
    explicit cursor(std::vector<token> tokens) : tokens_(std::move(tokens)) {}

    [[nodiscard]] bool eof() const noexcept {
        return pos_ >= tokens_.size();
    }

    // test and consume keywords if all of them match
    bool accept(std::initializer_list<std::string_view> keywords) {
        std::size_t i = pos_;
        for(auto&& k : keywords) {
            if (i >= tokens_.size() || ! tokens_[i].is(k)) {
                return false;
            }
            ++i;
        }
        pos_ = i;
        return true;
    }

    bool accept_symbol(char c) {
        if (! eof() && tokens_[pos_].is_symbol(c)) {
            ++pos_;
            return true;
        }
        return false;
    }

    token const* next() {
        if (eof()) {
            return nullptr;
        }
        return &tokens_[pos_++];
    }

    [[nodiscard]] token const* peek() const {
        if (eof()) {
            return nullptr;
        }
        return &tokens_[pos_];
    }

private:
    std::vector<token> tokens_{};
    std::size_t pos_{};
};

// <table-list> ::= name (, name)* | ( ) | ( name (, name)* )
std::string parse_name_list(cursor& c, std::vector<std::string>& out) {
    bool paren = c.accept_symbol('(');
    if (paren && c.accept_symbol(')')) {
        return {};
    }
    while(true) {
        auto* t = c.next();
        if (t == nullptr || t->kind_ == token::kind::symbol) {
            return "table name is expected";
        }
        out.emplace_back(t->text_);
        if (! c.accept_symbol(',')) {
            break;
        }
    }
    if (paren && ! c.accept_symbol(')')) {
        return "')' is expected";
    }
    return {};
}

// <key-value-pair-list>, the values are ignored
std::string skip_key_value_list(cursor& c) {
    bool paren = c.accept_symbol('(');
    if (paren && c.accept_symbol(')')) {
        return {};
    }
    while(true) {
        if (c.next() == nullptr) {
            return "key is expected";
        }
        if (c.accept_symbol('=')) {
            if (c.next() == nullptr) {
                return "value is expected";
            }
        }
        if (! c.accept_symbol(',')) {
            break;
        }
    }
    if (paren && ! c.accept_symbol(')')) {
        return "')' is expected";
    }
    return {};
}

std::string parse_begin(cursor& c, statement& out) {
    bool is_long = false;
    if (c.accept({"START"})) {
        is_long = c.accept({"LONG"});
        if (! c.accept({"TRANSACTION"})) {
            return "TRANSACTION is expected";
        }
    } else {
        c.accept({"BEGIN"});
        is_long = c.accept({"LONG"});
        if (! c.accept({"TRANSACTION"}) && is_long) {
            return "TRANSACTION is expected";
        }
    }
    enum class read_write { unspecified, read_write, read_only_deferrable, read_only_immediate };
    read_write rw{read_write::unspecified};
    bool include_ddl{};
    auto& opts = out.begin_;
    bool read_area{};
    bool write_preserve{};
    while(! c.eof()) {
        if (c.accept({"READ", "ONLY", "IMMEDIATE"})) {
            rw = read_write::read_only_immediate;
            continue;
        }
        if (c.accept({"READ", "ONLY", "DEFERRABLE"}) || c.accept({"READ", "ONLY"})) {
            rw = read_write::read_only_deferrable;
            continue;
        }
        if (c.accept({"READ", "WRITE"})) {
            rw = read_write::read_write;
            continue;
        }
        if (c.accept({"WRITE", "PRESERVE"})) {
            write_preserve = true;
            if (auto err = parse_name_list(c, opts.write_preserves_); ! err.empty()) {
                return err;
            }
            continue;
        }
        if (c.accept({"INCLUDE", "DEFINITION"}) || c.accept({"INCLUDE", "DEFINITIONS"}) || c.accept({"INCLUDE", "DDL"})) {
            include_ddl = true;
            continue;
        }
        if (c.accept({"READ", "AREA"})) {
            read_area = true;
            bool found{};
            while(true) {
                if (c.accept({"INCLUDE"})) {
                    if (auto err = parse_name_list(c, opts.inclusive_read_areas_); ! err.empty()) {
                        return err;
                    }
                    found = true;
                    continue;
                }
                if (c.accept({"EXCLUDE"})) {
                    if (auto err = parse_name_list(c, opts.exclusive_read_areas_); ! err.empty()) {
                        return err;
                    }
                    found = true;
                    continue;
                }
                break;
            }
            if (! found) {
                return "INCLUDE or EXCLUDE is expected after READ AREA";
            }
            continue;
        }
        if (c.accept({"EXECUTE"})) {
            bool excluding{};
            if (c.accept({"EXCLUDING"})) {
                excluding = true;
            } else if (! c.accept({"PRIOR"})) {
                return "PRIOR or EXCLUDING is expected after EXECUTE";
            }
            bool immediate = c.accept({"IMMEDIATE"});
            if (! immediate) {
                c.accept({"DEFERRABLE"});
            }
            if (excluding) {
                opts.priority_ = immediate ?
                    sql::request::TransactionPriority::INTERRUPT_EXCLUDE : sql::request::TransactionPriority::WAIT_EXCLUDE;
            } else {
                opts.priority_ = immediate ?
                    sql::request::TransactionPriority::INTERRUPT : sql::request::TransactionPriority::WAIT;
            }
            continue;
        }
        if (c.accept({"AS"})) {
            auto* t = c.next();
            if (t == nullptr || t->kind_ == token::kind::symbol) {
                return "label is expected after AS";
            }
            opts.label_ = t->text_;
            continue;
        }
        if (c.accept({"WITH"})) {
            // transaction properties are not supported, just ignored
            if (auto err = skip_key_value_list(c); ! err.empty()) {
                return err;
            }
            continue;
        }
        return "unexpected token in transaction options: " + c.peek()->text_;
    }
    bool ro = rw == read_write::read_only_deferrable || rw == read_write::read_only_immediate;
    if (is_long && rw == read_write::read_only_deferrable) {
        return "transaction type is conflicted between \"LONG\" and \"READ ONLY\"";
    }
    if (write_preserve && ro) {
        return "transaction type is conflicted between \"READ ONLY\" and \"WRITE PRESERVE\"";
    }
    if (include_ddl && ro) {
        return "include ddl is conflicted \"READ ONLY\"";
    }
    bool ltx = is_long || rw == read_write::read_only_immediate || write_preserve || read_area;
    if (ltx) {
        opts.kind_ = begin_options::kind::ltx;
    } else if (rw == read_write::read_only_deferrable) {
        opts.kind_ = begin_options::kind::rtx;
    } else {
        opts.kind_ = begin_options::kind::occ;
    }
    // include ddl is effective only for LTX, same as tgsql
    opts.modifies_definitions_ = include_ddl && ltx;
    return {};
}

std::string parse_commit(cursor& c, statement& out) {
    c.accept({"COMMIT"});
    out.commit_status_ = sql::request::CommitStatus::COMMIT_STATUS_UNSPECIFIED;
    if (c.accept({"WAIT", "FOR"})) {
        if (c.accept({"ACCEPTED"})) {
            out.commit_status_ = sql::request::CommitStatus::ACCEPTED;
        } else if (c.accept({"AVAILABLE"})) {
            out.commit_status_ = sql::request::CommitStatus::AVAILABLE;
        } else if (c.accept({"STORED"})) {
            out.commit_status_ = sql::request::CommitStatus::STORED;
        } else if (c.accept({"PROPAGATED"})) {
            out.commit_status_ = sql::request::CommitStatus::PROPAGATED;
        } else {
            return "commit status (ACCEPTED, AVAILABLE, STORED or PROPAGATED) is expected";
        }
    }
    if (! c.eof()) {
        return "unexpected token in commit: " + c.peek()->text_;
    }
    return {};
}

// split by white spaces respecting single quotes
std::vector<std::string> split_arguments(std::string_view s) {
    std::vector<std::string> ret{};
    std::string cur{};
    bool in_quote{};
    bool has{};
    for(std::size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (in_quote) {
            if (c == '\'' && i + 1 < s.size() && s[i + 1] == '\'') {
                cur.push_back(c);
                ++i;
            } else if (c == '\'') {
                in_quote = false;
                cur.push_back(c);
            } else {
                cur.push_back(c);
            }
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(c)) != 0) {
            if (has) {
                ret.emplace_back(std::move(cur));
                cur.clear();
                has = false;
            }
            continue;
        }
        if (c == '\'') {
            in_quote = true;
        }
        cur.push_back(c);
        has = true;
    }
    if (has) {
        ret.emplace_back(std::move(cur));
    }
    return ret;
}

std::string parse_prepare(std::string_view body, statement& out) {
    // <name> [<var>:<type> ...] AS <sql>
    auto name = first_word(body);
    if (name.empty()) {
        return "statement name is expected: \\prepare <name> [<var>:<type> ...] AS <sql>";
    }
    out.name_ = std::string{name};
    auto rest = trim(body.substr(name.size()));
    while(true) {
        auto w = first_word(rest);
        if (w.empty()) {
            return "AS is expected: \\prepare <name> [<var>:<type> ...] AS <sql>";
        }
        auto after = rest.substr(w.size());
        if (to_upper(w) == "AS" && (after.empty() || std::isspace(static_cast<unsigned char>(after[0])) != 0)) {
            out.sql_ = std::string{trim(after)};
            break;
        }
        if (after.empty() || after[0] != ':') {
            return "variable definition <var>:<type> is expected: " + std::string{w};
        }
        auto type = first_word(after.substr(1));
        auto t = atom_type_of(type);
        if (t == sql::common::AtomType::TYPE_UNSPECIFIED) {
            return "unsupported type for variable " + std::string{w} + ": " + std::string{type};
        }
        out.variables_.emplace_back(std::string{w}, t);
        rest = trim(after.substr(1 + type.size()));
    }
    if (out.sql_.empty()) {
        return "sql is expected after AS";
    }
    return {};
}

std::string parse_execute(std::string_view body, statement& out) {
    auto args = split_arguments(body);
    if (args.empty()) {
        return "statement name is expected: \\execute <name> [<var>=<value> ...]";
    }
    out.name_ = args[0];
    for(std::size_t i = 1; i < args.size(); ++i) {
        auto& a = args[i];
        auto pos = a.find('=');
        if (pos == std::string::npos || pos == 0) {
            return "parameter <var>=<value> is expected: " + a;
        }
        auto var = a.substr(0, pos);
        auto value = a.substr(pos + 1);
        if (to_upper(value) == "NULL") {
            out.parameters_.emplace_back(var, std::nullopt);
            continue;
        }
        if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
            std::string unquoted{};
            for(std::size_t j = 1; j + 1 < value.size(); ++j) {
                unquoted.push_back(value[j]);
                if (value[j] == '\'' && value[j + 1] == '\'') {
                    ++j;
                }
            }
            value = std::move(unquoted);
        }
        out.parameters_.emplace_back(var, std::move(value));
    }
    return {};
}

}  // namespace

sql::common::AtomType atom_type_of(std::string_view type_name) {
    static const std::unordered_map<std::string, sql::common::AtomType> map{
        {"BOOLEAN", sql::common::AtomType::BOOLEAN},
        {"INT", sql::common::AtomType::INT4},
        {"INTEGER", sql::common::AtomType::INT4},
        {"BIGINT", sql::common::AtomType::INT8},
        {"REAL", sql::common::AtomType::FLOAT4},
        {"FLOAT", sql::common::AtomType::FLOAT4},
        {"DOUBLE", sql::common::AtomType::FLOAT8},
        {"DECIMAL", sql::common::AtomType::DECIMAL},
        {"CHAR", sql::common::AtomType::CHARACTER},
        {"VARCHAR", sql::common::AtomType::CHARACTER},
        {"CHARACTER", sql::common::AtomType::CHARACTER},
        {"BINARY", sql::common::AtomType::OCTET},
        {"VARBINARY", sql::common::AtomType::OCTET},
        {"DATE", sql::common::AtomType::DATE},
        {"TIME", sql::common::AtomType::TIME_OF_DAY},
        {"TIMESTAMP", sql::common::AtomType::TIME_POINT},
    };
    if (auto it = map.find(to_upper(type_name)); it != map.end()) {
        return it->second;
    }
    return sql::common::AtomType::TYPE_UNSPECIFIED;
}

std::string parse_statement(std::string_view text, statement& out) {
    out = {};
    out.text_ = std::string{text};
    auto trimmed = trim(text);
    if (starts_with(trimmed, "\\")) {
        auto name = to_lower(first_word(trimmed.substr(1)));
        auto body = trim(trimmed.substr(1 + name.size()));
        out.name_ = name;
        if (name == "prepare") {
            out.kind_ = statement::kind::prepare;
            return parse_prepare(body, out);
        }
        if (name == "execute") {
            out.kind_ = statement::kind::execute;
            return parse_execute(body, out);
        }
        out.kind_ = statement::kind::special;
        out.arguments_ = split_arguments(body);
        return {};
    }
    auto keyword = to_upper(first_word(trimmed));
    if (keyword == "BEGIN" || keyword == "START") {
        out.kind_ = statement::kind::begin;
        cursor c{tokenize(trimmed)};
        return parse_begin(c, out);
    }
    if (keyword == "COMMIT") {
        out.kind_ = statement::kind::commit;
        cursor c{tokenize(trimmed)};
        return parse_commit(c, out);
    }
    if (keyword == "ROLLBACK") {
        out.kind_ = statement::kind::rollback;
        if (! trim(trimmed.substr(keyword.size())).empty()) {
            return "unexpected token after ROLLBACK";
        }
        return {};
    }
    if (keyword == "EXPLAIN") {
        out.kind_ = statement::kind::explain;
        auto rest = trim(trimmed.substr(keyword.size()));
        if (starts_with(rest, "(")) {
            // explain options are not supported, just skipped
            auto pos = rest.find(')');
            if (pos == std::string_view::npos) {
                return "')' is expected for explain options";
            }
            rest = trim(rest.substr(pos + 1));
        }
        out.sql_ = std::string{rest};
        return {};
    }
    if (keyword == "CALL") {
        out.kind_ = statement::kind::call;
        return {};
    }
    out.kind_ = statement::kind::sql;
    return {};
}

}  // namespace tateyama::service_cli
