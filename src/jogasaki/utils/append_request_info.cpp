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
#include "append_request_info.h"

#include <glog/logging.h>
#include <string>
#include <string_view>
#include <ostream>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/extract_sql_and_tx_id.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/utils/proto_debug_string.h>

namespace jogasaki::utils {

void print_error(request_context& context, std::string_view msg) {
    if (! VLOG_IS_ON(log_error)) {
        return;
    }
    std::stringstream sb{};
    sb << msg;
    utils::append_request_info(sb, context);
    VLOG_LP(log_error) << sb.str();
}

void append_request_info(std::ostream& in, request_context& context) {
    auto const& req = context.req_info().request_source();
    if (! req) {
        return;
    }
    proto::sql::request::Request proto_req{};
    auto s = req->payload();
    if (! proto_req.ParseFromArray(s.data(), static_cast<int>(s.size()))) {
        // request must have been parsed once received, so this normally should not happen
        return;
    }
    in << " request_id:" << context.req_info().id() << ", session_id:" << req->session_id()
       << ", local_id:" << req->local_id() << ", len:" << s.size() << ", msg:" << utils::to_debug_string(proto_req);

    std::shared_ptr<std::string> sql_text{};
    std::shared_ptr<error::error_info> err{};
    std::string tx_id{};
    if(utils::extract_sql_and_tx_id(proto_req, sql_text, tx_id, err, req->session_id())) {
        if (sql_text) {
            in << ", sql_text:\"" << *sql_text << "\"";
        }
        if (! tx_id.empty()) {
            in << ", tx_id:" << tx_id;
        }
    }
    if (err) {
        VLOG_LP(log_debug) << "extracting sql or tx id failed " << *err;
    }
}

} // namespace jogasaki::utils
