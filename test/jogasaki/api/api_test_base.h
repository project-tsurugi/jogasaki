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

#include <gtest/gtest.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/test_base.h>
#include <jogasaki/test_utils/temporary_folder.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;
using takatori::util::fail;

class api_test_base : public test_base {

protected:
    virtual bool to_explain() = 0;

public:

    void db_setup(std::shared_ptr<configuration> cfg = std::make_shared<configuration>());

    void db_teardown();

    api::impl::database* db_impl();

    void explain(api::executable_statement& stmt);

    void execute_query(
        std::string_view query,
        api::parameter_set const& params,
        api::transaction& tx,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        api::transaction& tx,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        api::parameter_set const& params,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        api::transaction& tx,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        std::vector<mock::basic_record>& out
    );

    void execute_statement(
        std::string_view query,
        api::parameter_set const& params,
        api::transaction& tx
    );
    void execute_statement(
        std::string_view query,
        api::parameter_set const& params
    );
    void execute_statement(
        std::string_view query,
        api::transaction& tx
    );

    void execute_statement(
        std::string_view query
    );

    void resolve(std::string& query, std::string_view place_holder, std::string value);

    template <class T>
    void set(api::parameter_set& ps, std::string_view place_holder, api::field_type_kind kind, T value) {
        db_->register_variable(place_holder, kind);
        switch(kind) {
            case api::field_type_kind::int4:
                if constexpr (std::is_convertible_v<T, std::int32_t>) {
                    ps.set_int4(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::int8:
                if constexpr (std::is_convertible_v<T, std::int64_t>) {
                    ps.set_int8(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::float4:
                if constexpr (std::is_convertible_v<T, float>) {
                    ps.set_float4(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::float8:
                if constexpr (std::is_convertible_v<T, double>) {
                    ps.set_float8(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::character:
                if constexpr (std::is_convertible_v<T, std::string_view>) {
                    ps.set_character(place_holder, value);
                } else {
                    fail();
                }
                break;
            default:
                fail();
        }
    }

    [[nodiscard]] std::string path() const;

    test::temporary_folder temporary_{};  //NOLINT
    std::unique_ptr<jogasaki::api::database> db_;  //NOLINT

private:
    void execute_query(
        std::unique_ptr<api::prepared_statement>& prepared,
        api::parameter_set const& params,
        api::transaction& tx,
        std::vector<mock::basic_record>& out
    );

};

}
