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
#include <glog/logging.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

#include <shakujo/parser/Parser.h>

#include <yugawara/storage/configurable_provider.h>

#include <mizugaki/translator/shakujo_translator.h>

#include <takatori/type/int.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

using namespace ::mizugaki::translator;

using code = shakujo_translator_code;
using result_kind = shakujo_translator::result_type::kind_type;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

namespace tinfo = ::shakujo::common::core::type;

class compiler_test : public ::testing::Test {};

using kind = field_type_kind;

std::unique_ptr<shakujo::model::program::Program> gen_shakujo_program(std::string_view sql) {
    shakujo::parser::Parser parser;
    std::unique_ptr<shakujo::model::program::Program> program;
    try {
        std::stringstream ss{std::string(sql)};
        program = parser.parse_program("compiler_test", ss);
    } catch (shakujo::parser::Parser::Exception &e) {
        LOG(ERROR) << "parse error:" << e.message() << " (" << e.region() << ")" << std::endl;
    }
    return program;
};

TEST_F(compiler_test, simple) {
    std::string sql = "select * from T1";
    auto p = gen_shakujo_program(sql);

    std::shared_ptr<::yugawara::storage::configurable_provider> storages
            = std::make_shared<::yugawara::storage::configurable_provider>();

    std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table("T0", {
            "T0",
            {
                    { "C0", type::int4() },
                    { "C1", type::int4() },
                    { "C2", type::int4() },
            },
    });
    std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index("I0", {
            t0,
            "I0",
            {
                    t0->columns()[0],
            },
            {},
            {
                    ::yugawara::storage::index_feature::find,
                    ::yugawara::storage::index_feature::scan,
                    ::yugawara::storage::index_feature::unique,
                    ::yugawara::storage::index_feature::primary,
            },
    });

    shakujo_translator translator;
    shakujo_translator_options options {
            storages,
            {},
            {},
            {},
    };
    placeholder_map placeholders;
    ::takatori::document::document_map documents;
    ::shakujo::model::IRFactory ir;
    ::yugawara::binding::factory bindings { options.get_object_creator() };





}

}

