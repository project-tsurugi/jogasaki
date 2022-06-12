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
#include <jogasaki/executor/file/loader.h>

#include <xmmintrin.h>
#include <gtest/gtest.h>
#include <jogasaki/test_utils/temporary_folder.h>

#include <jogasaki/executor/file/parquet_writer.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/api_test_base.h>
#include <jogasaki/utils/create_tx.h>

namespace jogasaki::executor::file {

using kind = meta::field_type_kind;
using accessor::text;

using namespace std::chrono_literals;

class loader_test :
    public ::testing::Test,
    public testing::api_test_base {

public:

    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->single_thread(false);
        db_setup(cfg);
        auto* impl = db_impl();
        temporary_.prepare();
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }

    std::string path() {
        return temporary_.path();
    }

    test::temporary_folder temporary_{};  //NOLINT
};

TEST_F(loader_test, simple) {
    boost::filesystem::path p{path()};
    p = p / "simple.parquet";
    auto rec0 = mock::create_nullable_record<kind::int8, kind::float8>(10, 100.0);
    auto rec1 = mock::create_nullable_record<kind::int8, kind::float8>(20, 200.0);
    auto writer = parquet_writer::open(
        std::make_shared<meta::external_record_meta>(
            rec0.record_meta(),
            std::vector<std::optional<std::string>>{"C0", "C1"}
    ), p.string());
    ASSERT_TRUE(writer);

    writer->write(rec0.ref());
    writer->write(rec1.ref());
    writer->close();
    ASSERT_LT(0, boost::filesystem::file_size(p));

    auto* impl = db_impl();

    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0(C0, C1) VALUES (:p0, :p1)", variables, prepared));

    auto ps = api::create_parameter_set();
    ps->set_float8("p1", 1000.0);
    ps->set_reference_column("p0", "C0");
    auto trans = utils::create_transaction(*db_);

    auto* tx = reinterpret_cast<api::impl::transaction*>(trans->get());
    auto request_ctx = tx->create_request_context(
        nullptr, //channel not needed for load
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool())
    );
    loader ldr{
        std::vector<std::string>{p.string()},
        request_ctx.get(),
        prepared,
        std::shared_ptr{std::move(ps)},
        db_.get(),
        tx
    };

    ldr();
    while(ldr.run_count() != 0) {
        _mm_pause();
    }
    std::this_thread::sleep_for(100ms);
    trans->commit();

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(10,1000.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(20,1000.0)), result[1]);
    }

}

}

