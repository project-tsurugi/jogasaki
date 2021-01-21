/*
 * Copyright 2018-2019 tsurugi project.
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
#include <iostream>
#include <vector>
#include <random>
#include <numa.h>

#include <glog/logging.h>
#include <boost/thread/latch.hpp>
#include <boost/thread.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/compiler.h>
#include <yugawara/binding/factory.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/plan/process.h>
#include <takatori/statement/execute.h>

#include <performance-tools/synchronizer.h>

#include "params.h"
#include "cli_constants.h"

#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/performance_tools.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>

#include "../common/load.h"

DEFINE_bool(use_multithread, true, "whether using multiple threads");  //NOLINT
DEFINE_int32(partitions, 1, "Number of partitions");  //NOLINT
DEFINE_int32(records_per_partition, 100000, "Number of records per partition");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(sequential_data, false, "use sequential data instead of randomly generated");  //NOLINT
DEFINE_bool(randomize_partition, true, "randomize read partition and avoid read/write happening on the same thread");  //NOLINT
DEFINE_bool(dump, false, "dump mode: generate data, and dump it into files. Must be exclusively used with --load.");  //NOLINT
DEFINE_bool(load, false, "load mode: instead of generating data, load data from files and run. Must be exclusively used with --dump.");  //NOLINT
DEFINE_bool(no_text, false, "use record schema without text type");  //NOLINT
DEFINE_int32(prepare_pages, 600, "prepare specified number of memory pages per partition that are first touched beforehand. Specify -1 to disable.");  //NOLINT
DEFINE_bool(interactive, false, "run on interactive mode. The other options specified on command line is saved as common option.");  //NOLINT
DEFINE_bool(mutex_prepare_pages, false, "use mutex when preparing pages.");  //NOLINT
DEFINE_bool(wait_prepare_pages, true, "wait for all threads completing preparing pages.");  //NOLINT
DEFINE_bool(filter, false, "additionally filter records by a condition");  //NOLINT

namespace jogasaki::scan_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace takatori::util;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::exchange::forward;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::ops;
using namespace jogasaki::scheduler;

using namespace meta;
using namespace takatori::util;

namespace t = ::takatori::type;
namespace v = ::takatori::value;
namespace descriptor = ::takatori::descriptor;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;
namespace binding = ::yugawara::binding;

using ::takatori::util::fail;
using ::takatori::util::downcast;
using ::takatori::util::string_builder;
using namespace ::yugawara;
using namespace ::yugawara::variable;

using kind = meta::field_type_kind;
constexpr std::size_t max_char_len = 100;

constexpr kvs::order asc = kvs::order::ascending;
constexpr kvs::order desc = kvs::order::descending;
constexpr kvs::order undef = kvs::order::undefined;

bool fill_from_flags(
    jogasaki::scan_cli::params& s,
    jogasaki::configuration& cfg,
    std::string const& str = {}
) {
    gflags::FlagSaver saver{};
    if (! str.empty()) {
        if(! gflags::ReadFlagsFromString(str, "", false)) {
            std::cerr << "parsing options failed" << std::endl;
        }
    }
    cfg.single_thread(!FLAGS_use_multithread);

    s.partitions_ = FLAGS_partitions;
    s.records_per_partition_ = FLAGS_records_per_partition;
    s.debug_ = FLAGS_debug;
    s.sequential_data_ = FLAGS_sequential_data;
    s.randomize_partition_ = FLAGS_randomize_partition;
    s.dump_ = FLAGS_dump;
    s.load_ = FLAGS_load;
    s.no_text_ = FLAGS_no_text;
    s.interactive_ = FLAGS_interactive;
    s.prepare_pages_ = FLAGS_prepare_pages;
    s.mutex_prepare_pages_ = FLAGS_mutex_prepare_pages;
    s.wait_prepare_pages_ = FLAGS_wait_prepare_pages;
    s.filter_ = FLAGS_filter;

    if (s.dump_ && s.load_) {
        LOG(ERROR) << "--dump and --load must be exclusively used with each other.";
        return false;
    }

    cfg.core_affinity(FLAGS_core_affinity);
    cfg.initial_core(FLAGS_initial_core);
    cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);

    if (FLAGS_minimum) {
        cfg.single_thread(true);
        cfg.thread_pool_size(1);
        cfg.initial_core(1);
        cfg.core_affinity(false);

        s.partitions_ = 1;
        s.records_per_partition_ = 3;
    }

    if (cfg.assign_numa_nodes_uniformly()) {
        cfg.core_affinity(true);
    }

    std::cout << std::boolalpha <<
        "partitions:" << s.partitions_ <<
        " records_per_partition:" << s.records_per_partition_ <<
        " debug:" << s.debug_ <<
        " sequential:" << s.sequential_data_ <<
        " randomize:" << s.randomize_partition_ <<
        " dump:" << s.dump_ <<
        " load:" << s.load_ <<
        " no_text:" << s.no_text_ <<
        " prepare_pages:" << s.prepare_pages_ <<
        " mutex_prepare_pages:" << s.mutex_prepare_pages_ <<
        " wait_prepare_pages:" << s.wait_prepare_pages_ <<
        " filter:" << s.filter_ <<
        std::endl;
    return true;
}

void dump_perf_info(bool prepare = true, bool run = true, bool completion = false) {
    auto& watch = utils::get_watch();
    if (prepare) {
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_begin, time_point_storage_prepared, "prepare storage");
    }
    if (run) {
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_start_preparing_output_buffer, time_point_output_buffer_prepared, "prepare out buffer");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_output_buffer_prepared, time_point_start_creating_request, "wait preparing all buffers");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_start_creating_request, time_point_request_created, "create request");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_request_created, time_point_schedule, "wait all requests");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_schedule, time_point_schedule_completed, "process request");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_schedule_completed, time_point_result_dumped, "dump result");
    }
    if (completion) {
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_close_db, time_point_release_pool, "close db");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_release_pool, time_point_start_completion, "release memory pool");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_start_completion, time_point_end_completion, "complete and clean-up");
    }
}

class cli {
public:
    // entry point from main
    int operator()(params& param, std::shared_ptr<configuration> const& cfg) {
        map_thread_to_storage_ = init_map(param);
        numa_nodes_ = numa_max_node()+1;
        db_ = kvs::database::open();
        if (param.interactive_) {
            common_options_ = param.original_args_;
            run_interactive(param, cfg);
        } else {
            run(param, cfg);
        }
        utils::get_watch().set_point(time_point_close_db, 0);
        LOG(INFO) << "start closing db";
        (void)db_->close();
        utils::get_watch().set_point(time_point_release_pool, 0);
        LOG(INFO) << "start releasing memory pool";
        (void)global::page_pool(global::pool_operation::reset);
        utils::get_watch().set_point(time_point_start_completion, 0);
        LOG(INFO) << "start completion";
        return 0;
    }

    void run(params& param, std::shared_ptr<configuration> const& cfg) {
        utils::get_watch().set_point(time_point_begin, 0);
        threading_prepare_storage(param, db_.get(), cfg.get(), contexts_);
        utils::get_watch().set_point(time_point_storage_prepared, 0);
        if (param.dump_) return;
        threading_create_and_schedule_request(param, db_, cfg, contexts_);
        dump_perf_info(true, true, false);
    }

    std::string merge_options(std::string const& line = {}) {
        std::string str{common_options_};
        str.append(1, ' ');
        str.append(line);
        std::vector<std::string> options{};
        boost::split(options, str, boost::is_space());
        std::vector<std::string> formatted{};
        for(auto&&o : options) {
            if(o.empty()) continue;
            if(o[0] != '-') {
                if (formatted.empty()) return {};
                auto& prev = formatted.back();
                prev.append(1, '=');
                prev.append(o);
            } else {
                formatted.emplace_back(o);
            }
        }
        std::stringstream ss{};
        for(auto&&s : formatted) {
            ss << s;
            ss << std::endl;
        }
        return ss.str();
    }

    void show_interactive_usage() {
        std::cout <<
            " usage: " << std::endl <<
            " > <command> [<options>]" << std::endl <<
            "  command: " << std::endl <<
            "    h : show this help" << std::endl <<
            "    o : set/show common options" << std::endl <<
            "    p : prepare data" << std::endl <<
            "    r : run" << std::endl <<
            "    q : quit" << std::endl
            ;
    }

    void run_interactive(params& param, std::shared_ptr<configuration> const& cfg) {
        bool to_exit = false;
        while(! to_exit) {
            std::cerr << "> ";
            std::string line{};
            int command = std::cin.get();
            if (command == 0x0a) continue;
            std::getline(std::cin, line);
            switch (command) {
                case 'o': {
                    if (! line.empty()) {
                        common_options_ = std::move(line);
                    }
                    std::cout << common_options_ << std::endl;
                    fill_from_flags(param, *cfg, merge_options());
                    break;
                }
                case 'p': {
                    map_thread_to_storage_ = init_map(param);
                    fill_from_flags(param, *cfg, merge_options(line));
                    utils::get_watch().restart();
                    utils::get_watch().set_point(time_point_begin, 0);
                    threading_prepare_storage(param, db_.get(), cfg.get(), contexts_);
                    utils::get_watch().set_point(time_point_storage_prepared, 0);
                    dump_perf_info(true, false, false);
                    break;
                }
                case 'r': {
                    fill_from_flags(param, *cfg, merge_options(line));
                    utils::get_watch().restart();
                    threading_create_and_schedule_request(param, db_, cfg, contexts_);
                    dump_perf_info(false, true, false);
                    break;
                }
                case 'q': {
                    to_exit = true;
                    break;
                }
                case 'h':
                default: {
                    show_interactive_usage();
                    break;
                }
            }
        }
    }

    void prepare_pages(std::int32_t pages) {
        auto& pool = global::page_pool();
        std::vector<memory::page_pool::page_info> v{};
        v.reserve(pages);
        for(std::size_t i=0, n=pages; i<n; ++i) {
            auto p = pool.acquire_page(! first_touched_);
            std::memset(p.address(), '\1', memory::page_size);
            v.emplace_back(p);
        }
        for(auto&& p : v) {
            pool.release_page(p);
        }
    }

    void threading_prepare_storage(
        params& param,
        kvs::database* db,
        configuration const* cfg,
        std::vector<std::shared_ptr<plan::compiler_context>>& contexts
    ) {
        auto num_threads = param.partitions_;
        contexts.resize(num_threads);
        boost::thread_group thread_group{};
        std::vector<int> result(num_threads);
        for(std::size_t thread_id = 1; thread_id <= num_threads; ++thread_id) {
            auto thread = new boost::thread([&db, &cfg, thread_id, &param, this, &contexts]() {
                set_core_affinity(thread_id, cfg);
                LOG(INFO) << "thread " << thread_id << " storage creation start";
                contexts[thread_id-1] = prepare_storage(param, db, thread_id, cfg);
                LOG(INFO) << "thread " << thread_id << " storage creation end";
            });
            thread_group.add_thread(thread);
        }
        thread_group.join_all();
        LOG(INFO) << "joined all threads for storage creation";
    }

    void set_core_affinity(
        std::size_t thread_id,
        configuration const* cfg
    ) {
        if(cfg->core_affinity()) {
            auto cpu = thread_id+cfg->initial_core();
            if (cfg->assign_numa_nodes_uniformly()) {
                numa_run_on_node(static_cast<int>(cpu % numa_nodes_));
            } else {
                pthread_t x = pthread_self();
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(cpu, &cpuset); //NOLINT
                ::pthread_setaffinity_np(x, sizeof(cpu_set_t), &cpuset);
            }
        }
    }

    std::shared_ptr<plan::compiler_context> prepare_storage(params& param, kvs::database* db, std::size_t storage_id, configuration const* cfg) {
        (void)cfg;
        std::string table_name("T");
        table_name.append(std::to_string(storage_id));
        std::string index_name("I");
        index_name.append(std::to_string(storage_id));
        // generate takatori compile info and statement
        auto compiler_context = std::make_shared<plan::compiler_context>();
        if (param.no_text_) {
            create_compiled_info_no_text(compiler_context, table_name, index_name);
        } else {
            create_compiled_info(compiler_context, table_name, index_name, param);
        }

        compiler_context->storage_provider()->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const&) {
            db->create_storage(id);
        });
        if (param.load_) {
            common_cli::load_storage("db", db, index_name);
            return compiler_context;
        }
        utils::populate_storage_data(db, compiler_context->storage_provider(), index_name, param.records_per_partition_, param.sequential_data_);
        if (param.dump_) {
            common_cli::dump_storage("db", db, index_name);
        }
        return compiler_context;
    }

    bool threading_create_and_schedule_request(
        params& param,
        std::shared_ptr<kvs::database> db,
        std::shared_ptr<configuration> cfg,
        std::vector<std::shared_ptr<plan::compiler_context>>& contexts
    ) {
        auto partitions = param.partitions_;
        std::size_t prepared = 0;
        for(auto&& p : contexts) {
            if (p) prepared++;
        }
        if (prepared < partitions) {
            std::cerr << "Only " << prepared << " of " << partitions << " partitions are prepared" << std::endl;
            return false;
        }
        boost::thread_group thread_group{};
        boost::latch prepare_completion_latch(partitions);
        std::vector<int> result(partitions);
        if (param.wait_prepare_pages_) {
            sync_start_request_.set_threads(partitions);
        }
        for(std::size_t thread_id = 1; thread_id <= partitions; ++thread_id) {
            auto thread = new boost::thread([&db, &cfg, thread_id, &param, this, &contexts, &prepare_completion_latch]() {
                set_core_affinity(thread_id, cfg.get());
                auto storage_id = map_thread_to_storage_[thread_id-1]+1;
                create_and_schedule_request(param, cfg, db, prepare_completion_latch, storage_id, contexts[storage_id-1]);
            });
            thread_group.add_thread(thread);
        }
        if (param.wait_prepare_pages_) {
            sync_start_request_.notify_start();
        }
        first_touched_ = true;
        thread_group.join_all();
        return true;
    }

    void create_and_schedule_request(
        params const& param,
        std::shared_ptr<configuration> const& cfg,
        std::shared_ptr<kvs::database> db,
        boost::latch& prepare_completion_latch,
        std::size_t thread_id,
        std::shared_ptr<plan::compiler_context> const& compiler_context
    ) {
        utils::get_watch().set_point(time_point_start_preparing_output_buffer, thread_id);
        LOG(INFO) << "thread " << thread_id << " start preparing output buffer";
        if (param.prepare_pages_ != -1) {
            if (param.mutex_prepare_pages_) {
                std::lock_guard lock{mutex_on_prepare_pages_};
                prepare_pages(param.prepare_pages_);
            } else {
                prepare_pages(param.prepare_pages_);
            }
        }
        utils::get_watch().set_point(time_point_output_buffer_prepared, thread_id);
        LOG(INFO) << "thread " << thread_id << " output buffer prepared";
        if (param.wait_prepare_pages_) {
            sync_start_request_.wait_start();
        }
        utils::get_watch().set_point(time_point_start_creating_request, thread_id);
        LOG(INFO) << "thread " << thread_id << " create request start";
        // create step graph with only process
        auto& p = unsafe_downcast<takatori::statement::execute>(compiler_context->executable_statement()->statement()).execution_plan();
        auto& p0 = find_process(p);
        auto channel = std::make_shared<class channel>();
        data::result_store result{};
        auto tx = db->create_transaction(true);
        auto context = std::make_shared<request_context>(
            channel,
            cfg,
            std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool()),
            std::move(db),
            std::move(tx),
            &result
        );
        common::graph g{*context};
        g.emplace<process::step>(jogasaki::plan::impl::create(p0, compiler_context->executable_statement()->compiled_info()));

        std::shared_ptr<configuration> thread_cfg = std::make_shared<configuration>(*cfg);
        if (cfg->core_affinity()) {
            if (cfg->assign_numa_nodes_uniformly()) {
                // update cfg for this thread so that newly created thread in dag_controller runs on specified num node
                thread_cfg->force_numa_node(static_cast<int>((thread_id-1+thread_cfg->initial_core()) % numa_nodes_));
            } else {
                thread_cfg->initial_core(static_cast<int>(thread_id-1+cfg->initial_core()));
            }
        }
        dag_controller dc{std::move(thread_cfg)};
        utils::get_watch().set_point(time_point_request_created, thread_id);
        prepare_completion_latch.count_down_and_wait();
        LOG(INFO) << "thread " << thread_id << " schedule request begin";
        utils::get_watch().set_point(time_point_schedule, thread_id);
        dc.schedule(g);
        utils::get_watch().set_point(time_point_schedule_completed, thread_id);
        LOG(INFO) << "thread " << thread_id << " schedule request end";
        dump_result_data(result, param);
        utils::get_watch().set_point(time_point_result_dumped, thread_id);
    }

    void dump_result_data(data::result_store const& result, params const& param) {
        for(std::size_t i=0, n=result.size(); i < n; ++i) {
            LOG(INFO) << "dumping result for partition " << i;
            auto& store = result.store(i);
            auto record_meta = store.meta();
            auto it = store.begin();
            std::size_t count = 0;
            std::size_t hash = 0;
            while(it != store.end()) {
                auto record = it.ref();
                if(param.debug_ && count < 100) {
                    std::stringstream ss{};
                    ss << record << *record_meta;
                    LOG(INFO) << ss.str();
                }
                if (count % 1000 == 0) {
                    std::stringstream ss{};
                    ss << record << *record_meta;
                    // check only 1/1000 records to save time
                    hash ^= std::hash<std::string>{}(ss.str());
                }
                ++it;
                ++count;
            }
            LOG(INFO) << "record count: " << count << " hash: " << std::hex << hash;
        }
    }

    std::vector<std::size_t> init_map(params& param) {
        std::vector<std::size_t> ret{};
        ret.reserve(param.partitions_);
        for(std::size_t i=0; i < param.partitions_; ++i) {
            ret.emplace_back(i);
        }
        if (param.randomize_partition_) {
            std::mt19937_64 mt{};  //NOLINT
            std::shuffle(ret.begin(), ret.end(), mt);
        }
        return ret;
    }
private:
    std::vector<std::size_t> map_thread_to_storage_{};
    std::size_t numa_nodes_{};
    std::shared_ptr<kvs::database> db_{};
    std::vector<std::shared_ptr<plan::compiler_context>> contexts_{};
    std::string common_options_{};
    std::mutex mutex_on_prepare_pages_{};
    performance_tools::Synchronizer sync_start_request_{};
    bool first_touched_{};

    void create_compiled_info(
        std::shared_ptr<plan::compiler_context> const& compiler_context,
        std::string_view table_name,
        std::string_view index_name,
        params const& param
    ) {
        binding::factory bindings;
        std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();

        std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table({
            table_name,
            {
                { "C0", t::int4(), nullity(false) },
                { "C1", t::int8() , nullity(true) },
                { "C2", t::float8() , nullity(true) },
                { "C3", t::float4() , nullity(true) },
                { "C4", t::character(t::varying, max_char_len) , nullity(true) },
            },
        });
        std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index({
            t0,
            index_name,
            {
                t0->columns()[0],
                t0->columns()[1],
            },
            {
                t0->columns()[2],
                t0->columns()[3],
                t0->columns()[4],
            },
            {
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            },
        });

        storage::column const& t0c0 = t0->columns()[0];
        storage::column const& t0c1 = t0->columns()[1];
        storage::column const& t0c2 = t0->columns()[2];
        storage::column const& t0c3 = t0->columns()[3];
        storage::column const& t0c4 = t0->columns()[4];

        auto p = std::make_shared<takatori::plan::graph_type>();
        auto&& p0 = p->insert(takatori::plan::process {});
        auto c0 = bindings.stream_variable("c0");
        auto c1 = bindings.stream_variable("c1");
        auto c2 = bindings.stream_variable("c2");
        auto c3 = bindings.stream_variable("c3");
        auto c4 = bindings.stream_variable("c4");
        auto& r0 = p0.operators().insert(relation::scan {
            bindings(*i0),
            {
                { bindings(t0c0), c0 },
                { bindings(t0c1), c1 },
                { bindings(t0c2), c2 },
                { bindings(t0c3), c3 },
                { bindings(t0c4), c4 },
            },
        });

        object_creator creator{};
        std::shared_ptr<yugawara::analyzer::expression_mapping> expressions = std::make_shared<yugawara::analyzer::expression_mapping>();
        relation::filter* f1{};
        if (param.filter_) {
            using namespace takatori::scalar;
            auto expr = creator.create_unique<scalar::binary>(
                binary_operator::conditional_and,
                scalar::compare {
                    comparison_operator::greater,
                    variable_reference(c1),
                    immediate { value::int8(5), type::int8() }
                },
                scalar::compare {
                    comparison_operator::greater,
                    variable_reference(c2),
                    immediate { value::float8(5.0), type::float8() }
                }
            );
            expressions->bind(*expr, t::boolean {});
            expressions->bind(expr->left(), t::boolean{});
            expressions->bind(expr->right(), t::boolean{});
            auto& l = static_cast<scalar::compare&>(expr->left());
            expressions->bind(l.left(), t::int8 {});
            expressions->bind(l.right(), t::int8 {});
            auto& r = static_cast<scalar::compare&>(expr->right());
            expressions->bind(r.left(), t::float8 {});
            expressions->bind(r.right(), t::float8 {});

            // use emplace to avoid copying expr, whose parts have been registered by bind() above
            f1 = &p0.operators().emplace<relation::filter>(
                std::move(expr)
            );
        }

        auto&& r1 = p0.operators().insert(relation::emit {
            {
                { c0, "c0"},
                { c1, "c1"},
                { c2, "c2"},
                { c3, "c3"},
                { c4, "c4"},
            },
        });

        if (! param.filter_) {
            r0.output() >> r1.input();
        } else {
            r0.output() >> (*f1).input();
            (*f1).output() >> r1.input();
        }

        auto vm = std::make_shared<yugawara::analyzer::variable_mapping>();
        vm->bind(c0, t::int4{});
        vm->bind(c1, t::int8{});
        vm->bind(c2, t::float8{});
        vm->bind(c3, t::float4{});
        vm->bind(c4, t::character{t::varying, max_char_len});
        vm->bind(bindings(t0c0), t::int4{});
        vm->bind(bindings(t0c1), t::int8{});
        vm->bind(bindings(t0c2), t::float8{});
        vm->bind(bindings(t0c3), t::float4{});
        vm->bind(bindings(t0c4), t::character{t::varying, max_char_len});
        yugawara::compiled_info c_info{expressions, vm};

        compiler_context->storage_provider(std::move(storages));
        compiler_context->executable_statement(
            std::make_shared<plan::executable_statement>(
                creator.create_unique<takatori::statement::execute>(std::move(*p)),
                c_info,
                std::shared_ptr<model::statement>{}
            )
        );
    }

    void create_compiled_info_no_text(
        std::shared_ptr<plan::compiler_context> const& compiler_context,
        std::string_view table_name,
        std::string_view index_name
    ) {
        binding::factory bindings;
        std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();

        std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table({
            table_name,
            {
                { "C0", t::int4() , nullity(true) },
                { "C1", t::int8() , nullity(false) },
                { "C2", t::float8(), nullity(false) },
                { "C3", t::float4(), nullity(false)  },
                { "C4", t::int8() , nullity(false) },
                { "C5", t::int8(), nullity(false)  },
                { "C6", t::int8(), nullity(false)  },
                { "C7", t::int8(), nullity(false)  },
                { "C8", t::int8(), nullity(false)  },
                { "C9", t::int8(), nullity(false)  },
                { "C10", t::int8(), nullity(false)  },
                { "C11", t::int8(), nullity(false)  },
                { "C12", t::int8(), nullity(false)  },
                { "C13", t::int8(), nullity(false)  },
            },
        });
        std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index({
            t0,
            index_name,
            {
                t0->columns()[0],
                t0->columns()[1],
            },
            {
                t0->columns()[2],
                t0->columns()[3],
                t0->columns()[4],
                t0->columns()[5],
                t0->columns()[6],
                t0->columns()[7],
                t0->columns()[8],
                t0->columns()[9],
                t0->columns()[10],
                t0->columns()[11],
                t0->columns()[12],
                t0->columns()[13],
            },
            {
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            },
        });

        storage::column const& t0c0 = t0->columns()[0];
        storage::column const& t0c1 = t0->columns()[1];
        storage::column const& t0c2 = t0->columns()[2];
        storage::column const& t0c3 = t0->columns()[3];
        storage::column const& t0c4 = t0->columns()[4];
        storage::column const& t0c5 = t0->columns()[5];
        storage::column const& t0c6 = t0->columns()[6];
        storage::column const& t0c7 = t0->columns()[7];
        storage::column const& t0c8 = t0->columns()[8];
        storage::column const& t0c9 = t0->columns()[9];
        storage::column const& t0c10 = t0->columns()[10];
        storage::column const& t0c11 = t0->columns()[11];
        storage::column const& t0c12 = t0->columns()[12];
        storage::column const& t0c13 = t0->columns()[13];

        auto p = std::make_shared<takatori::plan::graph_type>();
        auto&& p0 = p->insert(takatori::plan::process {});
        auto c0 = bindings.stream_variable("c0");
        auto c1 = bindings.stream_variable("c1");
        auto c2 = bindings.stream_variable("c2");
        auto c3 = bindings.stream_variable("c3");
        auto c4 = bindings.stream_variable("c4");
        auto c5 = bindings.stream_variable("c5");
        auto c6 = bindings.stream_variable("c6");
        auto c7 = bindings.stream_variable("c7");
        auto c8 = bindings.stream_variable("c8");
        auto c9 = bindings.stream_variable("c9");
        auto c10 = bindings.stream_variable("c10");
        auto c11 = bindings.stream_variable("c11");
        auto c12 = bindings.stream_variable("c12");
        auto c13 = bindings.stream_variable("c13");
        auto& r0 = p0.operators().insert(relation::scan {
            bindings(*i0),
            {
                { bindings(t0c0), c0 },
                { bindings(t0c1), c1 },
                { bindings(t0c2), c2 },
                { bindings(t0c3), c3 },
                { bindings(t0c4), c4 },
                { bindings(t0c5), c5 },
                { bindings(t0c6), c6 },
                { bindings(t0c7), c7 },
                { bindings(t0c8), c8 },
                { bindings(t0c9), c9 },
                { bindings(t0c10), c10 },
                { bindings(t0c11), c11 },
                { bindings(t0c12), c12 },
                { bindings(t0c13), c13 },
            },
        });

        auto&& r1 = p0.operators().insert(relation::emit {
            {
                { c0, "c0"},
                { c1, "c1"},
                { c2, "c2"},
                { c3, "c3"},
                { c4, "c4"},
                { c5, "c5"},
                { c6, "c6"},
                { c7, "c7"},
                { c8, "c8"},
                { c9, "c9"},
                { c10, "c10"},
                { c11, "c11"},
                { c12, "c12"},
                { c13, "c13"},
            },
        });

        r0.output() >> r1.input();

        auto vm = std::make_shared<yugawara::analyzer::variable_mapping>();
        vm->bind(c0, t::int4{});
        vm->bind(c1, t::int8{});
        vm->bind(c2, t::float8{});
        vm->bind(c3, t::float4{});
        vm->bind(c4, t::int8{});
        vm->bind(c5, t::int8{});
        vm->bind(c6, t::int8{});
        vm->bind(c7, t::int8{});
        vm->bind(c8, t::int8{});
        vm->bind(c9, t::int8{});
        vm->bind(c10, t::int8{});
        vm->bind(c11, t::int8{});
        vm->bind(c12, t::int8{});
        vm->bind(c13, t::int8{});
        vm->bind(bindings(t0c0), t::int4{});
        vm->bind(bindings(t0c1), t::int8{});
        vm->bind(bindings(t0c2), t::float8{});
        vm->bind(bindings(t0c3), t::float4{});
        vm->bind(bindings(t0c4), t::int8{});
        vm->bind(bindings(t0c5), t::int8{});
        vm->bind(bindings(t0c6), t::int8{});
        vm->bind(bindings(t0c7), t::int8{});
        vm->bind(bindings(t0c8), t::int8{});
        vm->bind(bindings(t0c9), t::int8{});
        vm->bind(bindings(t0c10), t::int8{});
        vm->bind(bindings(t0c11), t::int8{});
        vm->bind(bindings(t0c12), t::int8{});
        vm->bind(bindings(t0c13), t::int8{});
        yugawara::compiled_info c_info{{}, vm};

        compiler_context->storage_provider(std::move(storages));
        object_creator creator{};
        compiler_context->executable_statement(
            std::make_shared<plan::executable_statement>(
                creator.create_unique<takatori::statement::execute>(std::move(*p)),
                c_info,
                std::shared_ptr<model::statement>{}
            )
        );
    }

    takatori::plan::process const& find_process(takatori::plan::graph_type const& p) {
        takatori::plan::process const* p0{};
        takatori::plan::sort_from_upstream(p, [&p0](takatori::plan::step const& s){
            if (s.kind() == takatori::plan::step_kind::process) {
                p0 = &dynamic_cast<takatori::plan::process const&>(s);
            }
        });
        if (! p0) fail();
        return *p0;
    }
}; // class cli

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("scan cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("scan cli");
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    jogasaki::scan_cli::params s{};
    auto cfg = std::make_shared<jogasaki::configuration>();

    if(! fill_from_flags(s, *cfg)) return -1;
    if (s.interactive_) {
        std::stringstream ss{};
        if (argc > 1) {
            for(std::size_t i=1, n=argc; i < n; ++i) {
                std::string arg{argv[i]};  //NOLINT
                if(arg.rfind("interactive") != std::string::npos) continue;
                ss << arg;
                ss << " ";
            }
            s.original_args_ = ss.str();
        }
    }
    try {
        jogasaki::scan_cli::cli{}(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    jogasaki::utils::get_watch().set_point(jogasaki::scan_cli::time_point_end_completion, 0);
    LOG(INFO) << "end completion";
    jogasaki::scan_cli::dump_perf_info(false, false, true);
    return 0;
}
