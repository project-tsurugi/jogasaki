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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/type/int.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;

class validate_user_scenario5_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(validate_user_scenario5_test, query_with_many_output_columns) {
    // issue #206 crash on query that has >64 output columns
    execute_statement(
        "CREATE TABLE target ("
        "target_id int PRIMARY KEY NOT NULL,"
        "tenant_id int NOT NULL,"
        "target_name varchar(40) NOT NULL,"
        "description varchar(256),"
        "tiff_dir varchar(80),"
        "hdfs_archive_dir varchar(80),"
        "work_dir varchar(80),"
        "app_home_dir varchar(80),"
        "tmp_dir varchar(80),"
        "exiftool varchar(80),"
        "hadoop varchar(80),"
        "parallelism int,"
        "windows_mode int,"
        "worker_nodes varchar(512),"
        "worker_parallelism varchar(512),"
        "camera_data_file varchar(80),"
        "pos_data_file varchar(80),"
        "start_date_time char(19),"
        "end_date_time char(19)"
        ")"
    );

    execute_statement("INSERT INTO target (target_id, tenant_id, target_name, description, tiff_dir, hdfs_archive_dir, work_dir, app_home_dir, tmp_dir, exiftool, hadoop, parallelism, windows_mode, worker_nodes, worker_parallelism, camera_data_file, pos_data_file, start_date_time, end_date_time) VALUES (1,1,'test','','/home/suzuka/data/images/2018','','/work','$HOME/psc','$HOME/tmp','/usr/bin/exiftool','',4,0,'dbs41',NULL,'/home/suzuka/psc/conf/sensor_width_camera_database_PASCO.txt',' /home/suzuka/data/csv/CO_nagaoka20181112.csv','2021-12-13 13:43:00','2022-01-06 13:33:03')");

    execute_statement(
        "CREATE TABLE job ("
        "job_id int PRIMARY KEY NOT NULL,"
        "target_id int NOT NULL,"
        "job_name varchar(40) NOT NULL,"
        "description varchar(256),"
        "clean_tmp_dir int,"
        "enable_general_task int,"
        "max_retries int,"
        "speculative_execution int,"
        "timeout_duration int,"
        "timeout_killafter_duration int,"
        "ply_thinout_angle_range int,"
        "envs_imagelisting varchar(80),"
        "params_imagelisting varchar(80),"
        "envs_computefeatures varchar(80),"
        "params_computefeatures varchar(80),"
        "envs_computematches varchar(80),"
        "params_computematches varchar(80),"
        "envs_geometricfilter varchar(80),"
        "params_geometricfilter varchar(80),"
        "envs_incrementalsfm varchar(80),"
        "params_incrementalsfm varchar(80),"
        "envs_openmvg2openmvs varchar(80),"
        "params_openmvg2openmvs varchar(80),"
        "envs_densifypointcloud varchar(80),"
        "params_densifypointcloud varchar(80),"
        "envs_densifypointcloud2 varchar(80),"
        "params_densifypointcloud2 varchar(80),"
        "envs_refinemesh varchar(80),"
        "params_refinemesh varchar(80),"
        "envs_reconstructmesh varchar(80),"
        "params_reconstructmesh varchar(80),"
        "envs_texturemesh varchar(80),"
        "params_texturemesh varchar(80),"
        "split_definition_file varchar(80),"
        "zukaku_shape_file varchar(80),"
        "image_shape_file varchar(80),"
        "intersect_percent int,"
        "buffer_percent int,"
        "nadir_count int,"
        "right_count int,"
        "left_count int,"
        "forward_count int,"
        "backward_count int,"
        "save_level int,"
        "clean_regist_3d_data int,"
        "start_date_time char(19),"
        "end_date_time char(19)"
        ")"
    );

    execute_statement("INSERT INTO job (job_id, target_id, job_name, description, clean_tmp_dir, enable_general_task, max_retries, speculative_execution, timeout_duration, timeout_killafter_duration, ply_thinout_angle_range, envs_imagelisting, params_imagelisting, envs_computefeatures, params_computefeatures, envs_computematches, params_computematches, envs_geometricfilter, params_geometricfilter, envs_incrementalsfm, params_incrementalsfm, envs_openmvg2openmvs, params_openmvg2openmvs, envs_densifypointcloud, params_densifypointcloud, envs_densifypointcloud2, params_densifypointcloud2, envs_refinemesh, params_refinemesh, envs_reconstructmesh, params_reconstructmesh, envs_texturemesh, params_texturemesh, split_definition_file, zukaku_shape_file, image_shape_file, intersect_percent, buffer_percent, nadir_count, right_count, left_count, forward_count, backward_count, save_level, clean_regist_3d_data, start_date_time, end_date_time) VALUES (1, 1, 'testJob', NULL, 0, 0, 2, 1, 9000, 60, 20, NULL, '-P -c 1', 'OMP_NUM_THREADS=8', '-m SIFT -p HIGH -u 0', 'OMP_NUM_THREADS=8', '-r 0.6', NULL, '-g e', 'OMP_NUM_THREADS=8', '--sfm_engine GLOBAL -P -f NONE', 'OMP_NUM_THREADS=8', '', '', '--fusion-mode -1', '', '--fusion-mode -2', '', '--decimate=1', '', '--quality-factor=2 --min-point-distance=1.5 --decimate=0.3', '', '--cost-smoothness-ratio=1 --patch-packing-heuristic=0 --export-type ply', '/home/suzuka/psc/conf/DividedDefinition.csv.gz', '/home/suzuka/data/shp/zkk25_9.shp', '/home/suzuka/data/shp/nagaoka_2018_PhotoArea_wgs84_jpg.shp', 30, 0, 2, 2, 2, 2, 2, 0, 0, '2021-12-13 13:43:00','2022-01-06 13:33:03')");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM target INNER JOIN job ON target.target_id = job.target_id", result);
    ASSERT_EQ(1, result.size());
}

}
