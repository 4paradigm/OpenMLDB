# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# fesql bin
add_library(fesql_flags STATIC ${CMAKE_SOURCE_DIR}/src/flags.cc)
add_executable(fesql fesql.cc)
target_link_libraries(fesql fesql_core fesql_flags fesql_sdk)

# udf exporter
add_executable(export_udf_info export_udf_info.cc)
target_link_libraries(export_udf_info fesql_core)

# engine execution
add_executable(fesql_run_engine fesql_run_engine.cc)
target_link_libraries(fesql_run_engine fesql_core fesql_flags gtest sqlite3)
