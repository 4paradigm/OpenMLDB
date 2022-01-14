# Copyright 2022 4Paradigm
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


install(
  FILES ${CMAKE_CURRENT_SOURCE_DIR}/openmldb_exporter.py
  TYPE BIN
  PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)

install(
  FILES ${CMAKE_CURRENT_SOURCE_DIR}/openmldb_exporter.conf
    ${CMAKE_CURRENT_SOURCE_DIR}/prometheus_example.yml
    ${CMAKE_CURRENT_SOURCE_DIR}/openmldb_dashboard.json
  DESTINATION conf
  PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ
)
