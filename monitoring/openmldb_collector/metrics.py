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
"""
metric definatitons of OpenMLDB
"""

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.metrics import Enum
from prometheus_client.utils import INF

NAMESPACE = "openmldb"

# labels
ENDPOINT = "endpoint"
ROLE = "role"
TABLE_ID = "tid"
TABLE_PATH = "table_path"
DEPLOY_PATH = "deploy_path"
STORAGE_MODE = "storage_mode"

component_lables = [ENDPOINT, ROLE]
table_lables = [TABLE_PATH, TABLE_ID, STORAGE_MODE]

connected_seconds = Counter("connected",
                            "duration for a component conncted time in seconds",
                            component_lables,
                            namespace=NAMESPACE,
                            unit="seconds")

component_status = Enum(
    "status",
    "component status",
    component_lables,
    states=["online", "offline"],
    namespace=NAMESPACE,
)

tablet_memory_application = Gauge(
    "tablet_memory_application",
    "tablet application memory usage in bytes",
    [ENDPOINT],
    namespace=NAMESPACE,
    unit="bytes",
)

tablet_memory_actual = Gauge(
    "tablet_memory_actual_used",
    "actual memory used in bytes for tablet application",
    [ENDPOINT],
    namespace=NAMESPACE,
    unit="bytes",
)

table_rows = Gauge(
    "table_rows",
    "table row count",
    table_lables,
    namespace=NAMESPACE,
)

table_partitions = Gauge(
    "table_partitions",
    "table partition count",
    table_lables,
    namespace=NAMESPACE,
)

table_partitions_unalive = Gauge(
    "table_partitions_unalive",
    "table partition count that is unalive",
    table_lables,
    namespace=NAMESPACE,
)

table_replica = Gauge(
    "table_replica",
    "table replica count",
    table_lables,
    namespace=NAMESPACE,
)

table_disk = Gauge(
    "table_disk",
    "table disk usage in bytes",
    table_lables,
    namespace=NAMESPACE,
    unit="bytes",
)

table_memory = Gauge(
    "table_memory",
    "table memory usage in bytes",
    table_lables,
    namespace=NAMESPACE,
    unit="bytes",
)

BUCKETS = (1 / 1000000, 1 / 100000, 1 / 10000, 1 / 1000, 1 / 100, 1 / 10, 1, 10, 100, 1000, 10000, 100000, 1000000, INF)
deploy_response_time = Histogram(
    "deploy_response_time",
    "Deployment query response time histogram",
    [DEPLOY_PATH],
    subsystem="info_schema",
    namespace=NAMESPACE,
    unit="seconds",
    buckets=BUCKETS,
)
