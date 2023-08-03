#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

import pytest
from diagnostic_tool.diagnose import parse_arg, main
from .case_conf import OpenMLDB_ZK_CLUSTER


def test_rpc():
    cluster_arg = f"--cluster={OpenMLDB_ZK_CLUSTER}"
    args = parse_arg(
        [
            "foo",
            "rpc",
            cluster_arg,
        ]
    )
    main(args)

    main(parse_arg(["foo", "rpc", cluster_arg, "ns"]))
    main(parse_arg(["foo", "rpc", cluster_arg, "tablet1"]))
    # no taskmanager in test onebox
    if not "onebox" in OpenMLDB_ZK_CLUSTER:
        main(parse_arg(["foo", "rpc", cluster_arg, "tm"]))
