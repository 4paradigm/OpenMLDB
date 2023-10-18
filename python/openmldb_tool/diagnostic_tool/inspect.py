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

""" gen multi-level readable reports for cluster devops """
from absl import flags
import json
from collections import defaultdict
from prettytable import PrettyTable

from .rpc import RPC

# ANSI escape codes
flags.DEFINE_bool("nocolor", False, "disable color output", short_name="noc")
flags.DEFINE_integer(
    "table_width",
    12,
    "max columns in one row, 1 partition use r+1 cols, set k*(r+1)",
    short_name="tw",
)
flags.DEFINE_integer(
    "offset_diff_thresh", 100, "offset diff threshold", short_name="od"
)

# color: red, green
RED = "\033[31m"
GREEN = "\033[32m"
BLUE = "\033[34m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"


# switch by nocolor flag
def cr_print(color, obj):
    if flags.FLAGS.nocolor or color == None:
        print(obj)
    else:
        print(f"{color}{obj}{RESET}")


def server_ins(server_map):
    print("\n\nServer Detail")
    print(server_map)
    offlines = []
    for component, value_list in server_map.items():
        for endpoint, status in value_list:
            if status != "online":
                offlines.append(f"[{component}]{endpoint}")
                continue  # offline tablet is needlessly to rpc
    if offlines:
        s = "\n".join(offlines)
        cr_print(RED, f"offline servers:\n{s}")
    else:
        cr_print(GREEN, "all servers online (no backup tm and apiserver)")
    return offlines


# support nocolor
def light(color, symbol, detail):
    if flags.FLAGS.nocolor:
        return f"{symbol} {detail}"
    else:
        return f"{color}{symbol}{RESET} {detail}"


def state2light(state):
    state = state.ljust(15)  # state str should be less than 15
    if not state.startswith("k"):
        # meta mismatch status, all red
        return light(RED, "X", state)
    else:
        # meta match, get the real state
        state = state[1:]
        if state.startswith("TableNormal"):
            # green
            return light(GREEN, "O", state)
        else:
            # ref https://github.com/4paradigm/OpenMLDB/blob/0462f8a9682f8d232e8d44df7513cff66870d686/tools/tool.py#L291
            # undefined is loading too: state == "kTableLoading" or state == "kTableUndefined"
            # snapshot doesn't mean unhealthy: state == "kMakingSnapshot" or state == "kSnapshotPaused"
            return light(YELLOW, "=", state)


# similar with `show table status` warnings field, but easier to read
# prettytable.colortable just make table border and header lines colorful, so we color the value
def check_table_info(t, replicas_on_tablet, tablet2idx):
    pnum, rnum = t["partition_num"], t["replica_num"]
    assert pnum == len(t["table_partition"])
    # multi-line for better display, max display columns in one row
    valuable_cols = pnum * (rnum + 1)
    display_width = min(flags.FLAGS.table_width, valuable_cols)
    # if real multi-line, the last line may < width, padding with empty string
    rest = valuable_cols % display_width
    total_cols = valuable_cols + (0 if rest == 0 else display_width - rest)

    idx_row = [""] * total_cols
    leader_row = [""] * total_cols
    followers_row = [""] * total_cols

    table_mark = 0
    hint = ""
    for i, p in enumerate(t["table_partition"]):
        # each partition add 3 row, and rnum + 1 columns
        # tablet idx  pid | 1 | 4 | 5
        # leader       1        o
        # followers         o       o
        pid = p["pid"]
        assert pid == i

        # sort by list tablets
        replicas = []
        for r in p["partition_meta"]:
            tablet = r["endpoint"]
            # tablet_has_partition useless
            # print(r["endpoint"], r["is_leader"], r["tablet_has_partition"])
            replicas_on_t = replicas_on_tablet[t["tid"]][p["pid"]]
            # may can't find replica on tablet, e.g. tablet server is not ready
            info_on_tablet = {}
            if r["endpoint"] not in replicas_on_t:
                info_on_tablet = {"state": "Miss", "mode": "Miss", "offset": -1}
            else:
                info_on_tablet = replicas_on_t[r["endpoint"]]
            # print(info_on_tablet)
            m = {
                "role": "leader" if r["is_leader"] else "follower",
                "state": info_on_tablet["state"],
                "acrole": info_on_tablet["mode"],
                "offset": info_on_tablet["offset"],
            }
            replicas.append((tablet2idx[tablet], m))

        assert len(replicas) == rnum
        replicas.sort(key=lambda x: x[0])
        leader_ind = [i for i, r in enumerate(replicas) if r[1]["role"] == "leader"]
        # replica on offline tablet is still in the ns meta, so leader may > 1
        # assert len(ind) <= 1, f"should be only one leader or miss leader in {replicas}"

        # show partition idx and tablet server idx
        cursor = i * (rnum + 1)
        idx_row[cursor : cursor + rnum + 1] = ["p" + str(pid)] + [
            r[0] for r in replicas
        ]

        # fulfill leader line
        if leader_ind:
            for leader in leader_ind:
                # leader state
                lrep = replicas[leader][1]
                if lrep["state"] != "Miss" and lrep["acrole"] != "kTableLeader":
                    lrep["state"] = "NotLeaderOnT"  # modify the state
                leader_row[cursor + leader + 1] = state2light(lrep["state"])
        else:
            # can't find leader in nameserver metadata, set in the first column(we can't find leader on any tablet)
            leader_row[cursor] = state2light("NotFound")

        # fulfill follower line
        for i, r in enumerate(replicas):
            idx = cursor + i + 1
            if i in leader_ind:
                continue
            frep = r[1]
            if frep["state"] != "Miss" and frep["acrole"] != "kTableFollower":
                frep["state"] = "NotFollowerOnT"
            followers_row[idx] = state2light(frep["state"])

        # after state adjust, diag table
        replicas = [r[1] for r in replicas]  # tablet server is needless now
        # fatal: leader replica is not normal, may read/write fail
        # get one normal leader, the partition can work
        if not leader_ind or not any(
            [replicas[i]["state"] == "kTableNormal" for i in leader_ind]
        ):
            table_mark = max(4, table_mark)
            hint += f"partition {pid} leader replica is not normal\n"
        # warn: need repair(may auto repair by auto_failover), but not in emergency
        #  follower replica is not normal
        if any([r["state"] != "kTableNormal" for r in replicas]):
            table_mark = max(3, table_mark)
            hint += f"partition {pid} has unhealthy replicas\n"

        #  offset is not consistent, only check normal replicas
        offsets = [r["offset"] for r in replicas if r["state"] == "kTableNormal"]
        if offsets and max(offsets) - min(offsets) > flags.FLAGS.offset_diff_thresh:
            table_mark = max(3, table_mark)
            hint += (
                f"partition {pid} has offset diff > {flags.FLAGS.offset_diff_thresh}\n"
            )

    x = PrettyTable(align="l")

    x.field_names = [i for i in range(display_width)]
    step = display_width
    for i in range(0, len(idx_row), step):
        x.add_row(idx_row[i : i + step])
        x.add_row(leader_row[i : i + step])
        x.add_row(followers_row[i : i + step], divider=True)

    table_summary = ""
    if table_mark >= 4:
        table_summary = light(
            RED,
            "X",
            f"Fatal table {t['db']}.{t['name']}, read/write may fail, need repair immediately",
        )
    elif table_mark >= 3:
        table_summary = light(
            YELLOW, "=", f"Warn table {t['db']}.{t['name']}, still work, but need repair"
        )
    if table_summary:
        table_summary += "\n" + hint
    return x, table_summary


def show_table_info(t, replicas_on_tablet, tablet2idx):
    """check table info and display for ut"""
    print(
        f"Table {t['tid']} {t['db']}.{t['name']} {t['partition_num']} partitions {t['replica_num']} replicas"
    )
    table, _ = check_table_info(t, replicas_on_tablet, tablet2idx)
    print(table.get_string(border=True, header=False))


def table_ins(connect):
    print("\n\nTable Healthy Detail")
    rs = connect.execfetch("show table status like '%';")
    rs.sort(key=lambda x: x[0])
    print(f"summary: {len(rs)} tables(including system tables)")
    warn_tables = []
    for t in rs:
        # any warning means unhealthy, partition_unalive may be 0 but already unhealthy, warnings is accurate?
        if t[13]:
            warn_tables.append(t)
    if warn_tables:
        # only show tables name
        s = "\n".join([f"{t[2]}.{t[1]}" for t in warn_tables])
        cr_print(RED, f"unhealthy tables:\n{s}")
    else:
        cr_print(GREEN, "all tables are healthy")
    return warn_tables


def partition_ins(server_map, related_ops):
    print("\n\nTable Partition Detail")
    # ns table info
    rpc = RPC("ns")
    res = rpc.rpc_exec("ShowTable", {"show_all": True})
    if not res:
        cr_print(RED, "get table info failed or empty from nameserver")
        return
    res = json.loads(res)
    all_table_info = res["table_info"]

    # get table info from tablet server
    # <tid, <pid, <tablet, replica>>>
    replicas = defaultdict(lambda: defaultdict(dict))
    tablets = server_map["tablet"]  # has status
    invalid_tablets = set()
    for tablet, status in tablets:
        if status == "offline":
            invalid_tablets.add(tablet)
            continue
        # GetTableStatusRequest empty field means get all
        rpc = RPC(tablet)
        res = None
        try:
            res = json.loads(rpc.rpc_exec("GetTableStatus", {}))
        except Exception as e:
            print(f"rpc {tablet} failed")
        # may get empty when tablet server is not ready
        if not res or res["code"] != 0:
            cr_print(RED, f"get table status failed or empty from {tablet}(online)")
            invalid_tablets.add(tablet)
            continue
        if "all_table_status" not in res:
            # just empty replica on tablet, skip
            continue
        for rep in res["all_table_status"]:
            rep["tablet"] = tablet
            # tid, pid are int
            tid, pid = rep["tid"], rep["pid"]
            replicas[tid][pid][tablet] = rep

    tablet2idx = {tablet[0]: i + 1 for i, tablet in enumerate(tablets)}
    print(f"tablet server order: {tablet2idx}")
    if invalid_tablets:
        cr_print(
            RED,
            f"some tablet servers are offline/bad, can't get table info(exclude empty table server): {invalid_tablets}",
        )

    # display, depends on table info, replicas are used to check
    all_table_info.sort(key=lambda x: x["tid"])
    # related op map
    related_ops_map = {}
    for op in related_ops:
        db = op[9]
        table = op[10]
        if db not in related_ops_map:
            related_ops_map[db] = {}
        if table not in related_ops_map[db]:
            related_ops_map[db][table] = []
        related_ops_map[db][table].append(op)
    # print(f"related ops: {related_ops_map}")
    print("")  # for better display
    diag_result = []
    for t in all_table_info:
        # no need to print healthy table
        table, diag_hint = check_table_info(t, replicas, tablet2idx)
        if diag_hint:
            print(
                f"Table {t['tid']} {t['db']}.{t['name']} {t['partition_num']} partitions {t['replica_num']} replicas"
            )
            print(table.get_string(header=False))
            if t["db"] in related_ops_map and t["name"] in related_ops_map[t["db"]]:
                diag_hint += f"related op: {sorted(related_ops_map[t['db']][t['name']], key=lambda x: x[11])}"  # 11 is pid
            diag_result.append(diag_hint)
    # comment for table info display, only for unhealthy table TODO: draw a example
    if diag_result:
        print(
            """
Example:
tablet server order: {'xxx': 1, 'xxx': 2, 'xxx': 3}              -> get real tablet addr by idx
+----+-------------------+------------------+------------------+
| p0 | 1                 | 2                | 3                | -> p0: partition 0, 1-3: tablet server idx
|    | [light] status    |                  |                  | -> leader replica is on tablet 1
|    |                   | [light] status   | [light] status   | -> follower replicas are on tablet 2, 3
+----+-------------------+------------------+------------------+
light:
Green O -> OK
Yellow = -> replica meta is ok but state is not normal
Red X -> NotFound/Miss/NotFollowerOnT/NotLeaderOnT"""
        )
    return diag_result


def ops_ins(connect):
    # op sorted by id TODO: detail to show all include succ op?
    print("\n\nOps Detail")
    print("> failed ops do not mean cluster is unhealthy, just for reference")
    rs = connect.execfetch("show jobs from NameServer;")
    should_warn = []
    from datetime import datetime
    # already in order
    ops = [list(op) for op in rs]
    for i in range(len(ops)):
        op = ops[i]
        op[3] = str(datetime.fromtimestamp(int(op[3]) / 1000)) if op[4] else "..."
        op[4] = str(datetime.fromtimestamp(int(op[4]) / 1000)) if op[4] else "..."
        if op[2] != "FINISHED":
            should_warn.append(op)
    # peek last one to let user know if cluster has tried to recover, or we should wait
    print("last one op(check time): ", ops[-1])
    if not should_warn:
        print("all nameserver ops are finished")
    else:
        print("last 10 unfinished ops:")
        print(*should_warn[-10:], sep="\n")
    recover_type = ["kRecoverTableOP", "kChangeLeaderOP", "kReAddReplicaOP", "kOfflineReplicaOP"]
    related_ops = [
        op
        for op in should_warn
        if op[1] in recover_type and op[2] in ["Submitted", "RUNNING"]
    ]
    return related_ops


def inspect_hint(server_hint, table_hints):
    print(
        """

==================
Summary & Hint
==================
Server:
"""
    )
    if server_hint:
        cr_print(RED, f"offline servers {server_hint}, restart them first")
    else:
        cr_print(GREEN, "all servers online")
    print("\nTable:\n")
    for h in table_hints:
        print(h)
    if table_hints:
        print(
            """
    Make sure all servers online, and no ops for the table is running.
    Repair table manually, run recoverdata, check https://openmldb.ai/docs/zh/main/maintain/openmldb_ops.html.
    Check 'Table Partitions Detail' above for detail.
    """
        )
    else:
        cr_print(GREEN, "all tables are healthy")
