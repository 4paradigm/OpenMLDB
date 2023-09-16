import pytest
from diagnostic_tool.inspect import show_table_info
from absl import flags

flags.FLAGS["nocolor"].parse(False)
flags.FLAGS["table_width"].parse(12)


def test_show():
    # assume 3 tablet server
    tablets = ["0.0.0.0:1111", "0.0.0.0:2222", "0.0.0.0:3333"]
    tablet2idx = {tablet: i + 1 for i, tablet in enumerate(tablets)}
    # simple
    t_info = {
        "name": "TABLE_A",
        "table_partition": [
            {
                "pid": 0,
                "partition_meta": [
                    {
                        "endpoint": tablets[0],
                        "is_leader": True,
                        # "offset": 0,
                        # "record_cnt": 0,
                        # "record_byte_size": 0,
                        # "tablet_has_partition": true,
                        # "diskused": 9025,
                    },
                    {
                        "endpoint": tablets[1],
                        "is_leader": False,
                    },
                ],
                # "term_offset": [{"term": 1, "offset": 0}],
                # "record_cnt": 0,
                # "record_byte_size": 0,
                # "diskused": 9025,
            }
        ],
        "tid": 0,
        "partition_num": 1,
        "replica_num": 2,
        "db": "DB_A",
    }

    replicas = {
        0: {
            0: {
                tablets[0]: {
                    "tid": 0,  # actually not used in show_table_info
                    "pid": 0,  # not used in show_table_info
                    # "offset": 5,
                    "mode": "kTableLeader",
                    "state": "kTableNormal",
                    # "is_expire": True,
                    # "record_cnt": 1,
                    # "idx_cnt": 1,
                    # "ts_idx_status": [
                    #     {"idx_name": "id", "seg_cnts": [0, 0, 0, 0, 0, 0, 1, 0]}
                    # ],
                    "name": "Foo",
                    # "record_byte_size": 127,
                    # "record_idx_byte_size": 177,
                    # "record_pk_cnt": 1,
                    # "compress_type": "kNoCompress",
                    # "skiplist_height": 1,
                    # "diskused": 10074,
                    # "storage_mode": "kMemory",
                    "tablet": tablets[0],
                },
                tablets[1]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[1],
                },
            }
        }
    }

    show_table_info(t_info, replicas, tablet2idx)

    print("healthy ns meta, but replicas on tablet are all follower")
    t_info = {
        "name": "TABLE_A",
        "table_partition": [
            {
                "pid": 0,
                "partition_meta": [
                    {
                        "endpoint": tablets[0],
                        "is_leader": True,
                    },
                    {
                        "endpoint": tablets[1],
                        "is_leader": False,
                    },
                    {
                        "endpoint": tablets[2],
                        "is_leader": False,
                    },
                ],
            }
        ],
        "tid": 0,
        "partition_num": 1,
        "replica_num": 3,
        "db": "DB_A",
    }
    replicas = {
        0: {
            0: {
                tablets[0]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[0],
                },
                tablets[1]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[1],
                },
                tablets[2]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[2],
                },
            }
        }
    }
    show_table_info(t_info, replicas, tablet2idx)

    print("ns meta all followers, no leader")
    t_info = {
        "name": "TABLE_A",
        "table_partition": [
            {
                "pid": 0,
                "partition_meta": [
                    {
                        "endpoint": tablets[0],
                        "is_leader": False,
                    },
                    {
                        "endpoint": tablets[1],
                        "is_leader": False,
                    },
                    {
                        "endpoint": tablets[2],
                        "is_leader": False,
                    },
                ],
            }
        ],
        "tid": 0,
        "partition_num": 1,
        "replica_num": 3,
        "db": "DB_A",
    }
    replicas = {
        0: {
            0: {
                tablets[0]: {
                    "mode": "kTableLeader",
                    "state": "kTableNormal",
                    "tablet": tablets[0],
                },
                tablets[1]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[1],
                },
                tablets[2]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[2],
                },
            }
        }
    }
    show_table_info(t_info, replicas, tablet2idx)

    print("no corresponding replica on tablet server")
    t_info = {
        "name": "TABLE_A",
        "table_partition": [
            {
                "pid": 0,
                "partition_meta": [
                    {
                        "endpoint": tablets[0],
                        "is_leader": True,
                    },
                    {
                        "endpoint": tablets[1],
                        "is_leader": False,
                    },
                    {
                        "endpoint": tablets[2],
                        "is_leader": False,
                    },
                ],
            }
        ],
        "tid": 0,
        "partition_num": 1,
        "replica_num": 3,
        "db": "DB_A",
    }
    replicas = {
        0: {
            0: {
                tablets[1]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[1],
                },
                tablets[2]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                    "tablet": tablets[2],
                },
            }
        }
    }
    show_table_info(t_info, replicas, tablet2idx)

    print("meta match, but state is not normal")
    t_info = {
        "name": "TABLE_A",
        "table_partition": [
            {
                "pid": 0,
                "partition_meta": [
                    {
                        "endpoint": tablets[0],
                        "is_leader": True,
                    },
                    {
                        "endpoint": tablets[1],
                        "is_leader": False,
                    },
                    {
                        "endpoint": tablets[2],
                        "is_leader": False,
                    },
                ],
            },
            {
                "pid": 1,
                "partition_meta": [
                    {
                        "endpoint": tablets[0],
                        "is_leader": True,
                    },
                    {
                        "endpoint": tablets[1],
                        "is_leader": False,
                    },
                    {
                        "endpoint": tablets[2],
                        "is_leader": False,
                    },
                ],
            },
        ],
        "tid": 0,
        "partition_num": 2,
        "replica_num": 3,
        "db": "DB_A",
    }
    replicas = {
        0: {
            0: {
                tablets[0]: {
                    "mode": "kTableFollower",
                    "state": "kTableLoading",
                    "tablet": tablets[0],
                },
                tablets[1]: {
                    "mode": "kTableFollower",
                    "state": "kMakingSnapshot",
                    "tablet": tablets[1],
                },
                tablets[2]: {
                    "mode": "kTableFollower",
                    "state": "kSnapshotPaused",
                    "tablet": tablets[2],
                },
            },
            1: {
                tablets[1]: {
                    "mode": "kTableFollower",
                    "state": "kTableUndefined",
                },
                tablets[2]: {
                    "mode": "kTableFollower",
                    "state": "kTableNormal",
                },
            },
        }
    }
    show_table_info(t_info, replicas, tablet2idx)

    print("more partitions, display well")
    partnum = 13
    meta_pattern = {
        "partition_meta": [
            {
                "endpoint": tablets[0],
                "is_leader": True,
            },
        ],
    }
    t_info = {
        "name": "TABLE_A",
        "table_partition": [],
        "tid": 0,
        "partition_num": partnum,
        "replica_num": 1,
        "db": "DB_A",
    }
    replicas = {0: {}}

    for i in range(partnum):
        t_info["table_partition"].append({"pid": i, **meta_pattern})

    for i in range(partnum):
        replicas[0][i] = {
            tablets[0]: {
                "mode": "kTableLeader",
                "state": "kTableNormal",
                "tablet": tablets[0],
            }
        }
    print(t_info, replicas)
    show_table_info(t_info, replicas, tablet2idx)

    print("nocolor")
    flags.FLAGS["nocolor"].parse(True)
    show_table_info(t_info, replicas, tablet2idx)
