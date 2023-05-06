import requests

from prometheus_client.parser import text_string_to_metric_families
import time

import random


def test_components_online(global_url):
    # Make a request to your application to get the Prometheus metrics
    response = requests.get(global_url)

    # Parse the metrics from the response
    metrics = text_string_to_metric_families(response.text)

    ns_cnt = 0
    tb_cnt = 0
    # Assert that the metrics are as expected
    for metric in metrics:
        # all components online
        if metric.name == "openmldb_status":
            for sample in metric.samples:
                if sample.value == 1.0:
                    if sample.labels["role"] == "nameserver":
                        ns_cnt += 1
                    elif sample.labels["role"] == "tablet":
                        tb_cnt += 1

                    assert sample.labels["openmldb_status"] == "online"

    assert ns_cnt == 2
    assert tb_cnt == 3


def test_tablet_mem(global_url):
    response = requests.get(global_url)

    metrics = text_string_to_metric_families(response.text)

    app_cnt = 0
    actual_cnt = 0
    for metric in metrics:
        if metric.name == "openmldb_tablet_memory_application_bytes":
            for sample in metric.samples:
                assert sample.value > 0
                app_cnt += 1
        elif metric.name == "openmldb_tablet_memory_actual_used_bytes":
            for sample in metric.samples:
                assert sample.value > 0
                actual_cnt += 1

    assert app_cnt == 3
    assert actual_cnt == 3


def test_table_status(global_url, conn):
    response = requests.get(global_url)

    metrics = text_string_to_metric_families(response.text)

    metric_name_to_len_dict = {
        "openmldb_table_rows": 0,
        "openmldb_table_partitions": 0,
        "openmldb_table_replica": 0,
        "openmldb_table_disk_bytes": 0,
        "openmldb_table_memory_bytes": 0
    }

    metric_expect_value_dict = {
        "openmldb_table_rows": 3,
        "openmldb_table_partitions": 8,  # default value
        "openmldb_table_replica": 3,
    }

    # before: no tables
    for metric in metrics:
        if metric.name in list(metric_name_to_len_dict.keys()):
            metric_name_to_len_dict[metric.name] = len(metric.samples)

    # new table
    db = "db" + str(int(time.time()))
    tb = "tb" + str(int(time.time()))
    conn.execute("create database " + db)
    conn.execute("use " + db)
    conn.execute("create table " + tb + " (id int, val string)")
    conn.execute("insert into " + tb + " values (1, '100')")
    conn.execute("insert into " + tb + " values (2, '200')")
    conn.execute("insert into " + tb + " values (3, '300')")

    # wait for metric pull
    time.sleep(60)

    response = requests.get(global_url)
    metrics = text_string_to_metric_families(response.text)

    for metric in metrics:
        if metric.name in list(metric_name_to_len_dict.keys()):
            # one more series
            assert len(
                metric.samples) == metric_name_to_len_dict[metric.name] + 1

            for sample in metric.samples:
                if sample.labels["table_path"] == db + "_" + tb:
                    if metric.name in list(metric_expect_value_dict.keys()):
                        # rows, partition, replica
                        assert sample.value == metric_expect_value_dict[
                            metric.name], f"{sample}"
                    elif metric.name == "openmldb_table_memory_bytes":
                        # memory bytes
                        assert sample.value > 0, f"{sample}"
                    elif metric.name == "openmldb_table_disk_bytes":
                        assert sample.value == 0, f"{sample}"

                    assert sample.labels[
                        "storage_mode"] == "memory", f"{sample}"


def test_connected_seconds(global_url):
    response = requests.get(global_url)
    metrics = text_string_to_metric_families(response.text)

    cnt = 0
    ns_cnt = 0
    tablet_cnt = 0
    for metric in metrics:
        if metric.name == "openmldb_connected_seconds":
            cnt += 1
            for sample in metric.samples:
                if sample.labels["role"] == "tablet":
                    tablet_cnt += 1
                elif sample.labels["role"] == "nameserver":
                    ns_cnt += 1
                assert sample.value > 0.0

    assert cnt == 1
    assert ns_cnt == 2
    assert tablet_cnt == 3


BUCKET_CNT_EACH_DEPLOY = 14
DEPLOY_SAMPLE_CNT_EACH_DEPLOY = BUCKET_CNT_EACH_DEPLOY + 1 + 1
DEPLOY_METRIC_NAME = "openmldb_info_schema_deploy_response_time_seconds"
DEPLOY_METRIC_NAME_BUCKET = DEPLOY_METRIC_NAME + "_bucket"
DEPLOY_METRIC_NAME_COUNT = DEPLOY_METRIC_NAME + "_count"
DEPLOY_METRIC_NAME_SUM = DEPLOY_METRIC_NAME + "_sum"
def test_deploy_response_time(global_url, conn, api_url):
    response = requests.get(global_url)
    metrics = text_string_to_metric_families(response.text)

    old_deploy_sample_cnt = 0;
    for metric in metrics:
        if metric.name == DEPLOY_METRIC_NAME:
            old_deploy_sample_cnt = len(metric.samples)


    db = "db" + str(int(time.time()))
    tb = "tb" + str(int(time.time()))
    dp = "dp" + str(int(time.time()))
    conn.execute("create database " + db)
    conn.execute("use " + db)
    conn.execute(f"create table {tb} (id int, val string, ts timestamp)")
    conn.execute(f"deploy {dp} select id, count(val) over w as cnt from {tb} window w as (partition by id order by ts rows_range between 2s preceding and current row)")

    post_ep = f"{api_url}/dbs/{db}/deployments/{dp}"
    post_data = {
        "input": [ [ 1, "12", 1000 ] ]
    }

    deploy_cnt = random.randint(5, 100)

    for _ in range(deploy_cnt):
        try:
            res = requests.post(post_ep, json=post_data, timeout=5)
            assert res.status_code == 200, f"{res}"
        except Exception as e:
            assert False, f"apiserver is down: {e}"

    time.sleep(60)

    response = requests.get(global_url)
    metrics = text_string_to_metric_families(response.text)

    new_bucket = 0
    new_cnt = 0
    new_sum = 0
    new_cnt_value = 0
    for metric in metrics:
        print(metric)

        if metric.name == DEPLOY_METRIC_NAME:
            assert len(metric.samples) == old_deploy_sample_cnt + DEPLOY_SAMPLE_CNT_EACH_DEPLOY

            for sample in metric.samples:
                if sample.labels["deploy_path"] == db + "." + dp:
                    if sample.name == DEPLOY_METRIC_NAME_BUCKET:
                        new_bucket += 1
                    elif sample.name == DEPLOY_METRIC_NAME_COUNT:
                        new_cnt += 1
                        new_cnt_value = sample.value
                    elif sample.name == DEPLOY_METRIC_NAME_SUM:
                        new_sum += 1


    assert new_cnt_value == deploy_cnt
    assert new_cnt == 1
    assert new_sum == 1
    assert new_bucket == BUCKET_CNT_EACH_DEPLOY

