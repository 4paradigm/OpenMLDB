
import random
import numpy as np
import time
import datetime

from fesql_const import current_time
from fesql_window import WindowDesc
from gen_const_data import random_literal_string

from fesql_param import sample_integer_config


def sample_bool(nullable=True):
    if nullable:
        return random.choice([True, False, None])
    else:
        return random.choice([True, False])


def sample_int16(nullable=True):
    categories = [0, -1, 1, 2**15 - 1, -2**15]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        return random.randint(-2**15, 2**15 - 1)


def sample_int32(nullable=True):
    categories = [0, -1, 1, 2 ** 31 - 1, -2 ** 31 + 1]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        return random.randint(-2 ** 31 + 1, 2 ** 31 - 1)


def sample_int64(nullable=True):
    categories = [0, -1, 1, 2 ** 63 - 1, -2 ** 63]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        return random.randint(-2 ** 63, 2 ** 63 - 1)


def sample_float(nullable=True):
    finfo = np.finfo(np.float32)
    categories = [
        0.0, 1.0, -1.0,
        # "nan", "+inf", "-inf",
        float(finfo.tiny),
        float(finfo.min),
        float(finfo.max)
    ]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        lower = round(float(finfo.min), 5)
        upper = round(float(finfo.max), 5)
        return random.uniform(lower, upper)


def sample_double(nullable=True):
    finfo = np.finfo(np.float64)
    categories = [
        0.0, 1.0, -1.0,
        # "nan", "+inf", "-inf",
        float(finfo.tiny),
        float(finfo.min),
        float(finfo.max)
    ]
    if nullable:
        categories = categories + [None]
    idx = random.randint(0, len(categories))
    if idx < len(categories):
        return categories[idx]
    else:
        lower = round(float(finfo.min) / 2, 15)
        upper = round(float(finfo.max) / 2, 15)
        return random.uniform(lower, upper)


def sample_date(args, nullable=True):
    res = sample_timestamp(args, nullable=nullable)
    if res is not None:
        res = time.strftime("%Y-%m-%d", time.localtime(res/1000))
    return res

def compute_pre_time(gen_time_mode:str):
    '''
    根据字符串计算当前时间之前的时间
    :param gen_time_mode:
    :return: 毫秒级时间戳，int
    '''
    time_unit = gen_time_mode[-1]
    time_num = int(gen_time_mode[0:-1])
    time_compute = {
        'S':current_time-datetime.timedelta(milliseconds=time_num),
        's':current_time-datetime.timedelta(seconds=time_num),
        'm':current_time-datetime.timedelta(minutes=time_num),
        'h':current_time-datetime.timedelta(hours=time_num),
        'd':current_time-datetime.timedelta(days=time_num)
    }
    if time_unit not in time_compute:
        raise Exception("不支持的时间单位")
    pre_time=int(time_compute[time_unit].timestamp()*1000)
    return pre_time

def gen_time_unit(gen_time_mode:str):
    begin_time=compute_pre_time(gen_time_mode)
    end_time=int(current_time.timestamp()*1000)
    gen_time = random.randint(begin_time, end_time)
    return gen_time

def gen_time_be(args):
    begin_time = int(args.begin_time)
    end_time = int(args.end_time)
    if end_time == 0:
        end_time = int(current_time.timestamp()*1000)
    gen_time = random.randint(begin_time, end_time)
    return gen_time

def has_time_window(w_defs:list):
    for w in w_defs:
        if w.rows_between == 0:
            return True
    return False

def get_max_time_window(w_defs:list,multiple:int):
    begin_time = current_time.timestamp()*1000
    end_time = 0
    for w in w_defs:
        # 0 ：时间窗口
        if w.rows_between == 0:
            rows_range_unit = w.rows_range_unit
            if rows_range_unit == "":
                rows_range_unit = "S"
            if w.preceding.lower().startswith("current"):
                begin_num = 0
            else:
                begin_num = int(w.preceding)*multiple
            begin = compute_pre_time(str(begin_num)+rows_range_unit)
            if begin < begin_time:
                begin_time = begin
            if w.following.lower().startswith("current"):
                end_num = 0
            else:
                end_num = int(w.following)//multiple
            end = compute_pre_time(str(end_num)+rows_range_unit)
            if end > end_time:
                end_time = end
    return begin_time,end_time

def gen_time_auto(w_defs:list,args):
    multiple = int(args.auto_time_multiple)
    # 判断是否包含时间窗口
    if has_time_window(w_defs):
        begin_time,end_time = get_max_time_window(w_defs, multiple)
        gen_time = random.randint(begin_time, end_time)
        return gen_time
    else:
        return gen_time_be(args)

def sample_ts_timestamp(args,w_defs:list, nullable=True):
    if nullable:
        if random.randint(0, 3) == 0:
            return None
    gen_time_mode = args.gen_time_mode.strip()
    if gen_time_mode == "auto":
        return gen_time_auto(w_defs, args)
    elif gen_time_mode == "be":
        return gen_time_be(args)
    else:
        return gen_time_unit(gen_time_mode)

def sample_timestamp(args, nullable=True):
    if nullable:
        if random.randint(0, 3) == 0:
            return None
    # lower = int(time.mktime(time.strptime("2000-01-01", "%Y-%m-%d")))
    # upper = int(time.mktime(time.strptime("2020-01-01", "%Y-%m-%d")))
    # return random.randint(lower, upper)
    return gen_time_be(args)

def sample_ts_value(dtype, args, w_defs:list, nullable=True):
    if dtype == "int64":
        return sample_int64(nullable=nullable)
    else:
        return sample_ts_timestamp(args, w_defs, nullable)


def sample_string(nullable=True):
    if nullable:
        if random.randint(0, 3) == 0:
            return None
    return random_literal_string()

def sample_value(dtype, args, nullable=True):
    if dtype == "bool":
        return sample_bool(nullable=nullable)
    elif dtype == "int16":
        return sample_int16(nullable=nullable)
    elif dtype == "int32":
        return sample_int32(nullable=nullable)
    elif dtype == "int64":
        return sample_int64(nullable=nullable)
    elif dtype == "float":
        return sample_float(nullable=nullable)
    elif dtype == "double":
        return sample_double(nullable=nullable)
    elif dtype == "date":
        return sample_date(args, nullable=nullable)
    elif dtype == "timestamp":
        return sample_timestamp(args, nullable=nullable)
    elif dtype == "string":
        return sample_string(nullable=nullable)
    else:
        raise ValueError("Can not sample from dtype: " + str(dtype))

def gen_pk_groups(args, partition_columns):
    '''
    根据pk的个数以及num_pk 生成pk样本组
    :param args:
    :param partition_columns:
    :return:
    '''
    pk_groups = []
    if partition_columns is not None:
        for col in partition_columns:
            group = []
            # num_pk 每个pk列生成pk的个数
            num_pk = sample_integer_config(args.num_pk)
            for _ in range(num_pk):
                pk = sample_value(col.dtype, args, nullable=False)
                group.append(pk)
            pk_groups.append(group)
    return pk_groups

def gen_simple_data(columns, args, w_defs:list,
                    index_column=None,
                    partition_columns=None,
                    order_columns=None,
                    pk_groups=None):
    '''
    生成样本数据
    :param columns:
    :param args:
    :param index_column:
    :param partition_columns:
    :param order_columns:
    :return:
    '''
    columns_dict = {}
    for idx, col in enumerate(columns):
        columns_dict[col.name] = idx
    column_num = len(columns)

    index_column_idx = -1
    if index_column is not None:
        index_column_idx = columns_dict[index_column.name]

    partition_idxs = []
    if partition_columns is not None:
        partition_idxs = [columns_dict[_.name] for _ in partition_columns]

    order_idxs = []
    if order_idxs is not None:
        order_idxs = [columns_dict[_.name] for _ in order_columns]

    normal_column_idxs = []
    for i in range(column_num):
        if i == index_column_idx or i in partition_idxs or i in order_idxs:
            continue
        normal_column_idxs.append(i)

    if pk_groups is None:
        pk_groups = []
        if partition_columns is not None:
            for col in partition_columns:
                group = []
                # num_pk 每个pk列生成pk的个数
                num_pk = sample_integer_config(args.num_pk)
                for _ in range(num_pk):
                    pk = sample_value(col.dtype, args, nullable=False)
                    group.append(pk)
                pk_groups.append(group)

    # output rows
    rows = []

    def gen_under_pk(cur_pks):
        # size_of_pk 决定每组key 下生成的数据条数
        for _ in range(sample_integer_config(args.size_of_pk)):
            row = [None for _ in range(column_num)]
            if index_column_idx >= 0:
                row[index_column_idx] = len(rows)
            for k, pk_val in enumerate(cur_pks):
                row[partition_idxs[k]] = pk_val
            for k in order_idxs:
                order_col = columns[k]
                order_val = sample_ts_value(order_col.dtype, args, w_defs, order_col.nullable)
                row[k] = order_val
            for k in normal_column_idxs:
                normal_col = columns[k]
                value = sample_value(normal_col.dtype, args, normal_col.nullable)
                row[k] = value
            rows.append(row)
    def recur_pk(pk_idx, cur_pks):
        '''
        递归地生成 pk 的样本数据
        :param pk_idx:
        :param cur_pks:
        :return:
        '''
        # cur_pks被塞满样本数据 则不进行递归
        if pk_idx == len(cur_pks):
            gen_under_pk(cur_pks)
        else:
            #根据下标取出第几组pk数据
            pk_grp = pk_groups[pk_idx]
            for pk_val in pk_grp:
                cur_pks[pk_idx] = pk_val
                # 递归调用生成笛卡尔积效果
                recur_pk(pk_idx + 1, cur_pks)

    pks = [None for _ in range(len(partition_idxs))]
    recur_pk(0, pks)

    return rows