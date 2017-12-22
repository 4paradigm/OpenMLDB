# -*- coding: utf-8 -*-
from logger import infoLogger
import commands
import copy


def exe_shell(cmd):
    print cmd
    infoLogger.debug(cmd)
    retcode, output = commands.getstatusoutput(cmd)
    return output


def read(file_name):
    strs = ""
    l = self.doRead(file_name)
    for s in l:
        strs = strs + s
    return strs


def do_read(file_name):
    l = []
    try:
        f = open(file_name, 'r')
        l = f.readlines()
    except IOError, (errno, strerror):
        infoLogger.error("I/O error(%s): %s" % (errno, strerror))
    except ValueError:
        infoLogger.error("Could not convert data to an integer.")
    except:
        infoLogger.error("Unexpected error:", sys.exc_info()[0])
    finally:
        try:
            f.close()
        except:
            infoLogger.error(traceback.print_exc())
    return l


def write(strs, file_name, patt):
    rst = False
    try:
        f = open(file_name, patt)
        f.write(strs)
        rst = True
    except IOError, (errno, strerror):
        infoLogger.error("I/O error(%s): %s" % (errno, strerror))
    except ValueError:
        infoLogger.error("Could not convert data to an integer.")
    except:
        infoLogger.error("Unexpected error:", sys.exc_info()[0])
    finally:
        try:
            f.close()
        except:
            infoLogger.error(traceback.print_exc())
    return rst


def gen_table_metadata(name, ttl, seg_cnt, *table_partitions):
    metadata = []
    basic_info_schema = ('name', 'ttl', 'seg_cnt')
    basic_info = zip(basic_info_schema, (name, ttl, seg_cnt))
    metadata.append(basic_info)
    tp_schema = ['endpoint', 'pid_group', 'is_leader']
    for tp in table_partitions:
        if tp is not None:
            tp_schema_tmp = copy.copy(tp_schema)
            if tp[0] is None:
                tp_schema_tmp.remove(tp_schema_tmp[0])
            if tp[1] is None:
                tp_schema_tmp.remove(tp_schema_tmp[1])
            if tp[2] is None:
                tp_schema_tmp.remove(tp_schema_tmp[2])
            metadata.append(zip(tp_schema_tmp, tp))
        else:
            metadata.append({})
    if name is None:
        metadata[0].remove(metadata[0][0])
    elif ttl is None:
        metadata[0].remove(metadata[0][1])
    elif seg_cnt is None:
        metadata[0].remove(metadata[0][2])
    # infoLogger.info(metadata)
    # print metadata
    return metadata


def gen_table_metadata_file(metadata, filepath):
    s = ''
    for basic in metadata[0]:
        s += '{}:{}\n'.format(basic[0], basic[1])
    for tp in metadata[1:]:
        s += 'table_partition {\n'
        for i in tp:
            s += '{}:{}\n'.format(i[0], i[1])
        s += '}\n'
    # infoLogger.info(s)
    print s
    write(s, filepath, 'w')


# m = gen_table_metadata('"1"', 3, 3, None)
# gen_table_metadata_file(m, 'naysameta')
