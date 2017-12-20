# -*- coding: utf-8 -*-
from logger import infoLogger
import commands


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
    tp_schema = ('endpoint', 'pid_group', 'is_leader')
    for tp in table_partitions:
        metadata.append(zip(tp_schema, tp))
    infoLogger.info(metadata)
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
    write(s, filepath, 'w')
