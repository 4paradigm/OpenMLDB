# -*- coding: utf-8 -*-
import conf as conf
from logger import infoLogger
import commands
import copy
import os

def exe_shell(cmd):
    infoLogger.debug(cmd)
    retcode, output = commands.getstatusoutput(cmd)
    infoLogger.debug(output)
    return output


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


def read(file_name):
    strs = ""
    l = do_read(file_name)
    for s in l:
        strs = strs + s
    return strs


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


def gen_table_metadata_file(name, ttl_type, ttl, seg_cnt, filepath,*table_info):
    metadata = ""
    basic_info_schema = ('name', 'ttl_type', 'ttl', 'seg_cnt')
    basic_info = zip(basic_info_schema, (name, ttl_type, ttl, seg_cnt))
    for key in basic_info:
        metadata += "{}:{}\n".format(key, basic_info[key])
    if table_info[0] is not None:
        for tp in table_info:
            metadata += "{} {\n".format(tp[0])
            for i in range(1, len(tp)):
                metadata += "{} : {}\n".format(tp[i][0], tp[i][1])
            metadata += "}\n"
    write(metadata, filepath, 'w')
    
            
    metadata.append([(i[0], i[1]) for i in basic_info if i[1] is not None])
    if table_partitions[0] is not None:
        for tp in table_partitions:
            ele_schema = conf.table_meta_ele[tp[0]]
            if tp is not None:
                ele_info = zip(ele_schema, tp[1:])
                infoLogger.info(ele_info)
                metadata.append((tp[0], [(i[0], i[1]) for i in ele_info if i[1] is not None]))
            else:
                metadata.append({})
    return metadata

def gen_table_metadata(name, ttl_type, ttl, seg_cnt, *table_partitions):
    metadata = []
    basic_info_schema = ('name', 'ttl_type', 'ttl', 'seg_cnt')
    basic_info = zip(basic_info_schema, (name, ttl_type, ttl, seg_cnt))
    metadata.append([(i[0], i[1]) for i in basic_info if i[1] is not None])
    if table_partitions[0] is not None:
        for tp in table_partitions:
            ele_schema = conf.table_meta_ele[tp[0]]
            if tp is not None:
                ele_info = zip(ele_schema, tp[1:])
                infoLogger.info(ele_info)
                metadata.append((tp[0], [(i[0], i[1]) for i in ele_info if i[1] is not None]))
            else:
                metadata.append({})
    return metadata

def gen_table_metadata_file(metadata, filepath):
    s = ''
    for basic in metadata[0]:
        s += '{}:{}\n'.format(basic[0], basic[1])
    for tp in metadata[1:]:
        s += tp[0] + ' {\n'
        for i in tp[1]:
            s += '{}:{}\n'.format(i[0], i[1])
        s += '}\n'
    write(s, filepath, 'w')
    infoLogger.info(read(filepath))

def gen_table_meta_file(meta, filepath):
    s = ''
    for key, value in meta.items():
        if isinstance(value, str):
            s += key + ": \"" + value + "\"\n"
        elif isinstance(value, int):
            s += key + ": " + str(value) + "\n"
        elif isinstance(value, list):
            for item in value:
                s += key + " {\n"
                for k, v in item.items():
                    if isinstance(v, str):
                        if v == "true" or v == "false":
                            s += "  " + k + ": " + v + "\n"
                        else:
                            s += "  " + k + ": \"" + v + "\"\n"
                    elif isinstance(v, int):
                        s += "  " + k + ": " + str(v) + "\n"
                    elif isinstance(v, list):
                        for value in v:
                            s += "  " + k + ": \"" + str(value) + "\"\n"
                s += "}\n"

    write(s, filepath, 'w')
    infoLogger.info(read(filepath))

def same_list(l1, l2):
    l2 = copy.deepcopy(l2)
    if len(l1) != len(l2):
        return False
    for i in l1:
        if i in l2:
            l2.remove(i)
    return l2 == []


def in_list(l1, l2):
    l1 = copy.deepcopy(l1)
    if len(l1) > len(l2):
        return False
    for i in l2:
        if i in l1:
            l1.remove(i)
    return l1 == []
