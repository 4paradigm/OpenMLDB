# gen_report.py
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

#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 wangtaize <wangtaize@m7-pce-dev01>
#
# Distributed under terms of the MIT license.

"""
generate benchmark echats report
"""
import re
import os
import pyecharts
from pyecharts import Line
def analyse_each_benchmark(report):
    
    result = []
    header = report[1]
    body = report[3:]
#     print(header)
    header_items = re.split("\s+",header)
    #print(header_items)
    items_cnt = len(header_items)
    for each in body:
        each_body_items = [ x for x in re.split("\s+",each) if x !="ns" ]
        #print(each_body_items)
        
        each_result = {}
        for i in range(0, items_cnt):
            item = header_items[i]
            value = each_body_items[i]
            each_result[item] = value;
        result.append(each_result)
    #print(result[0:2])
    return result
            
        
    
    
def analyse_benchmark_file(filename):
    fh = open(filename)

    # group benchmark
    result = {}
    benchmark_type = ""
    for line in  fh.readlines():
        line = line.strip()
        if line=="":
            continue
        #print(line)
        if line.endswith(":"):
            benchmark_type = line
            result[benchmark_type] = []
        else:
            result[benchmark_type].append(line)

    # print(result)
    report = {}
    
    for (k,v) in result.items():
        #print("start analyse %s" %(k) )
        k = k.replace(" ","_");
        report[k] = analyse_each_benchmark(v)
    # print(report)
    return report

    # parse each benchmark


def analyse_benchmark_trend(dir_path, file_list):
    benchmarks_map = {}
    list_size = len(file_list)
    pos = 0
    for file_name in file_list:
        
        analyse_report = analyse_benchmark_file(dir_path+"/"+file_name)
        for (k,v) in analyse_report.items():
            if not k in benchmarks_map:
                benchmarks_map[k] = {}
            benchmarks = benchmarks_map[k]
            for each in v:
                #print(v)
                Benchmark = each["Benchmark"]
                if not Benchmark in benchmarks:
                    benchmarks[Benchmark] = {
                        "Time":[0]*list_size,
                        "CPU":[0]*list_size,
                        "Iterations":[0]*list_size,

                    }
                benchmarks[Benchmark]["Time"].insert(pos,float(each["Time"]))
                benchmarks[Benchmark]["CPU"].insert(pos,float(each["CPU"]))
                benchmarks[Benchmark]["Iterations"].insert(pos,float(each["Iterations"]))
        pos+=1
            
    # print(benchmarks_map)
    print('analyse_benchmark_trend OK')
    return benchmarks_map


def generate_trend(benchmarks_map, columns, cate):
    lines = {}
    max_cnt = len(columns)
    for (group,benchmarks) in benchmarks_map.items():
        print(benchmarks)
        
        print(benchmarks.keys())
        for (bm_name,benchmark) in benchmarks.items():
            print(bm_name)
            #is_label_show是设置上方数据是否显示
            print(benchmark[cate])
            html_name = group + "_" + bm_name.split("/")[0]
            if not html_name in lines:
                line = Line("性能"+cate+"趋势","性能"+cate+"趋势", width=1600, height=1000)
                lines[html_name] = line
            
            line = lines[html_name]    
            line.add(bm_name, columns, benchmark[cate][0:max_cnt], is_label_show=True)
    for (name, line) in lines.items():
        render_name = "./render/"+cate+"Trend_"+name+".html"
        print(render_name)
        line.render(render_name)


def run():
    ts_columns = []
    for file_name in os.listdir("./data"):
        ts = file_name.split("_")[-1]
        ts_columns.append([ts,file_name])
        
    ts_columns.sort(key=lambda x:x[0])
    columns = [x[1] for x in ts_columns]
    benchmarks_map = analyse_benchmark_trend("./data", columns)
    generate_trend(benchmarks_map, columns, "Time")
    # generate_trend(benchmarks_map, columns, "CPU")
    # generate_trend(benchmarks_map, columns, "Iterations")

run();

    
